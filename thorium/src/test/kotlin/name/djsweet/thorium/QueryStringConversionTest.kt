package name.djsweet.thorium

import io.vertx.core.MultiMap
import io.vertx.core.json.JsonObject
import name.djsweet.query.tree.QPTrie
import name.djsweet.query.tree.QuerySpec
import net.jqwik.api.*
import org.junit.jupiter.api.Assertions.*
import java.util.*

private typealias UnaryTarget = Triple<Pair<String, List<String>>, String, Pair<String, ByteArray>>

private typealias BinaryTarget = Triple<
            Pair<String, List<String>>,
            String,
            Pair<Pair<String, ByteArray>, Pair<String, ByteArray>>
    >

class QueryStringConversionTest {
    private val notStartingJsonPath = Arbitraries
        .strings()
        .ofMaxLength(64)
        .filter { !it.startsWith("/") }
        .map { it to listOf(it) }
    private val jsonPathEntry = Arbitraries
        .strings()
        .ofMaxLength(64)
        .map {
            val builder = StringBuilder()
            for (ch in it) {
                when (ch) {
                    '/' -> {
                        builder.append("~1")
                    }
                    '~' -> {
                        builder.append("~0")
                    }
                    else -> {
                        builder.append(ch)
                    }
                }
            }
            builder.toString() to it
        }
    private val jsonPath = jsonPathEntry.list().ofMinSize(1).ofMaxSize(10).map {
        val builder = StringBuilder()
        val valueList = mutableListOf<String>()
        for ((encoded, actual) in it) {
            builder.append("/")
            builder.append(encoded)
            valueList.add(actual)
        }
        builder.toString() to valueList.toList()
    }

    @Provide
    fun invalidJsonPathEncodingCharacters(): Arbitrary<Char> = Arbitraries.chars().filter { it != '0' && it != '1' }

    @Provide
    fun randomInsertionValue(): Arbitrary<Double> = Arbitraries.doubles().between(0.0, true, 1.0, false)

    @Provide
    fun fullJsonPathSelectors(): Arbitrary<Pair<String, List<String>>> {
        return this.jsonPath
    }

    @Provide
    fun validKeySelectors(): Arbitrary<Pair<String, List<String>>> {
        return Arbitraries.oneOf(this.notStartingJsonPath, this.jsonPath)
    }

    private fun encodeStringListInRadix64(sl: List<String>): ByteArray {
        val byteEncoder = Radix64LowLevelEncoder()
        for (entry in sl) {
            byteEncoder.addString(entry)
        }
        return byteEncoder.encode()
    }

    @Property
    fun validKeySelectorsSuccessfullyEncode(
        @ForAll @From("validKeySelectors") selectorSpec: Pair<String, List<String>>
    ) {
        val (queryStringEncoded, toByteEncode) = selectorSpec
        val bytesEncoded = encodeStringListInRadix64(toByteEncode)
        var succeeded = false
        stringOrJsonPointerToKeyPath(queryStringEncoded).whenError {
            fail<String>("Could not decode $queryStringEncoded")
        }.whenSuccess {
            assertEquals(bytesEncoded.toList(), it.first.toList())
            succeeded = true
        }
        assertTrue(succeeded)
    }

    @Property
    fun invalidKeySelectorsEscapeAtEnd(
        @ForAll @From("fullJsonPathSelectors") selectorSpec: Pair<String, List<String>>
    ) {
        val (queryStringEncoded) = selectorSpec
        var succeeded = false
        stringOrJsonPointerToKeyPath("$queryStringEncoded~").whenSuccess {
            fail<String>("Should not have been able to decode '$queryStringEncoded~'")
        }.whenError {
            assertEquals(400, it.statusCode)
            succeeded = true
        }
        assertTrue(succeeded)
    }

    @Property
    fun invalidKeySelectorsInvalidEscape(
        @ForAll @From("fullJsonPathSelectors") selectorSpec: Pair<String, List<String>>,
        @ForAll @From("invalidJsonPathEncodingCharacters") badChar: Char,
        @ForAll @From("randomInsertionValue") insertionPointBasis: Double
    ) {
        val (queryStringEncoded) = selectorSpec
        val insertionPoint = (insertionPointBasis * (queryStringEncoded.length - 1) + 1).toInt()
        val testString = "${queryStringEncoded.substring(0, insertionPoint)}~$badChar${queryStringEncoded.substring(insertionPoint)}"
        var succeeded = false
        stringOrJsonPointerToKeyPath(testString).whenSuccess {
            fail<String>("Should not have been able to decode '$queryStringEncoded~'")
        }.whenError {
            assertEquals(400, it.statusCode)
            succeeded = true
        }
        assertTrue(succeeded)
    }

    private val maxStringLength = 128
    private val maxStringByteLength = maxStringLength * 4

    @Provide
    fun jsonValueWithinBudget(): Arbitrary<Pair<String, ByteArray>> {
        return Arbitraries.oneOf(listOf(
            Arbitraries.just(null).map { "null" to Radix64JsonEncoder.ofNull() },
            Arbitraries.oneOf(listOf(
                Arbitraries.just(true),
                Arbitraries.just(false)
            )).map { it.toString() to Radix64JsonEncoder.ofBoolean(it) },
            Arbitraries.doubles().map { it.toString() to Radix64JsonEncoder.ofNumber(it) },
            Arbitraries.strings().ofMaxLength(this.maxStringLength).filter {
                if (
                        it.startsWith(">") ||
                        it.startsWith("<") ||
                        it.startsWith("[") ||
                        it.startsWith("~") ||
                        it.startsWith("!")) {
                    false
                } else {
                    try {
                        JsonObject("{ \"item\": $it }")
                        false
                    } catch (x: Exception) {
                        true
                    }
                }
            }.map { it to Radix64JsonEncoder.ofString(it, this.maxStringByteLength) },
            Arbitraries.strings().ofMaxLength(this.maxStringLength).filter {
                try {
                    JsonObject("{ \"item\": \"$it\" }")
                    true
                } catch (x: Exception) {
                    false
                }
            }.map {
                "\"$it\"" to Radix64JsonEncoder.ofString(it, this.maxStringByteLength)
            }
        ))
    }

    @Property
    fun convertingQueryStringValueToByteArray(
        @ForAll @From("jsonValueWithinBudget") specs: Pair<String, ByteArray>
    ) {
        val (queryString, encodedResult) = specs
        var succeeded = false
        queryStringValueToEncoded("any", queryString, 0, this.maxStringByteLength).whenError {
            fail<String>("Converting query string value $queryString to byte array failed")
        }.whenSuccess {
            assertEquals(encodedResult.toList(), it.toList())
            succeeded = true
        }
        assertTrue(succeeded)
    }

    private val maxQueryStringTerms = 24

    @Provide
    fun validQueryStringUnaryTargets(): Arbitrary<List<UnaryTarget>> {
        val nonEqualityUnaryOpChoices = Arbitraries.oneOf(listOf(
            Arbitraries.just("<"),
            Arbitraries.just("<="),
            Arbitraries.just(">"),
            Arbitraries.just(">="),
            Arbitraries.just("~")
        ))
        val equalityContainsNotUnaryOpChoices = Arbitraries.oneOf(listOf(
            Arbitraries.just(""),
            Arbitraries.just("["),
            Arbitraries.just("!")
        ))
        val equalityContainsNotTerms = this.jsonPath.flatMap { pathPair ->
            equalityContainsNotUnaryOpChoices.flatMap { opString ->
                this.jsonValueWithinBudget().map { Triple(pathPair, opString, it) }
            }
        }.list().ofMaxSize(maxQueryStringTerms - 2)
        val unaryOpTerm = this.jsonPath.flatMap { pathPair ->
            nonEqualityUnaryOpChoices.flatMap { opString ->
                this.jsonValueWithinBudget().map{ Triple(pathPair, opString, it) }
            }
        }.list().ofMaxSize(1)
        return equalityContainsNotTerms.flatMap { et ->
            unaryOpTerm.map { ut ->
                val res = et.toMutableList()
                res.addAll(ut)
                res.toList()
            }
        }
    }

    @Provide
    fun validQueryStringsBinaryTargets(): Arbitrary<List<BinaryTarget>> {
        val betweenChoices = Arbitraries.oneOf(listOf(
            Arbitraries.just("[between]"),
            Arbitraries.just("[between)"),
            Arbitraries.just("(between)"),
            Arbitraries.just("(between]")
        ))
        return this.jsonPath.flatMap { pathPair ->
            this.jsonValueWithinBudget().list().ofMinSize(2).ofMaxSize(2).filter {
                it[0].javaClass === it[1].javaClass
            }.flatMap { entries ->
                betweenChoices.map { opString ->
                    val left = entries[0]
                    val right = entries[1]
                    val (_, leftBytes) = left
                    val (_, rightBytes) = right
                    val resultPair = if (Arrays.compareUnsigned(leftBytes, rightBytes) <= 0) {
                        Pair(left, right)
                    } else {
                        Pair(right, left)
                    }
                    Triple(pathPair, opString, resultPair)
                }
            }.list().ofMaxSize(1)
        }
    }

    private fun convertBooleanQPTrieToList(qpTrie: QPTrie<QPTrie<Boolean>>): List<Triple<List<Byte>, List<Byte>, Boolean>> {
        val result = mutableListOf<Triple<List<Byte>, List<Byte>, Boolean>>()
        qpTrie.visitAscendingUnsafeSharedKey { (jsonKey, jsonValues) ->
            jsonValues.visitAscendingUnsafeSharedKey { (jsonValue, booleanValue) ->
                result.add(Triple(jsonKey.toList(), jsonValue.toList(), booleanValue))
            }
        }
        return result
    }

    @Property
    fun validConversionOfQueryStringToFullQuery(
        @ForAll @From("validQueryStringUnaryTargets") unaryTargets: List<UnaryTarget>,
        @ForAll @From("validQueryStringsBinaryTargets") binaryTargets: List<BinaryTarget>
    ) {
        var querySpec = QuerySpec.empty
        var arrayContains = QPTrie<QPTrie<Boolean>>()
        var notEquals = QPTrie<QPTrie<Boolean>>()

        val dedupUnaryTargets = mutableMapOf<String, UnaryTarget>()
        for (target in unaryTargets) {
            val (pathPair, opString) = target
            if (opString != "" && opString != "!" && opString != "[") {
                // We're going to have a single binary target, we should skip
                // our non-equality, non-not-equality, non-array-contains
                // operation so that we don't (correctly) fail to parse this.
                if (binaryTargets.isNotEmpty()) {
                    continue
                }
            }
            dedupUnaryTargets[pathPair.first] = target
        }
        for (target in binaryTargets) {
            dedupUnaryTargets.remove(target.first.first)
        }

        val queryString = mutableMapOf<String, List<String>>()
        for ((pathPair, opString, valuePair) in dedupUnaryTargets.values) {
            val keyPath = encodeStringListInRadix64(pathPair.second)
            queryString[pathPair.first] = listOf("$opString${valuePair.first}")
            when (opString) {
                "" -> {
                    querySpec = querySpec.withEqualityTerm(keyPath, valuePair.second)
                }
                "[" -> {
                    arrayContains = arrayContains.update(keyPath) { (it ?: QPTrie()).put(valuePair.second, true) }
                }
                "!" -> {
                    notEquals = notEquals.update(keyPath) { (it ?: QPTrie()).put(valuePair.second, false) }
                }
                "<" -> {
                    querySpec = querySpec.withLessThanOrEqualToTerm(keyPath, valuePair.second)
                    notEquals = notEquals.update(keyPath) { (it ?: QPTrie()).put(valuePair.second, false) }
                }
                "<=" -> {
                    querySpec = querySpec.withLessThanOrEqualToTerm(keyPath, valuePair.second)
                }
                ">=" -> {
                    querySpec = querySpec.withGreaterThanOrEqualToTerm(keyPath, valuePair.second)
                }
                ">" -> {
                    querySpec = querySpec.withGreaterThanOrEqualToTerm(keyPath, valuePair.second)
                    notEquals = notEquals.update(keyPath) { (it ?: QPTrie()).put(valuePair.second, false) }
                }
                "~" -> {
                    // We intentionally skip the "in budget suffix" when working with starts-with, because
                    // a valid substring will never start with a byte array containing the "in budget suffix".
                    querySpec = querySpec.withStartsWithTerm(
                        keyPath,
                        Radix64JsonEncoder.removeInBudgetSuffixFromString(valuePair.second)
                    )
                }
            }
        }

        for ((pathPair, opString, boundsPair) in binaryTargets) {
            val (lowerBoundPair, upperBoundPair) = boundsPair
            val keyPath = encodeStringListInRadix64(pathPair.second)
            val lowerBoundOpString = if (opString.startsWith("[")) {
                ">="
            } else {
                ">"
            }
            val upperBoundOpString = if (opString.endsWith("]")) {
                "<="
            } else {
                "<"
            }
            if (!lowerBoundOpString.endsWith("=")) {
                notEquals = notEquals.update(keyPath) { (it ?: QPTrie()).put(lowerBoundPair.second, false) }
            }
            if (!upperBoundOpString.endsWith("=")) {
                notEquals = notEquals.update(keyPath) { (it ?: QPTrie()).put(upperBoundPair.second, false) }
            }
            queryString[pathPair.first] = listOf(
                "$upperBoundOpString${upperBoundPair.first}",
                "$lowerBoundOpString${lowerBoundPair.first}"
            )
            querySpec = querySpec.withBetweenOrEqualToTerm(keyPath, lowerBoundPair.second, upperBoundPair.second)
        }

        var succeeded = false
        convertQueryStringToFullQuery(queryString, this.maxQueryStringTerms, this.maxStringByteLength * 3)
            .whenError {
                fail<String>("Should have decoded query string: ${it.contents}")
            }.whenSuccess {
                succeeded = true
                assertEquals(querySpec, it.treeSpec)
                assertEquals(convertBooleanQPTrieToList(arrayContains), convertBooleanQPTrieToList(it.arrayContains))
                assertEquals(convertBooleanQPTrieToList(notEquals), convertBooleanQPTrieToList(it.notEquals))
            }
        assertTrue(succeeded)
    }
}