// SPDX-FileCopyrightText: 2023 Dani Sweet <thorium@djsweet.name>
//
// SPDX-License-Identifier: MIT

package name.djsweet.thorium

import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.json.jsonObjectOf
import name.djsweet.query.tree.QPTrie
import name.djsweet.query.tree.QuerySetTree
import name.djsweet.query.tree.QuerySpec
import name.djsweet.thorium.servers.QueryResponderSpec
import net.jqwik.api.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.*

// As in "full JsonPointer encoded string" to "path through nested objects"
private typealias JsonPointerSelector = Pair<String, List<String>>
// The left hand side is a JSON-encoded value. So 6 would be '6', "six" would be '"six"', null is always null.
private typealias JsonEncodingToRadix64Encoding = Pair<String, ByteArray>

data class UnaryTarget(
    val selector: JsonPointerSelector,
    val operator: String,
    val jsonValue: JsonEncodingToRadix64Encoding,
)

data class BinaryTarget(
    val selector: JsonPointerSelector,
    val operator: String,
    val lhs: JsonEncodingToRadix64Encoding,
    val rhs: JsonEncodingToRadix64Encoding,
)

class QueryStringConversionTest {
    private val notStartingJsonSelectors = Arbitraries
        .strings()
        .ofMaxLength(64)
        // JsonPointers start with /, and we've extended this with a prefix of ../ so that the default selection
        // happens within the "data" property, but we can still break out of that "data" property.
        // We also support non-JsonPointer selectors by default, so you can say
        //   x=y
        // which means data["x"] == "y". Since we want these to be valid JSON property selectors without also
        // colliding with the JSON Pointer syntax, we're dropping everything that starts with / or ../
        .filter { !it.startsWith("/") && !it.startsWith("../") }
        // All key paths are presumed to start with "data" if they are not JSON Pointers
        .map { it to listOf("data", it) }

    private val jsonPathEntry = Arbitraries
        .strings()
        .ofMaxLength(64)
        .map {
            val builder = StringBuilder()
            // These are escapes defined for JSON Pointers in https://datatracker.ietf.org/doc/html/rfc6901
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

    private val jsonPath = jsonPathEntry
        .list()
        .ofMinSize(1)
        .ofMaxSize(10)
        .flatMap { pathEntry ->
            Arbitraries
                .oneOf(listOf(Arbitraries.just(true), Arbitraries.just(false)))
                .map { fromMetadata -> Pair(pathEntry, fromMetadata) }
        }
        .map { (path, fromMetadata) ->
            val builder = StringBuilder()
            val valueList = mutableListOf<String>()
            // By default, every JSON accessor is defined in terms of the "data" property in the object.
            // Other top-level properties require that a full JSON Pointer be prefixed by "..", as if the
            // pointer were a POSIX path.
            if (fromMetadata) {
                builder.append("..")
            } else {
                valueList.add("data")
            }
            for ((encoded, actual) in path) {
                builder.append("/")
                builder.append(encoded)
                valueList.add(actual)
            }
            builder.toString() to valueList.toList()
        }

    @Provide
    fun invalidJsonPathEncodingCharacters(): Arbitrary<Char> =
        // ~0 and ~1 are well-defined and expected in https://datatracker.ietf.org/doc/html/rfc6901. We are trying
        // to test for invalid encodings like ~2, ~3, ~x, etc.
        Arbitraries.chars().filter { it != '0' && it != '1' }

    @Provide
    fun randomInsertionValue(): Arbitrary<Double> =
        Arbitraries.doubles().between(0.0, true, 1.0, false)

    @Provide
    fun fullJsonPathSelectors(): Arbitrary<JsonPointerSelector> {
        return this.jsonPath
    }

    @Provide
    fun validKeySelectors(): Arbitrary<JsonPointerSelector> {
        return Arbitraries.oneOf(this.notStartingJsonSelectors, this.jsonPath)
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
        @ForAll @From("validKeySelectors") selectorSpec: JsonPointerSelector
    ) {
        val (queryStringEncoded, toByteEncode) = selectorSpec
        val bytesEncoded = encodeStringListInRadix64(toByteEncode)
        var succeeded = false
        stringOrJsonPointerToStringKeyPath(queryStringEncoded).whenError {
            fail<String>("Could not decode $queryStringEncoded")
        }.whenSuccess {
            val encodedFromKeyPath = stringKeyPathToEncodedKeyPath(it)
            assertEquals(bytesEncoded.toList(), encodedFromKeyPath.first.toList())
            succeeded = true
        }
        assertTrue(succeeded)
    }

    @Property
    fun invalidKeySelectorsEscapeAtEnd(
        @ForAll @From("fullJsonPathSelectors") selectorSpec: JsonPointerSelector
    ) {
        val (queryStringEncoded) = selectorSpec
        var succeeded = false
        stringOrJsonPointerToStringKeyPath("$queryStringEncoded~").whenSuccess {
            fail<String>("Should not have been able to decode '$queryStringEncoded~'")
        }.whenError {
            assertEquals(400, it.statusCode)
            succeeded = true
        }
        assertTrue(succeeded)
    }

    @Property
    fun invalidKeySelectorsInvalidEscape(
        @ForAll @From("fullJsonPathSelectors") selectorSpec: JsonPointerSelector,
        @ForAll @From("invalidJsonPathEncodingCharacters") badChar: Char,
        @ForAll @From("randomInsertionValue") insertionPointBasis: Double
    ) {
        val (queryStringEncoded) = selectorSpec
        // We want to always preserve the prefix that signals to stringOrJsonPointerToStringKeyPath that we are working
        // with a JSON Pointer. If we aren't dealing with metadata, we start with /; if we are, we start with ../.
        // In the first case, we always skip past 1 character, and in the second case, we always skip past 3 characters.
        val preserveChars = if (queryStringEncoded.startsWith("../")) { 3 } else { 1 }
        val insertionPoint = (insertionPointBasis * (queryStringEncoded.length - preserveChars) + preserveChars).toInt()
        val testString = "${queryStringEncoded.substring(0, insertionPoint)}~$badChar${queryStringEncoded.substring(insertionPoint)}"
        var succeeded = false
        stringOrJsonPointerToStringKeyPath(testString).whenSuccess {
            fail<String>("Should not have been able to decode '$queryStringEncoded~'/'$testString' as '$it'")
        }.whenError {
            assertEquals(400, it.statusCode)
            succeeded = true
        }
        assertTrue(succeeded)
    }

    private val maxStringLength = 128
    private val maxStringByteLength = maxStringLength * 4

    @Provide
    fun jsonValueWithinBudget(): Arbitrary<JsonEncodingToRadix64Encoding> {
        // This provider yields JSON-encoded values (i.e. 'null', '"something"', '7') mapping to their
        // Radix64JsonEncoding.
        return Arbitraries.oneOf(listOf(
            Arbitraries.just(null).map { "null" to Radix64JsonEncoder.ofNull() },
            Arbitraries.oneOf(listOf(
                Arbitraries.just(true),
                Arbitraries.just(false)
            )).map { it.toString() to Radix64JsonEncoder.ofBoolean(it) },
            Arbitraries.doubles().map { it.toString() to Radix64JsonEncoder.ofNumber(it) },
            Arbitraries.strings().ofMaxLength(this.maxStringLength).filter {
                // This arbitrary tests bare strings that aren't properly JSON-encoded.
                // These prefixes are used to determine the exact operator to apply to the value at the selector.
                // While you could safely put these in double-quotes, you can't safely put them in a bare string,
                // because that would change the operator meaning.
                if (
                        it.startsWith(">") ||
                        it.startsWith("<") ||
                        it.startsWith("[") ||
                        it.startsWith("~") ||
                        it.startsWith("!")) {
                    false
                } else {
                    try {
                        // These bare strings must not be valid JSON. Specifically, numbers, booleans, and null
                        // will be treated as such if they aren't enclosed in quotes.
                        JsonObject("{ \"item\": $it }")
                        false
                    } catch (x: Exception) {
                        true
                    }
                }
            }.map { it to Radix64JsonEncoder.ofString(it, this.maxStringByteLength) },
            Arbitraries.strings().ofMaxLength(this.maxStringLength).filter {
                try {
                    // These strings need to be valid JSON strings. Specifically, no double quotes early-terminating
                    // the resulting string, _unless_ they are escaped.
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
        @ForAll @From("jsonValueWithinBudget") specs: JsonEncodingToRadix64Encoding
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

        // You can have as many equality terms as you'd like...
        val equalityContainsNotTerms = this.jsonPath.flatMap { selector ->
            equalityContainsNotUnaryOpChoices.flatMap { opString ->
                this.jsonValueWithinBudget().map { UnaryTarget(selector, opString, it) }
            }
        }.list().ofMaxSize(maxQueryStringTerms - 2)

        // But for any other operator, you can have at most one of them.
        val unaryOpTerm = this.jsonPath.flatMap { selector ->
            nonEqualityUnaryOpChoices.flatMap { opString ->
                val valuesWithinBudget = if (opString == "~") {
                    // ~ only works correctly with Strings, and will fail with an HTTP 400 otherwise.
                    this.jsonValueWithinBudget().filter{ Radix64JsonEncoder.isString(it.second) }
                } else {
                    this.jsonValueWithinBudget()
                }
                valuesWithinBudget.map{ UnaryTarget(selector, opString, it) }
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

        return this.jsonPath.flatMap { selector ->
            this.jsonValueWithinBudget().list().ofMinSize(2).ofMaxSize(2).filter {
                Radix64JsonEncoder.encodedValuesHaveSameType(it[0].second, it[1].second)
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
                    BinaryTarget(selector, opString, resultPair.first, resultPair.second)
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

        val deduplicatedUnaryTargets = mutableMapOf<String, UnaryTarget>()
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
            deduplicatedUnaryTargets[pathPair.first] = target
        }
        for (target in binaryTargets) {
            deduplicatedUnaryTargets.remove(target.selector.first)
        }

        val queryString = mutableMapOf<String, List<String>>()
        for ((pathPair, opString, valuePair) in deduplicatedUnaryTargets.values) {
            val keyPath = encodeStringListInRadix64(pathPair.second)
            queryString[pathPair.first] = listOf("$opString${valuePair.first}")
            // Here, we're gradually building up the query-under-test.
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
                    var queryValue = Radix64JsonEncoder.removeInBudgetSuffixFromString(valuePair.second)
                    // starts-with is somewhat incompatible with any Radix64 encoding, because
                    // we're going to end up with situations where the lowest 4 or 2 bits need
                    // to be ignored, due to the 4:3 ratio of encoded-decoded bytes, and
                    // the modulus of the byte length by 3 sometimes being 1 or 2.
                    //
                    // When the byte length modulo 3 is 1, we end up representing 1 byte as 2
                    // with 4 extra bits. When the byte length modulo 3 is 2, we end up representing
                    // 2 bytes as 3 with 2 extra bits. In both cases, we can only use the existing
                    // starts-with functionality for all but the last byte, which will have to be
                    // checked separately as part of the "starts with verification".
                    if (Radix64JsonEncoder.contentByteLengthAssumeNoEndElement(queryValue) % 4 > 0) {
                        queryValue = queryValue.copyOfRange(0, queryValue.size - 1)
                    }
                    querySpec = querySpec.withStartsWithTerm(
                        keyPath,
                        queryValue
                    )
                }
            }
        }

        for ((pathPair, opString, lowerBoundPair, upperBoundPair) in binaryTargets) {
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
            }.whenSuccess { (fullQuery) ->
                // FIXME: Test the affectedKeyIncrements as well
                succeeded = true
                assertEquals(querySpec, fullQuery.treeSpec)
                assertEquals(convertBooleanQPTrieToList(arrayContains), convertBooleanQPTrieToList(fullQuery.arrayContains))
                assertEquals(convertBooleanQPTrieToList(notEquals), convertBooleanQPTrieToList(fullQuery.notEquals))
            }
        assertTrue(succeeded)
    }

    @Test
    fun startsWithStringCorrectlyHandlesByteBoundaryMismatch() {
        // Consider the following query:
        //    key=~some.value
        // The value "some.value" has 10 characters. Because our Radix64 encoding
        // maps 3 bytes to 4 bytes, 2 bytes to 3 bytes, and 1 byte to 2 bytes, with
        // some trailing bits, this won't work naively, because of the trailing 4
        // bits on the last 2 bytes of the string.
        // But we need this to work in general, not just over strings that happen
        // to have lengths divisible by 3. So, we'll use the QueryTree directly for
        // the substring
        //    some.valu (9 characters)
        // which will match, and then perform an after-the-fact filtering over the
        // original string, to ensure that the full data does start with the entire
        //    some.value

        val fullStartsWith = "thorium.query.meta.remove"
        val dataKey = "key"
        val matchedData = jsonObjectOf(dataKey to fullStartsWith)
        val byteBudget = (dataKey.length + fullStartsWith.length) * 2
        val (matchedDataScalars, matchedDataArrays) = encodeJsonToQueryableData(
            matchedData,
            AcceptAllKeyValueFilterContext(),
            byteBudget,
            1
        )
        assertEquals(0L, matchedDataArrays.trie.size)
        assertEquals(1L, matchedDataScalars.trie.size)

        for (endIndex in 1..fullStartsWith.length) {
            val startsWithValue = fullStartsWith.substring(0, endIndex)

            // Just as important as matching the correct value is not matching
            // incorrect values. Making sure we match strings that don't fit neatly
            // into the 4 bytes per 3 bytes encoding could create false positives,
            // if we're not careful.
            val notQuiteValue = fullStartsWith.substring(0, endIndex - 1)
            val notQuiteMatchedData = jsonObjectOf(dataKey to notQuiteValue)
            val (notQuiteMatchedDataScalars, notQuiteMatchedDataArrays) = encodeJsonToQueryableData(
                notQuiteMatchedData,
                AcceptAllKeyValueFilterContext(),
                byteBudget,
                1
            )
            assertEquals(0L, notQuiteMatchedDataArrays.trie.size)
            assertEquals(1L, notQuiteMatchedDataScalars.trie.size)

            convertQueryStringToFullQuery(
                mapOf("../$dataKey" to listOf("~$startsWithValue")),
                1,
                byteBudget
            ).whenError {
                fail("Unexpected query decode failure: ${it.contents.encode()}")
            }.whenSuccess { (fullQuery) ->
                if (endIndex % 3 != 0) {
                    assertNotNull(fullQuery.startsWithVerification)
                } else {
                    assertNull(fullQuery.startsWithVerification)
                }
                // First, we're ensuring a true positive for all substrings
                val responder = QueryResponderSpec(
                    queries = listOf(fullQuery),
                    respondTo = "someone",
                    clientID = "me",
                    queryID = "the query",
                    addedAt = monotonicNowMS(),
                    arrayContainsCounts = listOf(0)
                )
                val (_, queryTree) = QuerySetTree<QueryResponderSpec>()
                    .addElementByQuery(fullQuery.treeSpec, responder)

                val foundResponders = mutableListOf<QueryResponderSpec>()
                queryTree.visitByData(matchedDataScalars.trie) { _, spec ->
                    foundResponders.add(spec)
                }

                assertEquals(1, foundResponders.size)
                assertEquals(responder, foundResponders[0])
                assertTrue(fullQuery.startsWithMatchesAtOffsets(matchedDataScalars.trie))

                // Next, we're ensuring no false positives for a substring that
                // should not match
                foundResponders.clear()
                queryTree.visitByData(notQuiteMatchedDataScalars.trie) { _, spec ->
                    foundResponders.add(spec)
                }

                assertEquals(0, foundResponders.size)
            }
        }
    }
}