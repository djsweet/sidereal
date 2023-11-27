package name.djsweet.thorium

import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.json.jsonObjectOf
import name.djsweet.query.tree.QPTrie
import name.djsweet.query.tree.QuerySpec
import java.lang.Exception
import java.util.*

/*
 * Rough description of a query string encoding a full query:
 * key=value -> key == value
 * key=<value -> key < value
 * key=<=value -> key <= value
 * key=>value -> key > value
 * key=>=value -> key >= value
 * key=~value -> key starts-with value
 * key=[value -> key is an array containing value
 * key=!value -> key != value
 *
 * You can have as many =, =[, =! operators as you'd like.
 * You can only have up to one =~, or up to two <, <=, >=, > operators, and if there are two,
 * they must share the same key and be some pair of (<, >); (<=, >); (<, >=); (<=, >=)
 */

private val stopOffsetRegex = Regex("[^/~]*")

internal fun indexOfStopCharacterWithoutNegative(haystack: String, from: Int): Int {
    val matchResult = stopOffsetRegex.matchAt(haystack, from)
    return if (matchResult == null) { from } else { matchResult.value.length + from }
}

private val jsonObjectForInvalidJsonPointer = JsonObject()
    .put("code", "invalid-json-pointer")
    .put("message", "Invalid JSON Pointer.")

internal fun stringOrJsonPointerToStringKeyPath(sp: String): HttpProtocolErrorOr<List<String>> {
    val result = mutableListOf<String>()
    val isMetadataBasedQuery = sp.startsWith("../")
    if (!isMetadataBasedQuery) {
        result.add("data")
    }
    if (sp.startsWith("/") || isMetadataBasedQuery) {
        var stringOffset = if (isMetadataBasedQuery) { 3 } else { 1 }
        val builder = StringBuilder()
        while (stringOffset < sp.length) {
            val stopAt = indexOfStopCharacterWithoutNegative(sp, stringOffset)
            if (stopAt > stringOffset) {
                builder.append(sp.substring(stringOffset, stopAt))
            }
            stringOffset = stopAt + 1
            if (stopAt >= sp.length || sp[stopAt] == '/') {
                result.add(builder.toString())
                builder.clear()
                continue
            }
            // At this point, stopAt should be ~.
            val escapeAt = stopAt + 1
            if (escapeAt >= sp.length) {
                return HttpProtocolErrorOr.ofError(HttpProtocolError(400, jsonObjectForInvalidJsonPointer))
            }
            when (sp[escapeAt]) {
                '0' -> {
                    builder.append('~')
                }
                '1' -> {
                    builder.append('/')
                }
                else -> {
                    return HttpProtocolErrorOr.ofError(HttpProtocolError(400, jsonObjectForInvalidJsonPointer))
                }
            }
            stringOffset = escapeAt + 1
        }
        if (builder.isNotEmpty()) {
            // The builder might not be empty; this can happen if a string ends with ~0 or ~1.
            result.add(builder.toString())
        }
        if (sp.endsWith("/")) {
            // If a string ends with / we'll treat this as a terminating empty string.
            // This is actually still defined by RFC 6901: each / only indicates that
            // a JSON key shall follow, up to either / or the end of the string, and ""
            // is a perfectly valid JSON key.
            result.add("")
        }
    } else {
        result.add(sp)
    }
    return HttpProtocolErrorOr.ofSuccess(result)
}

internal fun stringKeyPathToEncodedKeyPath(skp: Iterable<String>): Pair<ByteArray, Int> {
    val encoder = Radix64LowLevelEncoder()
    for (pathEntry in skp) {
        encoder.addString(pathEntry)
    }
    return encoder.encode() to encoder.getOriginalContentLength()
}

private val jsonObjectForStringTooLong = JsonObject()
    .put("code", "string-too-long")
    .put("message", "String value is too long.")

private fun encodedErrorIfOverBudget(k: String, s: String, byteBudget: Int): HttpProtocolErrorOr<ByteArray> {
    val encoded = Radix64JsonEncoder.ofString(s, byteBudget)
    return if (!Radix64JsonEncoder.isStringWithinBudget(encoded)) {
        HttpProtocolErrorOr.ofError(HttpProtocolError(400, jsonObjectForStringTooLong.copy().put("key", k)))
    } else {
        HttpProtocolErrorOr.ofSuccess(encoded)
    }
}

internal fun queryStringValueToEncoded(k: String, s: String, from: Int, byteBudget: Int): HttpProtocolErrorOr<ByteArray> {
    val working = s.substring(from)
    try {
        // FIXME: Putting the working string into a JSON object might be unnecessary
        return when (val parsed = JsonObject("{ \"item\": $working }").getValue("item")) {
            null -> HttpProtocolErrorOr.ofSuccess(Radix64JsonEncoder.ofNull())
            is String -> encodedErrorIfOverBudget(k, parsed, byteBudget)
            is Number -> HttpProtocolErrorOr.ofSuccess(Radix64JsonEncoder.ofNumber(parsed.toDouble()))
            is Boolean -> HttpProtocolErrorOr.ofSuccess(Radix64JsonEncoder.ofBoolean(parsed))
            else -> encodedErrorIfOverBudget(k, working, byteBudget)
        }
    } catch (x: Exception) {
        // We actually won't do anything here, because if this is invalid JSON, it's just a string.
    }
    return encodedErrorIfOverBudget(k, working, byteBudget)
}

private val jsonObjectForTooMuchInequality = jsonObjectOf(
    "code" to "inequality-operator-already-used",
    "message" to "Inequality operator has already been set for this query."
)
private val jsonObjectForExistingKey = jsonObjectOf(
    "code" to "equality-operator-already-used",
    "message" to "An equality comparison has already been set for this key."
)
private val jsonObjectForTooManyTerms = jsonObjectOf(
    "code" to "too-many-terms",
    "message" to "This query has too many terms."
)
private val jsonObjectForNonStringStartsWith = jsonObjectOf(
    "code" to "non-string-for-starts-with",
    "message" to "Starts-with queries must match a string value"
)

data class FullQueryAndAffectedKeyIncrements(
    val fullQuery: FullQuery,
    val affectedKeyIncrements: MutableList<Pair<List<String>, Int>>
)

fun convertQueryStringToFullQuery(
    qs: Map<String, List<String>>,
    maxTerms: Int,
    byteBudget: Int
): HttpProtocolErrorOr<FullQueryAndAffectedKeyIncrements> {
    var error: HttpProtocolError? = null
    var treeSpec = QuerySpec.empty
    var arrayContains: QPTrie<QPTrie<Boolean>> = QPTrie()
    var notEquals: QPTrie<QPTrie<Boolean>> = QPTrie()
    var inequalityOperator: String? = null
    var inequalityKey: ByteArray? = null
    var inequalityValue: ByteArray? = null
    var startsWithVerification: StartsWithVerification? = null
    var termCount = 0
    val affectedKeyIncrements = mutableListOf<Pair<List<String>, Int>>()
    for ((key, values) in qs) {
        for (value in values) {
            termCount++
            if (termCount > maxTerms) {
                return HttpProtocolErrorOr.ofError(
                    HttpProtocolError(400, jsonObjectForTooManyTerms.copy().put("allowedTerms", maxTerms))
                )
            }
            stringOrJsonPointerToStringKeyPath(key).whenError {
                error = it
            }.whenSuccess { stringKeyPath ->
                affectedKeyIncrements.add(Pair(stringKeyPath, 1))
                val (encodedKey, encodedKeyOriginalLength) = stringKeyPathToEncodedKeyPath(stringKeyPath)
                val remainingByteBudget = byteBudget - encodedKeyOriginalLength
                if (value.isEmpty()) {
                    treeSpec = treeSpec.withEqualityTerm(encodedKey, Radix64JsonEncoder.ofString(value, remainingByteBudget))
                } else {
                    val startFrom = if (value.startsWith("<") || value.startsWith(">")) {
                        if (value.startsWith("<=") || value.startsWith(">=")) {
                            2
                        } else {
                            1
                        }
                    } else if (value.startsWith("~")
                            || value.startsWith("[")
                            || value.startsWith("!")) {
                        1
                    } else {
                        0
                    }
                    queryStringValueToEncoded(key, value, startFrom, remainingByteBudget).whenError {
                        error = it
                    }.whenSuccess { encodedValue ->
                        if (value.startsWith("<")) {
                            val lastInequalityOperator = inequalityOperator
                            if (lastInequalityOperator != null) {
                                if (lastInequalityOperator.startsWith(">") && Arrays.equals(inequalityKey, encodedKey)) {
                                    // We can promote this condition to a between condition without it being an error,
                                    // because of the shared keys.
                                    inequalityOperator = "between"
                                    treeSpec =
                                        treeSpec.withBetweenOrEqualToTerm(encodedKey, inequalityValue!!, encodedValue)
                                    if (!value.startsWith("<=")) {
                                        notEquals =
                                            notEquals.update(encodedKey) { (it ?: QPTrie()).put(encodedValue, false) }
                                    }
                                } else {
                                    error = HttpProtocolError(
                                        400,
                                        jsonObjectForTooMuchInequality.copy().put("operator", lastInequalityOperator)
                                    )
                                }
                            } else if (value.startsWith("<=")) {
                                inequalityOperator = "<="
                                treeSpec = treeSpec.withLessThanOrEqualToTerm(encodedKey, encodedValue)
                            } else {
                                inequalityOperator = "<"
                                treeSpec = treeSpec.withLessThanOrEqualToTerm(encodedKey, encodedValue)
                                notEquals = notEquals.update(encodedKey) { (it ?: QPTrie()).put(encodedValue, false) }
                            }

                            inequalityKey = encodedKey
                            inequalityValue = encodedValue
                        } else if (value.startsWith(">")) {
                            val lastInequalityOperator = inequalityOperator
                            if (lastInequalityOperator != null) {
                                if (lastInequalityOperator.startsWith("<") && Arrays.equals(inequalityKey, encodedKey)) {
                                    inequalityOperator = "between"
                                    treeSpec =
                                        treeSpec.withBetweenOrEqualToTerm(encodedKey, encodedValue, inequalityValue!!)
                                    if (!value.startsWith(">=")) {
                                        notEquals =
                                            notEquals.update(encodedKey) { (it ?: QPTrie()).put(encodedValue, false) }
                                    }
                                } else {
                                    error = HttpProtocolError(
                                        400,
                                        jsonObjectForTooMuchInequality.copy().put("operator", lastInequalityOperator)
                                    )
                                }
                            } else if (value.startsWith(">=")) {
                                inequalityOperator = ">="
                                treeSpec = treeSpec.withGreaterThanOrEqualToTerm(encodedKey, encodedValue)
                            } else {
                                inequalityOperator = ">"
                                treeSpec = treeSpec.withGreaterThanOrEqualToTerm(encodedKey, encodedValue)
                                notEquals = notEquals.update(encodedKey) { (it ?: QPTrie()).put(encodedValue, false) }
                            }

                            inequalityKey = encodedKey
                            inequalityValue = encodedValue
                        } else if (value.startsWith("~")) {
                            if (inequalityOperator != null) {
                                error = HttpProtocolError(
                                    400,
                                    jsonObjectForTooMuchInequality.copy().put("operator", inequalityOperator)
                                )
                            } else if (!Radix64JsonEncoder.isString(encodedValue)) {
                                // FIXME: Test this explicitly
                                error = HttpProtocolError(
                                    400,
                                    jsonObjectForNonStringStartsWith.copy().put("value", value.substring(1))
                                )
                            } else {
                                inequalityOperator = "~"
                                // We intentionally skip the "in budget suffix" when working with starts-with, because
                                // a valid substring will never start with a byte array containing the "in budget suffix".
                                var queryValue = Radix64JsonEncoder.removeInBudgetSuffixFromString(encodedValue)
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
                                when (Radix64JsonEncoder.contentByteLengthAssumeNoEndElement(queryValue) % 4) {
                                    2 -> {
                                        // As in 1110 0000
                                        val mask = 0xE0
                                        val byteOffset = queryValue.size - 1
                                        val expected = queryValue[byteOffset].toInt().and(mask).toByte()
                                        startsWithVerification = StartsWithVerification(
                                            encodedKey,
                                            byteOffset,
                                            mask.toByte(),
                                            expected
                                        )
                                        queryValue = queryValue.copyOfRange(0, byteOffset)
                                    }
                                    3 -> {
                                        // As in 1111 1000
                                        val mask = 0xF8
                                        val byteOffset = queryValue.size - 1
                                        val expected = queryValue[byteOffset].toInt().and(mask).toByte()
                                        startsWithVerification = StartsWithVerification(
                                            encodedKey,
                                            byteOffset,
                                            mask.toByte(),
                                            expected
                                        )
                                        queryValue = queryValue.copyOfRange(0, byteOffset)
                                    }
                                }
                                treeSpec = treeSpec.withStartsWithTerm(
                                    encodedKey,
                                    queryValue
                                )
                                inequalityKey = encodedKey
                                inequalityValue = encodedValue
                            }
                        } else if (value.startsWith("[")) {
                            arrayContains = arrayContains.update(encodedKey) { (it ?: QPTrie()).put(encodedValue, true) }
                        } else if (value.startsWith("!")) {
                            notEquals = notEquals.update(encodedKey) { (it ?: QPTrie()).put(encodedValue, false) }
                        } else if (treeSpec.hasEqualityTerm(encodedKey)) {
                            error = HttpProtocolError(400, jsonObjectForExistingKey.copy().put("key", key))
                        } else {
                            treeSpec = treeSpec.withEqualityTerm(encodedKey, encodedValue)
                        }
                    }
                }
            }
        }
        val lastError = error
        if (lastError != null) {
            return HttpProtocolErrorOr.ofError(lastError)
        }
    }
    return HttpProtocolErrorOr.ofSuccess(FullQueryAndAffectedKeyIncrements(
        fullQuery = FullQuery(treeSpec, arrayContains, notEquals, startsWithVerification),
        affectedKeyIncrements = affectedKeyIncrements
    ))
}
