package name.djsweet.thorium

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import name.djsweet.query.tree.QPTrie

internal data class NonShareableScalarListQueryableData(
    val scalars: QPTrie<ByteArray>,
    val arrays: QPTrie<List<ByteArray>>
)

private fun updateResultForJsonArray(
    result: QPTrie<List<ByteArray>>,
    keyPath: Radix64LowLevelEncoder,
    values: JsonArray,
    stringByteBudget: Int
): QPTrie<List<ByteArray>> {
    val entries = mutableListOf<ByteArray>()
    for (value in values) {
        when (value) {
            is String -> {
                entries.add(Radix64JsonEncoder.ofString(value, stringByteBudget))
            }
            is Number -> {
                entries.add(Radix64JsonEncoder.ofNumber(value.toDouble()))
            }
            null -> {
                entries.add(Radix64JsonEncoder.ofNull())
            }
            is Boolean -> {
                entries.add(Radix64JsonEncoder.ofBoolean(value))
            }
        }
    }
    return result.putUnsafeSharedKey(
        keyPath.encode(),
        entries
    )
}

private inline fun updateResultForKeyValue(
    result: NonShareableScalarListQueryableData,
    keyPath: Radix64LowLevelEncoder,
    value: Any?,
    stringByteBudget: Int,
    onJsonObject: (obj: JsonObject) -> NonShareableScalarListQueryableData
): NonShareableScalarListQueryableData {
    return when (value) {
        is String -> {
            result.copy(scalars=result.scalars.putUnsafeSharedKey(
                keyPath.encode(),
                Radix64JsonEncoder.ofString(value, stringByteBudget)
            ))
        }
        is Number -> {
            result.copy(scalars=result.scalars.putUnsafeSharedKey(
                keyPath.encode(),
                Radix64JsonEncoder.ofNumber(value.toDouble())
            ))
        }
        null -> {
            result.copy(scalars=result.scalars.putUnsafeSharedKey(
                keyPath.encode(),
                Radix64JsonEncoder.ofNull()
            ))
        }
        is Boolean -> {
            result.copy(scalars=result.scalars.putUnsafeSharedKey(
                keyPath.encode(),
                Radix64JsonEncoder.ofBoolean(value)
            ))
        }
        is JsonObject -> {
            onJsonObject(value)
        }
        is JsonArray -> {
            result.copy(arrays=updateResultForJsonArray(
                result.arrays,
                keyPath,
                value,
                stringByteBudget
            ))
        }
        else -> {
            result
        }
    }
}

interface KeyValueFilterContext {
    fun willAcceptNone(): Boolean {
        return false
    }

    fun keysForObject(obj: JsonObject): Iterable<String>
    fun contextForObject(key: String): KeyValueFilterContext
}

internal fun encodeJsonToQueryableDataIterative(
    obj: JsonObject,
    baseResult: NonShareableScalarListQueryableData,
    topKeyPath: Radix64LowLevelEncoder,
    filterContext: KeyValueFilterContext,
    byteBudget: Int,
): NonShareableScalarListQueryableData {
    if (filterContext.willAcceptNone()) {
        return baseResult
    }
    val queue = ArrayDeque<Triple<Radix64LowLevelEncoder, JsonObject, KeyValueFilterContext>>()
    queue.add(Triple(topKeyPath, obj, filterContext))
    var result = baseResult
    while (!queue.isEmpty()) {
        val (parentKeyPath, curObj, curFilterContext) = queue.removeFirst()
        val fieldNames = curObj.fieldNames()
        for (key in curFilterContext.keysForObject(curObj)) {
            if (!fieldNames.contains(key)) {
                continue
            }
            val value = curObj.getValue(key)
            val keyPath = parentKeyPath.clone().addString(key)
            val keyContentLength = keyPath.getOriginalContentLength()
            if (keyContentLength >= byteBudget) {
                continue
            }
            result = updateResultForKeyValue(
                result,
                keyPath,
                value,
                byteBudget - keyContentLength,
            ) {
                queue.addLast(Triple(keyPath, it, curFilterContext.contextForObject(key)))
                result
            }
        }
    }
    return result
}

internal fun encodeJsonToQueryableDataRecursive(
    obj: JsonObject,
    baseResult: NonShareableScalarListQueryableData,
    topKeyPath: Radix64LowLevelEncoder,
    filterContext: KeyValueFilterContext,
    byteBudget: Int,
    recurseDepth: Int
): NonShareableScalarListQueryableData {
    if (filterContext.willAcceptNone()) {
        return baseResult
    }
    var result = baseResult
    val fieldNames = obj.fieldNames()
    for (key in filterContext.keysForObject(obj)) {
        if (!fieldNames.contains(key)) {
            continue
        }
        val value = obj.getValue(key)
        val keyPath = topKeyPath.clone().addString(key)
        val keyContentLength = keyPath.getOriginalContentLength()
        if (keyContentLength >= byteBudget) {
            continue
        }
        result = updateResultForKeyValue(
            result,
            keyPath,
            value,
            byteBudget - keyContentLength,
        ) {
            val filterContextForObject = filterContext.contextForObject(key)
            if (recurseDepth <= 0) {
                encodeJsonToQueryableDataIterative(
                    it,
                    result,
                    keyPath,
                    filterContextForObject,
                    byteBudget
                )
            } else {
                encodeJsonToQueryableDataRecursive(
                    it,
                    result,
                    keyPath,
                    filterContextForObject,
                    byteBudget,
                    recurseDepth - 1
                )
            }
        }
    }
    return result
}

data class ShareableScalarListQueryableData(
    val scalars: ShareableQPTrieOfByteArrays,
    val arrays: ShareableQPTrieOfByteArrayLists
)

class AcceptAllKeyValueFilterContext: KeyValueFilterContext {
    override fun keysForObject(obj: JsonObject): Iterable<String> {
        return obj.fieldNames()
    }

    override fun contextForObject(key: String): KeyValueFilterContext {
        return this
    }
}

private val emptyKeySet = setOf<String>()

class AcceptNoneKeyValueFilterContext: KeyValueFilterContext {
    override fun willAcceptNone(): Boolean {
        return true
    }

    override fun keysForObject(obj: JsonObject): Iterable<String> {
        return emptyKeySet
    }

    override fun contextForObject(key: String): KeyValueFilterContext {
        return this
    }
}

// This is used in benchmarking, so it can't be internal.
fun encodeJsonToQueryableData(
    obj: JsonObject,
    filterContext: KeyValueFilterContext,
    byteBudget: Int,
    recurseDepth: Int
): ShareableScalarListQueryableData {
    val result = encodeJsonToQueryableDataRecursive(
        obj,
        NonShareableScalarListQueryableData(scalars=QPTrie(), arrays=QPTrie()),
        Radix64LowLevelEncoder(),
        filterContext,
        byteBudget,
        recurseDepth
    )
    return ShareableScalarListQueryableData(
        scalars=ShareableQPTrieOfByteArrays(result.scalars),
        arrays=ShareableQPTrieOfByteArrayLists(result.arrays)
    )
}