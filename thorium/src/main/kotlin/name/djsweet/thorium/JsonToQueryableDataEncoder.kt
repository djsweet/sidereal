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

internal fun encodeJsonToQueryableDataIterative(
    obj: JsonObject,
    baseResult: NonShareableScalarListQueryableData,
    topKeyPath: Radix64LowLevelEncoder,
    byteBudget: Int,
): NonShareableScalarListQueryableData {
    val queue = ArrayDeque<Pair<Radix64LowLevelEncoder, JsonObject>>()
    queue.add(topKeyPath to obj)
    var result = baseResult
    while (!queue.isEmpty()) {
        val (parentKeyPath, curObj) = queue.removeFirst()
        for ((key, value) in curObj) {
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
                queue.addLast(keyPath to it)
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
    byteBudget: Int,
    recurseDepth: Int
): NonShareableScalarListQueryableData {
    var result = baseResult
    for ((key, value) in obj) {
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
            if (recurseDepth <= 0) {
                encodeJsonToQueryableDataIterative(it, result, keyPath, byteBudget)
            } else {
                encodeJsonToQueryableDataRecursive(it, result, keyPath, byteBudget, recurseDepth - 1)
            }
        }
    }
    return result
}

data class ShareableScalarListQueryableData(
    val scalars: ShareableQPTrieOfByteArrays,
    val arrays: ShareableQPTrieOfByteArrayLists
)

// This is used in benchmarking, so it can't be internal.
fun encodeJsonToQueryableData(obj: JsonObject, byteBudget: Int, recurseDepth: Int): ShareableScalarListQueryableData {
    val result = encodeJsonToQueryableDataRecursive(
        obj,
        NonShareableScalarListQueryableData(scalars=QPTrie(), arrays=QPTrie()),
        Radix64LowLevelEncoder(),
        byteBudget,
        recurseDepth
    )
    return ShareableScalarListQueryableData(
        scalars=ShareableQPTrieOfByteArrays(result.scalars),
        arrays=ShareableQPTrieOfByteArrayLists(result.arrays)
    )
}