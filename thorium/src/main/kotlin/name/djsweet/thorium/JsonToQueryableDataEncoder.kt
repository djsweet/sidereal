package name.djsweet.thorium

import io.vertx.core.json.JsonObject
import name.djsweet.query.tree.QPTrie

private inline fun updateResultForKeyValue(
    result: QPTrie<ByteArray>,
    keyPath: Radix64LowLevelEncoder,
    value: Any?,
    stringByteBudget: Int,
    onJsonObject: (obj: JsonObject) -> QPTrie<ByteArray>
): QPTrie<ByteArray> {
    return when (value) {
        null -> {
            result.putUnsafeSharedKey(keyPath.encode(), Radix64JsonEncoder.ofNull())
        }
        is Boolean -> {
            result.putUnsafeSharedKey(keyPath.encode(), Radix64JsonEncoder.ofBoolean(value))
        }
        is Number -> {
            result.putUnsafeSharedKey(keyPath.encode(), Radix64JsonEncoder.ofNumber(value.toDouble()))
        }
        is String -> {
            result.putUnsafeSharedKey(keyPath.encode(), Radix64JsonEncoder.ofString(value, stringByteBudget))
        }
        is JsonObject -> {
            onJsonObject(value)
        }
        else -> {
            result
        }
    }
}

internal fun encodeJsonToQueryableDataIterative(
    obj: JsonObject,
    baseTrie: QPTrie<ByteArray>,
    topKeyPath: Radix64LowLevelEncoder,
    byteBudget: Int,
): QPTrie<ByteArray> {
    val queue = ArrayDeque<Pair<Radix64LowLevelEncoder, JsonObject>>()
    queue.add(topKeyPath to obj)
    var result = baseTrie
    while (!queue.isEmpty()) {
        val (parentKeyPath, curObj) = queue.removeFirst()
        for ((key, value) in curObj) {
            val keyPath = parentKeyPath.clone().addString(key)
            val keyContentLength = keyPath.getFullContentLength()
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
    baseTrie: QPTrie<ByteArray>,
    topKeyPath: Radix64LowLevelEncoder,
    byteBudget: Int,
    recurseDepth: Int
): QPTrie<ByteArray> {
    var result = baseTrie
    for ((key, value) in obj) {
        val keyPath = topKeyPath.clone().addString(key)
        val keyContentLength = keyPath.getFullContentLength()
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

// This is used in benchmarking, so it can't be internal.
fun encodeJsonToQueryableData(obj: JsonObject, byteBudget: Int, recurseDepth: Int): ShareableQPTrie {
    val result = encodeJsonToQueryableDataRecursive(obj, QPTrie(), Radix64LowLevelEncoder(), byteBudget, recurseDepth)
    return ShareableQPTrie(result)
}