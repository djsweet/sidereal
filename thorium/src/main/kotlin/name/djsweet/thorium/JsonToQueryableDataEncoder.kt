package name.djsweet.thorium

import io.vertx.core.json.JsonObject
import name.djsweet.query.tree.QPTrie

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
            when (value) {
                null -> {
                    result = result.put(keyPath.encode(), Radix64JsonEncoder.ofNull())
                }
                is Boolean -> {
                    result = result.put(keyPath.encode(), Radix64JsonEncoder.ofBoolean(value))
                }
                is Double -> {
                    result = result.put(keyPath.encode(), Radix64JsonEncoder.ofNumber(value))
                }
                is String -> {
                    val stringByteBudget = byteBudget - keyContentLength
                    result = result.put(keyPath.encode(), Radix64JsonEncoder.ofString(value, stringByteBudget))
                }
                is JsonObject -> {
                    queue.addLast(keyPath to value)
                }
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
        when (value) {
            null -> {
                result = result.put(keyPath.encode(), Radix64JsonEncoder.ofNull())
            }
            is Boolean -> {
                result = result.put(keyPath.encode(), Radix64JsonEncoder.ofBoolean(value))
            }
            is Double -> {
                result = result.put(keyPath.encode(), Radix64JsonEncoder.ofNumber(value))
            }
            is String -> {
                val stringByteBudget = byteBudget - keyContentLength
                result = result.put(keyPath.encode(), Radix64JsonEncoder.ofString(value, stringByteBudget))
            }
            is JsonObject -> {
                result = if (recurseDepth <= 0) {
                    encodeJsonToQueryableDataIterative(value, result, keyPath, byteBudget)
                } else {
                    encodeJsonToQueryableDataRecursive(value, result, keyPath, byteBudget, recurseDepth - 1)
                }
            }
        }
    }
    return result
}

internal fun encodeJsonToQueryableData(obj: JsonObject, byteBudget: Int, recurseDepth: Int): ShareableQPTrie {
    val result = encodeJsonToQueryableDataRecursive(obj, QPTrie(), Radix64LowLevelEncoder(), byteBudget, recurseDepth)
    return ShareableQPTrie(result)
}