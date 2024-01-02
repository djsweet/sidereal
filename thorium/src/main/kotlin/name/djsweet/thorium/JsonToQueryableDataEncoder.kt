// SPDX-FileCopyrightText: 2023 Dani Sweet <thorium@djsweet.name>
//
// SPDX-License-Identifier: MIT

package name.djsweet.thorium

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import name.djsweet.query.tree.QPTrie

internal data class NonShareableScalarListQueryableData(
    val scalars: QPTrie<ByteArray>,
    val arrays: QPTrie<List<ByteArray>>
)

private fun updateResultForFullJsonArray(
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
    onJsonObject: (obj: JsonObject) -> NonShareableScalarListQueryableData,
    onJsonArray: (arr: JsonArray) -> NonShareableScalarListQueryableData,
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
            onJsonArray(value).copy(arrays=updateResultForFullJsonArray(
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

    fun offsetsForArray(obj: JsonArray): Iterable<Int>
    fun contextForArray(offset: Int): KeyValueFilterContext
}

internal class JsonToQueryableDataIterableQueue {
    data class ObjectQueueEntry(
        val serial: Int,
        val keyEncoder: Radix64LowLevelEncoder,
        val data: JsonObject,
        val filterContext: KeyValueFilterContext
    )

    data class ArrayQueueEntry(
        val serial: Int,
        val keyEncoder: Radix64LowLevelEncoder,
        val data: JsonArray,
        val filterContext: KeyValueFilterContext
    )

    private var currentSerial = 0
    private val objectQueue = ArrayDeque<ObjectQueueEntry>()
    private val arrayQueue = ArrayDeque<ArrayQueueEntry>()

    fun enqueueObject(
        keyEncoder: Radix64LowLevelEncoder,
        data: JsonObject,
        filterContext: KeyValueFilterContext
    ): JsonToQueryableDataIterableQueue {
        this.objectQueue.addLast(ObjectQueueEntry(
            this.currentSerial,
            keyEncoder,
            data,
            filterContext
        ))
        this.currentSerial++
        return this
    }

    fun enqueueArray(
        keyEncoder: Radix64LowLevelEncoder,
        data: JsonArray,
        filterContext: KeyValueFilterContext
    ): JsonToQueryableDataIterableQueue {
        this.arrayQueue.addLast(ArrayQueueEntry(
            this.currentSerial,
            keyEncoder,
            data,
            filterContext
        ))
        this.currentSerial++
        return this
    }

    inline fun processQueues(
        onJsonObject: (Radix64LowLevelEncoder, JsonObject, KeyValueFilterContext) -> Unit,
        onJsonArray: (Radix64LowLevelEncoder, JsonArray, KeyValueFilterContext) -> Unit
    ) {
        val arrayQueue = this.arrayQueue
        val objectQueue = this.objectQueue
        while (arrayQueue.isNotEmpty() || objectQueue.isNotEmpty()) {
            if (arrayQueue.isNotEmpty() && objectQueue.isNotEmpty()) {
                val fromArrayQueue = arrayQueue.first()
                val fromObjectQueue = objectQueue.first()
                if (fromArrayQueue.serial < fromObjectQueue.serial) {
                    val (_, keyEncoder, data, filterContext) = fromArrayQueue
                    arrayQueue.removeFirst()
                    onJsonArray(keyEncoder, data, filterContext)
                } else {
                    val (_, keyEncoder, data, filterContext) = fromObjectQueue
                    objectQueue.removeFirst()
                    onJsonObject(keyEncoder, data, filterContext)
                }
            } else if (arrayQueue.isNotEmpty()) {
                val (_, keyEncoder, data, filterContext) = arrayQueue.removeFirst()
                onJsonArray(keyEncoder, data, filterContext)
            } else { // this.objectQueue.isNotEmpty()
                val (_, keyEncoder, data, filterContext) = objectQueue.removeFirst()
                onJsonObject(keyEncoder, data, filterContext)
            }
        }
    }
}

internal fun encodeJsonToQueryableDataWithQueue(
    baseResult: NonShareableScalarListQueryableData,
    queue: JsonToQueryableDataIterableQueue,
    byteBudget: Int,
): NonShareableScalarListQueryableData {
    var result = baseResult
    val updateLocalResult: (Radix64LowLevelEncoder, Any, Int, KeyValueFilterContext) -> NonShareableScalarListQueryableData =
        { keyPath, value, stringByteBudget, filterContextForItem ->
            updateResultForKeyValue(
                result,
                keyPath,
                value,
                stringByteBudget,
                {
                    queue.enqueueObject(keyPath, it, filterContextForItem)
                    result
                },
                {
                    queue.enqueueArray(keyPath, it, filterContextForItem)
                    result
                }
            )
        }

    queue.processQueues(
        { parentKeyPath, curObj, curFilterContext ->
            val fieldNames = curObj.fieldNames()
            for (key in curFilterContext.keysForObject(curObj)) {
                if (!fieldNames.contains(key)) {
                    continue
                }
                val keyPath = parentKeyPath.clone().addString(key)
                val keyContentLength = keyPath.getOriginalContentLength()
                if (keyContentLength >= byteBudget) {
                    continue
                }
                val value = curObj.getValue(key)
                val filterContextForKey = curFilterContext.contextForObject(key)
                result = updateLocalResult(keyPath, value, byteBudget - keyContentLength, filterContextForKey)
            }
        },
        { parentKeyPath, curArray, curFilterContext ->
            val arrSize = curArray.size()
            for (offset in curFilterContext.offsetsForArray(curArray)) {
                if (offset >= arrSize) {
                    continue
                }
                val keyPath = parentKeyPath.clone().addString(offset.toString())
                val keyContentLength = keyPath.getOriginalContentLength()
                if (keyContentLength >= byteBudget) {
                    continue
                }
                val value = curArray.getValue(offset)
                val filterContextForArray = curFilterContext.contextForArray(offset)
                result = updateLocalResult(keyPath, value, byteBudget - keyContentLength, filterContextForArray)
            }
        }
    )
    return result
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
    val queue = JsonToQueryableDataIterableQueue()
    queue.enqueueObject(topKeyPath, obj, filterContext)
    return encodeJsonToQueryableDataWithQueue(baseResult, queue, byteBudget)
}

internal fun encodeJsonToQueryableDataIterative(
    arr: JsonArray,
    baseResult: NonShareableScalarListQueryableData,
    topKeyPath: Radix64LowLevelEncoder,
    filterContext: KeyValueFilterContext,
    byteBudget: Int,
): NonShareableScalarListQueryableData {
    if (filterContext.willAcceptNone()) {
        return baseResult
    }
    val queue = JsonToQueryableDataIterableQueue()
    queue.enqueueArray(topKeyPath, arr, filterContext)
    return encodeJsonToQueryableDataWithQueue(baseResult, queue, byteBudget)
}

internal fun updateResultForKeyValueRecursive(
    result: NonShareableScalarListQueryableData,
    keyPath: Radix64LowLevelEncoder,
    recurseDepth: Int,
    value: Any,
    byteBudget: Int,
    keyContentLength: Int,
    filterContext: KeyValueFilterContext
): NonShareableScalarListQueryableData {
    return updateResultForKeyValue(
        result,
        keyPath,
        value,
        byteBudget - keyContentLength,
        {
            if (recurseDepth <= 0) {
                encodeJsonToQueryableDataIterative(
                    it,
                    result,
                    keyPath,
                    filterContext,
                    // Passing in byteBudget directly is correct here: we want this to be the upper bound of
                    // the byte budget, from which the keyPath.originalContentLength is subtracted.
                    byteBudget
                )
            } else {
                encodeJsonToQueryableDataRecursive(
                    it,
                    result,
                    keyPath,
                    filterContext,
                    byteBudget,
                    recurseDepth - 1
                )
            }
        },
        {
            if (recurseDepth <= 0) {
                encodeJsonToQueryableDataIterative(
                    it,
                    result,
                    keyPath,
                    filterContext,
                    byteBudget
                )
            } else {
                encodeJsonToQueryableDataRecursive(
                    it,
                    result,
                    keyPath,
                    filterContext,
                    byteBudget,
                    recurseDepth - 1
                )
            }
        }
    )
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
        val keyPath = topKeyPath.clone().addString(key)
        val keyContentLength = keyPath.getOriginalContentLength()
        if (keyContentLength >= byteBudget) {
            continue
        }
        val value = obj.getValue(key)
        val filterContextForObject = filterContext.contextForObject(key)
        result = updateResultForKeyValueRecursive(
            result,
            keyPath,
            recurseDepth,
            value,
            byteBudget,
            keyContentLength,
            filterContextForObject
        )
    }
    return result
}

internal fun encodeJsonToQueryableDataRecursive(
    arr: JsonArray,
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
    for (i in filterContext.offsetsForArray(arr)) {
        val keyPath = topKeyPath.clone().addString(i.toString())
        val keyContentLength = keyPath.getOriginalContentLength()
        if (keyContentLength >= byteBudget) {
            continue
        }
        val value = arr.getValue(i)
        val filterContextForElement = filterContext.contextForArray(i)
        result = updateResultForKeyValueRecursive(
            result,
            keyPath,
            recurseDepth,
            value,
            byteBudget,
            keyContentLength,
            filterContextForElement
        )
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

    override fun offsetsForArray(obj: JsonArray): Iterable<Int> {
        return 0 until obj.size()
    }

    override fun contextForArray(offset: Int): KeyValueFilterContext {
        return this
    }
}

private val emptyKeySet = setOf<String>()
private val emptyIntSet = setOf<Int>()

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

    override fun offsetsForArray(obj: JsonArray): Iterable<Int> {
        return emptyIntSet
    }

    override fun contextForArray(offset: Int): KeyValueFilterContext {
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