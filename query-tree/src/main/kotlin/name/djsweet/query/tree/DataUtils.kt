package name.djsweet.query.tree

// Iterating over the available keys in a QPTrie is extremely expensive,
// unless you use keysIntoUnsafeSharedKey, which is around 10x as fast.
// We don't want to be caught constantly allocating and returning
// ArrayList instances, so we'll hold on to one per thread. We're not
// going to yield inside the users of this thread-local, so it should
// be safe to use.
val workingDataKeysArrayThreadLocal: ThreadLocal<ArrayList<ByteArray>> = ThreadLocal.withInitial { ArrayList() }

fun<T> workingDataForAvailableKeys(fullData: QPTrie<ByteArray>, keyDispatch: QPTrie<T>?): QPTrie<ByteArray> {
    if (keyDispatch == null) {
        return QPTrie()
    }
    val keyBasis = if (fullData.size < keyDispatch.size) { fullData } else { keyDispatch }
    var result = QPTrie<ByteArray>()
    val workingKeysArray = workingDataKeysArrayThreadLocal.get()
    workingKeysArray.clear()
    for (key in keyBasis.keysIntoUnsafeSharedKey(workingKeysArray)) {
        val fromFullData = fullData.get(key) ?: continue
        if (keyDispatch.get(key) != null) {
            result = result.putUnsafeSharedKey(key, fromFullData)
        }
    }
    return result
}

internal fun compareBytesUnsigned(left: Byte, right: Byte): Int {
    return java.lang.Byte.compareUnsigned(left, right)
}