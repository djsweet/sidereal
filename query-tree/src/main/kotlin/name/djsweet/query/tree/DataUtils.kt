package name.djsweet.query.tree

fun <T>workingDataForAvailableKeys(fullData: QPTrie<ByteArray>, keyDispatch: QPTrie<T>?): QPTrie<ByteArray> {
    if (keyDispatch == null) {
        return QPTrie()
    }
    val keyBasis = if (fullData.size < keyDispatch.size) { fullData } else { keyDispatch }
    var result = QPTrie<ByteArray>()
    for ((key) in keyBasis.iteratorUnsafeSharedKey()) {
        val fromFullData = fullData.get(key) ?: continue
        if (keyDispatch.get(key) != null) {
            result = result.put(key, fromFullData)
        }
    }
    return result
}

internal fun <U, V> mapSequence(iterable: Iterable<U>, functor: (item: U) -> V): Iterator<V> {
    return iterable.asSequence().map(functor).iterator()
}

internal fun <U, V> mapSequence(iterator: Iterator<U>, functor: (item: U) -> V): Iterator<V> {
    return iterator.asSequence().map(functor).iterator()
}

internal fun compareBytesUnsigned(left: Byte, right: Byte): Int {
    return java.lang.Byte.compareUnsigned(left, right)
}