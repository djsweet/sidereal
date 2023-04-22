package name.djsweet.query.tree

fun <T>workingDataForAvailableKeys(fullData: QPTrie<ByteArray>, keyDispatch: QPTrie<T>?): QPTrie<ByteArray> {
    if (keyDispatch == null) {
        return QPTrie()
    }
    val keyBasis = if (fullData.size < keyDispatch.size) { fullData } else { keyDispatch }
    var result = QPTrie<ByteArray>()
    for (kvp in keyBasis) {
        val fromFullData = fullData.get(kvp.first) ?: continue
        if (keyDispatch.get(kvp.first) != null) {
            result = result.put(kvp.first, fromFullData)
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