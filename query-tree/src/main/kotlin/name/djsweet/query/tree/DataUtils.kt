package name.djsweet.query.tree

fun<T> workingDataForAvailableEqualityKeys(
    fullData: QPTrie<ByteArray>,
    keyDispatch: QPTrie<QPTrie<T>>?
): QPTrie<ByteArray> {
    if (keyDispatch == null) {
        return QPTrie()
    }
    val keyBasis = if (fullData.size < keyDispatch.size) { fullData } else { keyDispatch }
    var result = QPTrie<ByteArray>()
    for ((key) in keyBasis.iteratorUnsafeSharedKey()) {
        val fromFullData = fullData.get(key) ?: continue
        val fromKeyDispatch = keyDispatch.get(key) ?: continue
        if (fromKeyDispatch.get(fromFullData) != null) {
            result = result.putNoCopy(key, fromFullData)
        }
    }
    return result
}

fun<T> workingDataForAvailableKeys(fullData: QPTrie<ByteArray>, keyDispatch: QPTrie<T>?): QPTrie<ByteArray> {
    if (keyDispatch == null) {
        return QPTrie()
    }
    val keyBasis = if (fullData.size < keyDispatch.size) { fullData } else { keyDispatch }
    var result = QPTrie<ByteArray>()
    for ((key) in keyBasis.iteratorUnsafeSharedKey()) {
        val fromFullData = fullData.get(key) ?: continue
        if (keyDispatch.get(key) != null) {
            result = result.putNoCopy(key, fromFullData)
        }
    }
    return result
}

internal class MapSequenceIterator<U, V>(
    private val basis: Iterator<U>,
    private val functor: (item: U) -> V
): Iterator<V> {
    override fun hasNext(): Boolean {
        return this.basis.hasNext()
    }

    override fun next(): V {
        return this.functor(this.basis.next())
    }
}

internal fun <U, V> mapSequence(iterable: Iterable<U>, functor: (item: U) -> V): Iterator<V> {
    return MapSequenceIterator(iterable.iterator(), functor)
}

internal fun <U, V> mapSequence(iterator: Iterator<U>, functor: (item: U) -> V): Iterator<V> {
    return MapSequenceIterator(iterator, functor)
}

internal fun compareBytesUnsigned(left: Byte, right: Byte): Int {
    return java.lang.Byte.compareUnsigned(left, right)
}