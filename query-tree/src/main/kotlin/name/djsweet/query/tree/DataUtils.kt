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