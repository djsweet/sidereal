package name.djsweet.query.tree

fun<T> workingDataForAvailableKeys(fullData: QPTrie<ByteArray>, keyDispatch: QPTrie<T>?): QPTrie<ByteArray> {
    if (keyDispatch == null) {
        return QPTrie()
    }
    var result = QPTrie<ByteArray>()
    if (fullData.size < keyDispatch.size) {
        fullData.visitUnsafeSharedKey { (key, value) ->
            if (keyDispatch.get(key) != null) {
                result = result.putUnsafeSharedKey(key, value)
            }
        }
    } else {
        keyDispatch.visitUnsafeSharedKey { (key) ->
            val value = fullData.get(key)
            if (value != null) {
                result = result.putUnsafeSharedKey(key, value)
            }
        }
    }
    return result
}

internal fun compareBytesUnsigned(left: Byte, right: Byte): Int {
    return java.lang.Byte.compareUnsigned(left, right)
}