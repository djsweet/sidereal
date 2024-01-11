// SPDX-FileCopyrightText: 2023 Dani Sweet <sidereal@djsweet.name>
//
// SPDX-License-Identifier: MIT

package name.djsweet.query.tree

fun<T> keysForValueUnion(fullData: QPTrie<ByteArray>, keyDispatch: QPTrie<T>?): ArrayList<ByteArray> {
    if (keyDispatch == null || keyDispatch.size == 0L) {
        // This is a slight optimization over the normal behavior: if keyDispatch
        // is null or empty, we're not going to iterate over any keys successfully anyway.
        return ArrayList()
    }
    val bestTrie = if (fullData.size <= keyDispatch.size) { fullData } else { keyDispatch }
    val result = ArrayList<ByteArray>(bestTrie.size.toInt())
    bestTrie.keysIntoUnsafeSharedKey(result)
    return result
}

internal fun compareBytesUnsigned(left: Byte, right: Byte): Int {
    return java.lang.Byte.compareUnsigned(left, right)
}