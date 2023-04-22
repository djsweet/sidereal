package name.djsweet.query.tree

import kotlinx.collections.immutable.PersistentList
import java.util.Arrays

internal fun byteArraysEqualUpTo(left: ByteArray, right: ByteArray, rightFrom: Int, rightEntries: Int): Int {
    val leftSize = left.size.coerceAtMost(rightEntries)
    val rightSize = (rightFrom + rightEntries).coerceAtMost(right.size)
    val ret = Arrays.mismatch(left, 0, leftSize, right, rightFrom, rightSize)
    return if (ret < 0) {
        leftSize.coerceAtMost(rightSize - rightFrom)
    } else {
        ret
    }
}

internal fun findByteInSortedArray(r: ByteArray, b: Byte): Int {
    for (i in r.indices) {
        if (r[i] == b) {
            return i
        }
        if (r[i] > b) {
            return -(i + 1)
        }
    }
    return -(r.size + 1)
}

// We have to pass in dst because we can't otherwise instantiate an Array;
// Kotlin won't let us instantiate Array with a type parameter, and needs
// some sort of reified class or interface instead.
internal fun <T>insertArray(src: Array<T>, dst: Array<T>, insertOffset: Int, v: T) {
    if (insertOffset > 0) {
        src.copyInto(dst, 0, 0, insertOffset)
    }
    dst[insertOffset] = v
    if (insertOffset < src.size) {
        src.copyInto(dst, insertOffset + 1, insertOffset)
    }
}

internal fun <T>removeArray(src: Array<T>, dst: Array<T>, removeOffset: Int) {
    if (removeOffset > 0) {
        src.copyInto(dst, 0, 0, removeOffset)
        if (removeOffset < dst.size) {
            src.copyInto(dst, removeOffset, removeOffset + 1)
        }
    } else {
        src.copyInto(dst, 0, 1)
    }
}

internal fun insertByteArray(src: ByteArray, insertOffset: Int, v: Byte): ByteArray {
    val dst = ByteArray(src.size + 1)
    if (insertOffset > 0) {
        src.copyInto(dst, 0, 0, insertOffset)
    }
    dst[insertOffset] = v
    if (insertOffset < src.size) {
        src.copyInto(dst, insertOffset + 1, insertOffset)
    }
    return dst
}

internal fun removeByteArray(src: ByteArray, removeOffset: Int): ByteArray {
    val dst = ByteArray(src.size - 1)
    if (removeOffset > 0) {
        src.copyInto(dst, 0, 0, removeOffset)
        if (removeOffset < dst.size) {
            src.copyInto(dst, removeOffset, removeOffset + 1)
        }
    } else {
        src.copyInto(dst, 0, 1)
    }
    return dst
}

internal fun concatByteArraysWithMiddleByte(left: ByteArray, mid: Byte, right: ByteArray): ByteArray {
    val result = ByteArray(left.size + right.size + 1)
    left.copyInto(result)
    result[left.size] = mid
    right.copyInto(result, left.size + 1)
    return result
}

internal fun concatByteArraysFromReverseList(arrays: PersistentList<ByteArray>): ByteArray {
    val newSize = arrays.sumOf { it.size }
    var offset = newSize
    val newArray = ByteArray(newSize)
    for (arr in arrays) {
        val nextOffset = offset - arr.size
        arr.copyInto(newArray, nextOffset)
        offset = nextOffset
    }
    return newArray
}

// This is unfortunately public due to jqwik requiring that.
class ByteArrayButComparable(internal val array: ByteArray): Comparable<ByteArrayButComparable> {
    override fun compareTo(other: ByteArrayButComparable): Int {
        return Arrays.compare(this.array, other.array)
    }

    override fun toString(): String {
        return this.array.toList().toString()
    }

    override fun equals(other: Any?): Boolean {
        return if (ByteArrayButComparable::class.isInstance(other)) {
            this.compareTo(other as ByteArrayButComparable) == 0
        } else {
            super.equals(other)
        }
    }

    override fun hashCode(): Int {
        return array.contentHashCode()
    }
}