// SPDX-FileCopyrightText: 2023 Dani Sweet <sidereal@djsweet.name>
//
// SPDX-License-Identifier: MIT

package name.djsweet.query.tree

import java.util.Arrays

internal fun byteArraysEqualUpTo(
    left: ByteArray,
    leftFrom: Int,
    leftEntries: Int,
    right: ByteArray,
    rightFrom: Int,
    rightEntries: Int
): Int {
    val leftTo = (leftFrom + leftEntries.coerceAtMost(rightEntries)).coerceAtMost(left.size)
    val rightTo = (rightFrom + rightEntries).coerceAtMost(right.size)
    val ret = Arrays.mismatch(left, leftFrom, leftTo, right, rightFrom, rightTo)
    return if (ret < 0) {
        leftTo.coerceAtMost(rightTo - rightFrom)
    } else {
        ret
    }
}

internal fun findByteInSortedArray(r: ByteArray, b: Byte): Int {
    for (i in r.indices) {
        val indexCompare = compareBytesUnsigned(r[i], b)
        if (indexCompare == 0) {
            return i
        }
        if (indexCompare > 0) {
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

internal fun concatByteArraysFromReverseList(arrays: ListNode<ByteArray>?): ByteArray {
    val newSize = listIterator(arrays).asSequence().sumOf { it.size }
    var offset = newSize
    val newArray = ByteArray(newSize)
    for (arr in listIterator(arrays)) {
        val nextOffset = offset - arr.size
        arr.copyInto(newArray, nextOffset)
        offset = nextOffset
    }
    return newArray
}

internal class ByteArrayButComparable(val array: ByteArray): Comparable<ByteArrayButComparable> {
    override fun compareTo(other: ByteArrayButComparable): Int {
        return Arrays.compareUnsigned(this.array, other.array)
    }

    override fun toString(): String {
        return this.array.toList().toString()
    }

    override fun equals(other: Any?): Boolean {
        return if (other is ByteArrayButComparable) {
            this.compareTo(other) == 0
        } else {
            super.equals(other)
        }
    }

    override fun hashCode(): Int {
        return array.contentHashCode()
    }
}