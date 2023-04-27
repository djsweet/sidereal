package name.djsweet.query.tree

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

data class ByteArrayThunk internal constructor(private val reverseSpec: ListNode<ByteArray>?) {
    private var reified: ByteArray? = null

    internal constructor(direct: ByteArray): this (null) {
        this.reified = direct
    }

    // Note that this is still thread-safe, because it only deals with loading and storing references.
    // All reference load/store operations are atomic according to the Java Memory Model. At the very
    // worst, we concatByteArraysFromReverseList more than strictly necessary, but we get to avoid
    // locking entirely.
    fun get(): ByteArray {
        var current = this.reified
        if (current == null) {
            current = concatByteArraysFromReverseList(this.reverseSpec)
            this.reified = current
        }
        return current
    }

    override fun equals(other: Any?): Boolean {
        return if (this === other) {
            true
        } else if (this.javaClass != other?.javaClass) {
            false
        } else {
            other as ByteArrayThunk
            this.get().contentEquals(other.get())
        }
    }

    override fun hashCode(): Int {
        return this.get().contentHashCode()
    }
}

internal class ByteArrayButComparable(val array: ByteArray): Comparable<ByteArrayButComparable> {
    constructor (arrayThunk: ByteArrayThunk): this(arrayThunk.get())

    override fun compareTo(other: ByteArrayButComparable): Int {
        return Arrays.compareUnsigned(this.array, other.array)
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