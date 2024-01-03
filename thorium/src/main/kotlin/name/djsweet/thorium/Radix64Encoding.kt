// SPDX-FileCopyrightText: 2023 Dani Sweet <thorium@djsweet.name>
//
// SPDX-License-Identifier: MIT

package name.djsweet.thorium

import java.util.*

/*
 * This is a Radix 64 + 2 control flags encoding. It supports arbitrarily nested arrays of byte buffers, while
 * preserving relative sort order of the encoded data.

 * Each encoded value takes the following form:
 * - 11111111 0xxxxxx1 0xx00001 00000000 - Single byte
 * - 11111111 0xxxxxx1 0xxxxxx1 0xxxx001 00000000 - Two bytes
 * - 11111111 0xxxxxx1 0xxxxxx1 0xxxxxx1 0xxxxxx1 00000000 - Three bytes
 * - 11111111 ... 00000000 11111111 ... 00000000 - Two array elements of arbitrary length bytes
 * - 11111111 11111111 ... 00000000 11111111 ... 00000000 00000000 - One array element of two array elements of arbitrary byte lengths
 * - 11111111 10000001 00000000 - Empty array, e.g. []
 * - 11111111 00000000 - Empty byte array element
 */

internal const val arrayEmptyElement = 0x81.toByte()
internal const val arrayStartElement = 0xff.toByte()
internal const val arrayEndElement = 0x00.toByte()

internal fun requiredLengthForRadix64Array(byteLength: Int): Int {
    if (byteLength == 0) {
        return 2
    }
    val mod3 = byteLength.mod(3)
    return (byteLength - mod3) / 3 * 4 + when (mod3) {
        2 -> 3
        1 -> 2
        else -> 0
    } + 2
}

internal fun encodeByteArray(src: ByteArray, dst: ByteArray, dstOffset: Int): Int {
    val resultLength = requiredLengthForRadix64Array(src.size)
    if (dstOffset + resultLength > dst.size) {
        throw IllegalArgumentException("Insufficient space in the destination buffer")
    }
    if (src.isEmpty()) {
        dst[dstOffset] = arrayStartElement
        dst[dstOffset + 1] = arrayEndElement
        return dstOffset + 2
    }
    val mod3 = src.size.mod(3)
    val maxLoopOffset = src.size - mod3
    dst[dstOffset] = arrayStartElement
    for (i in 0 until maxLoopOffset step 3) {
        val j = (i * 4) / 3 + 1 + dstOffset
        val byte3 = src[i].toInt().and(0xff)
        val byte2 = src[i + 1].toInt().and(0xff)
        val byte1 = src[i + 2].toInt().and(0xff)
        dst[j] = byte3.shr(1).or(0x01).toByte()
        dst[j + 1] = byte3.and(0x03).shl(5).or(byte2.shr(3)).or(0x01).toByte()
        dst[j + 2] = byte2.and(0x0f).shl(3).or(byte1.shr(5)).or(0x01).toByte()
        dst[j + 3] = byte1.and(0x3f).shl(1).or(0x01).toByte()
    }
    if (mod3 == 2) {
        val byte2 = src[src.size - 2].toInt().and(0xff)
        val byte1 = src[src.size - 1].toInt().and(0xff)
        val j = dstOffset + resultLength - 4
        dst[j] = byte2.shr(1).or(0x01).toByte()
        dst[j + 1] = byte2.and(0x03).shl(5).or(byte1.shr(3)).or(0x01).toByte()
        dst[j + 2] = byte1.and(0x0f).shl(3).or(0x01).toByte()
    } else if (mod3 == 1) {
        val byte1 = src[src.size - 1].toInt().and(0xff)
        val j = dstOffset + resultLength - 3
        dst[j] = byte1.shr(1).or(0x01).toByte()
        dst[j + 1] = byte1.and(0x03).shl(5).or(0x01).toByte()
    }
    val nextOffset = dstOffset + resultLength
    dst[nextOffset - 1] = arrayEndElement
    return nextOffset
}

internal fun encodeSingleByteArray(r: ByteArray): ByteArray {
    val result = ByteArray(requiredLengthForRadix64Array(r.size))
    encodeByteArray(r, result, 0)
    return result
}

internal class Radix64EncoderComponent(
    val bytes: ByteArray,
    val next: Radix64EncoderComponent?
)

internal abstract class Radix64Encoder {
    protected var contents: Radix64EncoderComponent? = null
    protected var contentLength: Int = 0
    protected var ogContentLength: Int = 0

    fun getFullContentLength(): Int {
        return this.contentLength
    }

    fun getOriginalContentLength(): Int {
        return this.ogContentLength
    }

    fun hasContents(): Boolean {
        return this.contents != null
    }

    fun encodeInto(dst: ByteArray, offset: Int) {
        var targetLength = this.contentLength
        if (offset + targetLength > dst.size) {
            throw IllegalArgumentException("Insufficient space in the destination buffer")
        }
        var targets = this.contents
        while (targets != null) {
            val bytes = targets.bytes
            val bytesSize = bytes.size
            bytes.copyInto(dst, offset + targetLength - bytesSize, 0, bytesSize)
            targetLength -= bytesSize
            targets = targets.next
        }
    }

    fun encode(): ByteArray {
        val result = ByteArray(this.getFullContentLength())
        this.encodeInto(result, 0)
        return result
    }
}

internal class Radix64LowLevelEncoder : Radix64Encoder() {
    companion object {
        private fun ofByteArray(r: ByteArray): ByteArray {
            val dst = ByteArray(requiredLengthForRadix64Array(r.size))
            encodeByteArray(r, dst, 0)
            return dst
        }
    }

    fun addByteArray(r: ByteArray): Radix64LowLevelEncoder {
        val dst = ofByteArray(r)
        this.contents = Radix64EncoderComponent(dst, this.contents)
        this.contentLength += dst.size
        this.ogContentLength += r.size
        return this
    }

    fun addString(s: String): Radix64LowLevelEncoder {
        return this.addByteArray(convertStringToByteArray(s))
    }

    fun addSubArray(e: Radix64Encoder): Radix64LowLevelEncoder {
        val contentLength = e.getFullContentLength()
        // contentLength == 0 could still mean an empty byte array.
        val component = if (e.hasContents()) {
            val result = ByteArray(contentLength + 2)
            result[0] = arrayStartElement
            result[result.size - 1] = arrayEndElement
            e.encodeInto(result, 1)
            result
        } else {
            byteArrayOf(arrayStartElement, arrayEmptyElement, arrayEndElement)
        }
        this.contents = Radix64EncoderComponent(component, this.contents)
        this.contentLength += component.size
        this.ogContentLength += e.getOriginalContentLength()
        return this
    }

    fun clone(): Radix64LowLevelEncoder {
        val replacement = Radix64LowLevelEncoder()
        replacement.contents = this.contents
        replacement.contentLength = this.contentLength
        replacement.ogContentLength = this.ogContentLength
        return replacement
    }
}

private fun decodeByteArray(src: ByteArray, srcOffset: Int): Pair<ByteArray, Int> {
    var curOffset = srcOffset + 1
    while (src[curOffset].toInt() != 0) {
        curOffset++
    }
    val encodedSize = curOffset - srcOffset - 1
    val modLength = encodedSize.mod(4)
    var decodedSize = encodedSize / 4 * 3
    when (modLength) {
        3 -> {
            decodedSize += 2
        }
        2 -> {
            decodedSize += 1
        }
        1 -> {
            throw IllegalArgumentException("Invalid encoding length by way of mod length $encodedSize")
        }
    }
    val result = ByteArray(decodedSize)
    val endOffset = curOffset - modLength
    curOffset = srcOffset + 1
    var dstOffset = 0
    if (encodedSize >= 4) {
        while (curOffset < endOffset) {
            val src0 = src[curOffset].toInt().and(0x7e).shr(1)
            val src1 = src[curOffset + 1].toInt().and(0x7e).shr(1)
            val src2 = src[curOffset + 2].toInt().and(0x7e).shr(1)
            val src3 = src[curOffset + 3].toInt().and(0x7e).shr(1)
            val dst0 = src0.shl(2).or(src1.shr(4)).toByte()
            val dst1 = src1.shl(4).or(src2.shr(2)).toByte()
            val dst2 = src2.shl(6).or(src3).toByte()
            result[dstOffset] = dst0
            result[dstOffset + 1] = dst1
            result[dstOffset + 2] = dst2
            curOffset += 4
            dstOffset += 3
        }
    }
    when (modLength) {
        3 -> {
            curOffset = endOffset
            val src0 = src[curOffset].toInt().and(0x7e).shr(1)
            val src1 = src[curOffset + 1].toInt().and(0x7e).shr(1)
            val src2 = src[curOffset + 2].toInt().and(0x7e).shr(3)
            val dst0 = src0.shl(2).or(src1.shr(4)).toByte()
            val dst1 = src1.shl(4).or(src2).toByte()
            result[dstOffset] = dst0
            result[dstOffset + 1] = dst1
        }
        2 -> {
            curOffset = endOffset
            val src0 = src[curOffset].toInt().and(0x7e).shr(1)
            val src1 = src[curOffset + 1].toInt().and(0x7e).shr(5)
            val dst0 = src0.shl(2).or(src1).toByte()
            result[dstOffset] = dst0
        }
    }
    return Pair(result, endOffset + modLength + 1)
}

class Radix64LowLevelDecoder(
    private val bytes: ByteArray
) {
    private var offset: Int = 0

    fun withByteArray(fn: (r: ByteArray) -> Unit): Boolean {
        if (this.offset >= this.bytes.size) {
            return false
        }
        val firstByte = this.bytes[this.offset]
        if (firstByte == 0x00.toByte()) {
            // We're at the end of a subArray and need to return upwards.
            return false
        }
        if (firstByte != 0xff.toByte()) {
            throw IllegalArgumentException("Invalid start of encoded element")
        }
        // This might be the start of a sub-array. We can't execute on those.
        if (this.offset + 1 >= this.bytes.size) {
            return false
        }
        val nextByte = this.bytes[this.offset + 1]
        if (nextByte == 0xff.toByte() || nextByte == 0x81.toByte()) {
            // This is, in fact, a sub-array, so we have to bail.
            return false
        }
        val (byteArray, nextOffset) = decodeByteArray(this.bytes, this.offset)
        this.offset = nextOffset
        fn(byteArray)
        return true
    }

    fun withString(fn: (s: String) -> Unit): Boolean {
        return this.withByteArray {
            fn(convertByteArrayToString(it))
        }
    }

    fun withSubArray(fn: (d: Radix64LowLevelDecoder) -> Unit): Boolean {
        if (this.offset >= this.bytes.size) {
            return false
        }
        val firstByte = this.bytes[this.offset]
        if (firstByte != 0xff.toByte()) {
            return false
        }
        if (this.offset + 1 >= this.bytes.size) {
            return false
        }

        val nextByte = this.bytes[this.offset + 1]
        if (nextByte == 0x81.toByte()) {
            // We assume a form 0xff 0x81 0x00, which means "empty subArray"
            fn(Radix64LowLevelDecoder(byteArrayOf()))
            this.offset += 3
            return true
        }
        if (nextByte != 0xff.toByte()) {
            return false
        }
        val decoder = Radix64LowLevelDecoder(this.bytes)
        decoder.offset = this.offset + 1
        fn(decoder)
        this.offset = decoder.offset + 1
        return true
    }
}

private val emptyByteBuffer = byteArrayOf()

internal fun fitStringIntoRemainingBytes(s: String, byteBudget: Int): Pair<Int, ByteArray> {
    // Initial allocation. If ba.size <= bs, we can just return here.
    val result = convertStringToByteArray(s)
    if (result.size <= byteBudget) {
        return result.size to result
    }
    if (byteBudget <= 0) {
        return result.size to emptyByteBuffer
    }

    // The resulting ByteArray is going to be a valid UTF-8 string, which means
    // that if we simply truncate the ByteArray, we might end up in the middle of
    // a multibyte character sequence. We want to make sure that the resulting
    // ByteArray only encodes valid multibyte character sequences, so we'll have to
    // stop slightly earlier.

    // UTF-8 byte sequences look like
    //     0xxxxxxx
    //     110xxxxx 10xxxxxx
    //     1110xxxx 10xxxxxx 10xxxxxx
    //     11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
    // If the highest bit of the last byte isn't set, we can safely use everything up to and including
    // that byte. Otherwise, we have to move backwards to find the byte indicating the length of the
    // multibyte sequence. If the full multibyte sequence can fit in the budget, that's where we split
    // the full byte buffer; otherwise, we split it at the byte just before the start of the soon-to-be-incomplete
    // multibyte sequence.

    var lastByteInclusive = byteBudget - 1
    if (result[lastByteInclusive].toInt().and(0x80) != 0) {
        while (lastByteInclusive >= 0 && result[lastByteInclusive].toInt().and(0xc0) == 0x80) {
            lastByteInclusive--
        }
        if (lastByteInclusive >= 0) {
            val lengthByte = result[lastByteInclusive].toInt()
            val nBytes = Integer.numberOfLeadingZeros(lengthByte.inv().and(0xff)) - 24
            lastByteInclusive -= 1
            val proposedEndInclusive = lastByteInclusive + nBytes
            if (proposedEndInclusive < byteBudget) {
                lastByteInclusive = proposedEndInclusive
            }
        }
    }
    return if (lastByteInclusive < 0) {
        result.size to emptyByteBuffer
    } else {
        result.size to result.copyOf(lastByteInclusive + 1)
    }
}

internal class Radix64JsonEncoder : Radix64Encoder() {
    companion object {
        private val numberEncodeScratch = ThreadLocal.withInitial { ByteArray(8) }

        private fun doubleIntoNumberEncodeScratch(d: Double) {
            // It's a negative value, we have to xor everything before passing in the full double.
            // That's the same as just an inversion operation.
            val l = java.lang.Double.doubleToLongBits(d)
            val writing = if (l < 0) { l.inv() } else { l.or(Long.MIN_VALUE) }
            convertLongIntoGivenByteArray(writing, this.numberEncodeScratch.get())
        }

        const val NULL_TAG = 0x00.toByte()
        const val BOOLEAN_TAG = 0x01.toByte()
        const val NUMBER_TAG = 0x02.toByte()
        const val STRING_TAG = 0x03.toByte()

        private val ARRAY_START = byteArrayOf(arrayStartElement)
        private val ARRAY_END = byteArrayOf(arrayEndElement)
        private val ARRAY_START_END = byteArrayOf(arrayStartElement, arrayEndElement)
        internal val NULL_VALUE = ARRAY_START + encodeSingleByteArray(byteArrayOf(NULL_TAG)) + byteArrayOf(arrayEndElement)
        private val BOOLEAN_PREFIX = ARRAY_START + encodeSingleByteArray(byteArrayOf(BOOLEAN_TAG))
        private val NUMBER_PREFIX = ARRAY_START + encodeSingleByteArray(byteArrayOf(NUMBER_TAG))
        private val STRING_PREFIX = ARRAY_START + encodeSingleByteArray(byteArrayOf(STRING_TAG))

        private val PREFIX_TAG_LENGTH = STRING_PREFIX.size

        private val FALSE_VALUE = BOOLEAN_PREFIX + encodeSingleByteArray(byteArrayOf(0x00)) + ARRAY_END
        private val TRUE_VALUE = BOOLEAN_PREFIX + encodeSingleByteArray(byteArrayOf(0x01)) + ARRAY_END

        private val NUMBER_SUFFIX_SIZE = requiredLengthForRadix64Array(8)  + 1
        private val IN_BUDGET_SUFFIX = byteArrayOf(arrayEndElement, arrayStartElement, arrayEndElement, arrayEndElement)
        private val MIN_IN_BUDGET_STRING_SIZE = STRING_PREFIX.size + ARRAY_START.size + IN_BUDGET_SUFFIX.size

        fun ofNull(): ByteArray {
            return NULL_VALUE
        }

        fun ofBoolean(b: Boolean): ByteArray {
            return if (b) { TRUE_VALUE } else { FALSE_VALUE }
        }

        fun ofNumber(n: Double): ByteArray {
            // IEEE 754 has a bit of a weird positive-negative situation: unlike virtually every CPU architecture's
            // treatment of signed integrals, it's not any sort of complemented, it's just a "negative" bit.
            //
            // We need to ensure that positive values are greater than negative values, and higher negative values are less
            // than lower negative values. So we run the complement ourselves, but only a one's complement; two's complement
            // exists to ensure no ambiguity with respect to zero but that ambiguity is baked into the IEEE 754 definition.
            //
            // This same trick is used by various FoundationDB layer libraries, which is where we got this idea.
            val result = ByteArray(NUMBER_PREFIX.size + NUMBER_SUFFIX_SIZE)
            NUMBER_PREFIX.copyInto(result, 0)
            result[result.size - 1] = arrayEndElement
            doubleIntoNumberEncodeScratch(n)

            encodeByteArray(this.numberEncodeScratch.get(), result, NUMBER_PREFIX.size)
            return result
        }

        private fun ofBudgetFitString(fullByteLength: Int, result: ByteArray): ByteArray {
            val encodedResult = encodeSingleByteArray(result)
            return STRING_PREFIX + if (fullByteLength <= result.size) {
                // We want to signal that we've managed to encode the entire string,
                // which will prevent us from matching on an "equality" check for a string
                // that has been truncated. We don't need any special third value in this
                // triple, we just need a third value at all, and an empty byte array works
                // just fine for this purpose.
                encodedResult + ARRAY_START_END
            } else {
                encodedResult
            } + ARRAY_END
        }

        fun ofString(s: String, byteBudget: Int): ByteArray {
            val (fullByteLength, result) = fitStringIntoRemainingBytes(s, byteBudget)
            return ofBudgetFitString(fullByteLength, result)
        }

        fun isString(ba: ByteArray): Boolean {
            val stringPrefixLength = STRING_PREFIX.size
            return Arrays.equals(
                ba,
                0,
                stringPrefixLength.coerceAtMost(ba.size),
                STRING_PREFIX,
                0,
                stringPrefixLength
            )
        }

        fun isStringWithinBudget(ba: ByteArray): Boolean {
            if (ba.size < MIN_IN_BUDGET_STRING_SIZE) {
                return false
            }
            if (!isString(ba)) {
                return false
            }
            return Arrays.equals(
                ba,
                ba.size - IN_BUDGET_SUFFIX.size,
                ba.size,
                IN_BUDGET_SUFFIX,
                0,
                IN_BUDGET_SUFFIX.size
            )
        }

        fun removeInBudgetSuffixFromString(ba: ByteArray): ByteArray {
            return if (!isStringWithinBudget(ba)) {
                ba
            } else {
                ba.copyOfRange(0, ba.size - IN_BUDGET_SUFFIX.size)
            }
        }

        fun contentByteLengthAssumeNoEndElement(ba: ByteArray): Int {
            // All the prefixes have the exact same length, so we'll use
            // STRING_PREFIX here as the size basis. The -1 is due to the
            // ARRAY_START element, but we're assuming (as part of the definition)
            // that there is no ARRAY_END element.
            return (ba.size - STRING_PREFIX.size - 1).coerceAtLeast(0)
        }

        fun encodedValuesHaveSameType(lhs: ByteArray, rhs: ByteArray): Boolean {
            val lhsSize = PREFIX_TAG_LENGTH.coerceAtMost(lhs.size)
            val rhsSize = PREFIX_TAG_LENGTH.coerceAtMost(rhs.size)
            return Arrays.equals(lhs, 0, lhsSize, rhs, 0, rhsSize)
        }
    }

    fun addNull(): Radix64JsonEncoder {
        this.contents = Radix64EncoderComponent(ofNull(), this.contents)
        this.contentLength += NULL_VALUE.size
        // Null bytes should still count as bytes, after all...
        this.ogContentLength += 1
        return this
    }

    fun addBoolean(b: Boolean): Radix64JsonEncoder {
        val value = ofBoolean(b)
        this.contents = Radix64EncoderComponent(value, this.contents)
        this.contentLength += value.size
        this.ogContentLength += 1
        return this
    }

    fun addNumber(n: Double): Radix64JsonEncoder {
        val value = ofNumber(n)
        this.contents = Radix64EncoderComponent(value, this.contents)
        this.contentLength += value.size
        this.ogContentLength += 8
        return this
    }

    fun addString(s: String, byteBudget: Int): Radix64JsonEncoder {
        val (fullByteLength, result) = fitStringIntoRemainingBytes(s, byteBudget)
        val value = ofBudgetFitString(fullByteLength, result)
        this.contents = Radix64EncoderComponent(value, this.contents)
        this.contentLength += value.size
        this.ogContentLength += result.size
        return this
    }

    fun clone(): Radix64JsonEncoder {
        val replacement = Radix64JsonEncoder()
        replacement.contents = this.contents
        replacement.contentLength = this.contentLength
        replacement.ogContentLength = this.ogContentLength
        return replacement
    }
}