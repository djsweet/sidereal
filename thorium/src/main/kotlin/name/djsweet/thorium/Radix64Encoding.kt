package name.djsweet.thorium

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

    fun getFullContentLength(): Int {
        return this.contentLength
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
    fun addByteArray(r: ByteArray): Radix64LowLevelEncoder {
        val dst = ByteArray(requiredLengthForRadix64Array(r.size))
        encodeByteArray(r, dst, 0)
        this.contents = Radix64EncoderComponent(dst, this.contents)
        this.contentLength += dst.size
        return this
    }

    fun addString(s: String): Radix64LowLevelEncoder {
        return this.addByteArray(s.encodeToByteArray())
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
        return this
    }

    fun clone(): Radix64LowLevelEncoder {
        val replacement = Radix64LowLevelEncoder()
        replacement.contents = this.contents
        replacement.contentLength = this.contentLength
        return replacement
    }
}

internal fun fitStringIntoRemainingBytes(s: String, byteBudget: Int): Pair<Int, ByteArray> {
    // Initial allocation. If ba.size <= bs, we can just return here.
    var result = s.encodeToByteArray()
    if (result.size <= byteBudget) {
        return result.size to result
    }
    if (byteBudget == 0) {
        return result.size to byteArrayOf()
    }
    val initialSize = result.size
    var lowerBound = 0
    var upperBound = s.length
    while (lowerBound < upperBound) {
        val testPoint = lowerBound + ((upperBound - lowerBound) / 2)
        result = s.encodeToByteArray(0, testPoint)

        if (result.size == byteBudget) {
            return initialSize to result
        }
        if (result.size < byteBudget) {
            lowerBound = testPoint
        } else {
            upperBound = testPoint
        }
    }
    val lowerResult = result
    val higherResult = s.encodeToByteArray(0, upperBound)
    return if (higherResult.size <= byteBudget) {
        initialSize to higherResult
    } else {
        initialSize to lowerResult
    }
}

internal class Radix64JsonEncoder : Radix64Encoder() {
    companion object {
        const val NULL_TAG = 0x00.toByte()
        const val BOOLEAN_TAG = 0x01.toByte()
        const val NUMBER_TAG = 0x02.toByte()
        const val STRING_TAG = 0x03.toByte()

        private val ARRAY_START = byteArrayOf(arrayStartElement)
        internal val ARRAY_END = byteArrayOf(arrayEndElement)
        internal val NULL_VALUE = ARRAY_START + encodeSingleByteArray(byteArrayOf(NULL_TAG)) + byteArrayOf(arrayEndElement)
        internal val BOOLEAN_PREFIX = ARRAY_START + encodeSingleByteArray(byteArrayOf(BOOLEAN_TAG))
        internal val NUMBER_PREFIX = ARRAY_START + encodeSingleByteArray(byteArrayOf(NUMBER_TAG))
        internal val STRING_PREFIX = ARRAY_START + encodeSingleByteArray(byteArrayOf(STRING_TAG))

        internal val FALSE_SUFFIX = encodeSingleByteArray(byteArrayOf(0x00)) + ARRAY_END
        internal val TRUE_SUFFIX = encodeSingleByteArray(byteArrayOf(0x01)) + ARRAY_END

        internal val NUMBER_SUFFIX_SIZE = requiredLengthForRadix64Array(8)  + 1
    }

    private val numberEncodeScratch = ByteArray(8)

    fun addNull(): Radix64JsonEncoder {
        this.contents = Radix64EncoderComponent(NULL_VALUE, this.contents)
        this.contentLength += NULL_VALUE.size
        return this
    }

    fun addBoolean(b: Boolean): Radix64JsonEncoder {
        var addedLength = BOOLEAN_PREFIX.size
        this.contents = Radix64EncoderComponent(BOOLEAN_PREFIX, this.contents)
        if (b) {
            addedLength += TRUE_SUFFIX.size
            this.contents = Radix64EncoderComponent(TRUE_SUFFIX, this.contents)
        } else {
            addedLength += FALSE_SUFFIX.size
            this.contents = Radix64EncoderComponent(FALSE_SUFFIX, this.contents)
        }
        this.contentLength += addedLength
        return this
    }

    private fun longIntoNumberEncodeScratch(l: Long) {
        this.numberEncodeScratch[0] = (l shr 56).toByte()
        this.numberEncodeScratch[1] = ((l shr 48) and 0xff).toByte()
        this.numberEncodeScratch[2] = ((l shr 40) and 0xff).toByte()
        this.numberEncodeScratch[3] = ((l shr 32) and 0xff).toByte()
        this.numberEncodeScratch[4] = ((l shr 24) and 0xff).toByte()
        this.numberEncodeScratch[5] = ((l shr 16) and 0xff).toByte()
        this.numberEncodeScratch[6] = ((l shr 8) and 0xff).toByte()
        this.numberEncodeScratch[7] = (l and 0xff).toByte()

        if (this.numberEncodeScratch[0] < 0) {
            // Negative bit is active, we need to ones complement everything
            this.numberEncodeScratch[0] = this.numberEncodeScratch[0].toInt().and(0xff).xor(0xff).toByte()
            this.numberEncodeScratch[1] = this.numberEncodeScratch[1].toInt().and(0xff).xor(0xff).toByte()
            this.numberEncodeScratch[2] = this.numberEncodeScratch[2].toInt().and(0xff).xor(0xff).toByte()
            this.numberEncodeScratch[3] = this.numberEncodeScratch[3].toInt().and(0xff).xor(0xff).toByte()
            this.numberEncodeScratch[4] = this.numberEncodeScratch[4].toInt().and(0xff).xor(0xff).toByte()
            this.numberEncodeScratch[5] = this.numberEncodeScratch[5].toInt().and(0xff).xor(0xff).toByte()
            this.numberEncodeScratch[6] = this.numberEncodeScratch[6].toInt().and(0xff).xor(0xff).toByte()
            this.numberEncodeScratch[7] = this.numberEncodeScratch[7].toInt().and(0xff).xor(0xff).toByte()
        } else {
            this.numberEncodeScratch[0] = this.numberEncodeScratch[0].toInt().and(0xff).or(0x80).toByte()
        }
    }

    fun addNumber(n: Double): Radix64JsonEncoder {
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
        this.longIntoNumberEncodeScratch(java.lang.Double.doubleToLongBits(n))

        encodeByteArray(this.numberEncodeScratch, result, NUMBER_PREFIX.size)
        this.contents = Radix64EncoderComponent(result, this.contents)
        this.contentLength += result.size
        return this
    }

    fun addString(s: String, byteBudget: Int): Radix64JsonEncoder {
        var addedLength = STRING_PREFIX.size
        val (fullByteLength, result) = fitStringIntoRemainingBytes(s, byteBudget)
        val encodedResult = encodeSingleByteArray(result)
        addedLength += encodedResult.size
        this.contents = Radix64EncoderComponent(STRING_PREFIX, this.contents)
        this.contents = Radix64EncoderComponent(encodeSingleByteArray(result), this.contents)
        if (fullByteLength <= result.size) {
            this.longIntoNumberEncodeScratch(s.length.toLong())
            val sizeEnd = encodeSingleByteArray(this.numberEncodeScratch)
            addedLength += sizeEnd.size
            this.contents = Radix64EncoderComponent(sizeEnd, this.contents)
        }
        this.contents = Radix64EncoderComponent(ARRAY_END, this.contents)
        addedLength += ARRAY_END.size
        this.contentLength += addedLength
        return this
    }

    fun clone(): Radix64JsonEncoder {
        val replacement = Radix64JsonEncoder()
        replacement.contents = this.contents
        replacement.contentLength = this.contentLength
        return replacement
    }
}