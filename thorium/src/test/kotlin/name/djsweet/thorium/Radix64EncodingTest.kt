package name.djsweet.thorium

import org.junit.jupiter.api.Assertions.*
import net.jqwik.api.*
import org.junit.jupiter.api.Test
import java.nio.charset.Charset
import java.util.*

private fun decodeByteArray(src: ByteArray, srcOffset: Int): Pair<ByteArray, Int> {
    var curOffset = srcOffset + 1
    while (src[curOffset].toInt() != 0) {
        curOffset++
    }
    val encodedSize = curOffset - srcOffset - 1
    // println("encoded size is $encodedSize")
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

internal class Radix64LowLevelDecoder(
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
            println("${this.offset} ${this.bytes.toList()}")
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
        // println("decoded ${this.offset} $nextOffset ${this.bytes.toList()}")
        this.offset = nextOffset
        fn(byteArray)
        return true
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

private fun byteArrayListsAreEqual(l: List<ByteArray>, r: List<ByteArray>): Boolean {
    if (l.size != r.size) {
        return false
    }
    for (i in l.indices) {
        val left = l[i]
        val right = r[i]
        if (!left.contentEquals(right)) {
            return false
        }
    }
    return true
}

class LowLevelEncodingSpec private constructor(
    internal var byteArrays: List<ByteArray>?,
    internal var subArrays: List<LowLevelEncodingSpec>?
): Comparable<LowLevelEncodingSpec> {
    companion object {
        fun ofByteArrays(byteArrays: Iterable<ByteArray>): LowLevelEncodingSpec {
            return LowLevelEncodingSpec(byteArrays.toList(), null)
        }

        fun ofSubArrays(subArrays: Iterable<LowLevelEncodingSpec>): LowLevelEncodingSpec {
            val subArraysList = subArrays.toList()
            return if (subArraysList.isEmpty()) {
                // Tests will sometimes generate empty subArray lists.
                // We can't disambiguate between these empty cases, so we'll say that empty byte array is canonical
                LowLevelEncodingSpec(listOf(), null)
            } else {
                LowLevelEncodingSpec(null, subArraysList)
            }
        }

        private fun decodeRecurse(parent: LowLevelEncodingSpec, parentDecoder: Radix64LowLevelDecoder) {
            val specs = mutableListOf<LowLevelEncodingSpec>()
            var didSubArray = false
            while (parentDecoder.withSubArray {
                val subSpec = LowLevelEncodingSpec()
                decodeRecurse(subSpec, it)
                specs.add(subSpec)
            }) {
                didSubArray = true
            }
            if (didSubArray) {
                parent.byteArrays = null
                parent.subArrays = specs
                return
            }
            val byteArrays = mutableListOf<ByteArray>()
            while (parentDecoder.withByteArray {
                byteArrays.add(it)
            }) {
                // We aren't doing anything in the body.
            }
            parent.byteArrays = byteArrays
            parent.subArrays = null
        }

        fun decode(byteArray: ByteArray): LowLevelEncodingSpec {
            val decoder = Radix64LowLevelDecoder(byteArray)
            val result = LowLevelEncodingSpec()
            decodeRecurse(result, decoder)
            return result
        }
    }

    private constructor(): this(null, null)

    private fun encodeRecurse(encoder: Radix64LowLevelEncoder) {
        val byteArrays = this.byteArrays
        val subArrays = this.subArrays
        if (byteArrays != null) {
            for (byteArray in byteArrays) {
                encoder.addByteArray(byteArray)
            }
        } else if (subArrays != null) {
            for (subArray in subArrays) {
                val subEncoder = Radix64LowLevelEncoder()
                subArray.encodeRecurse(subEncoder)
                encoder.addSubArray(subEncoder)
            }
        }
    }

    fun encode(): ByteArray {
        val encoder = Radix64LowLevelEncoder()
        this.encodeRecurse(encoder)
        return encoder.encode()
    }

    override fun equals(other: Any?): Boolean {
        return if (other is LowLevelEncodingSpec) {
            val thisByteArrays = this.byteArrays
            val otherByteArrays = other.byteArrays
            val thisSubArrays = this.subArrays
            val otherSubArrays = other.subArrays
            val byteArraysEqual = (thisByteArrays == null && otherByteArrays == null) ||
                    (thisByteArrays != null && otherByteArrays != null &&
                            byteArrayListsAreEqual(thisByteArrays, otherByteArrays))
            val subArraysEqual = (thisSubArrays == null && otherSubArrays == null) ||
                    (thisSubArrays != null && otherSubArrays != null && thisSubArrays == otherSubArrays)
            byteArraysEqual && subArraysEqual
        } else {
            super.equals(other)
        }
    }

    override fun hashCode(): Int {
        var result = byteArrays?.hashCode() ?: 0
        result = 31 * result + (subArrays?.hashCode() ?: 0)
        return result
    }

    override fun toString(): String {
        return "LowLevelEncodingSpec(byteArrays=${this.byteArrays?.map { it.toList() }}, subArrays=${this.subArrays?.map { it.toString() }})"
    }

    private fun compareByteArrayLists(left: List<ByteArray>, right: List<ByteArray>): Int {
        val maxLength = left.size.coerceAtMost(right.size)
        for (i in 0 until maxLength) {
            val leftBytes = left[i]
            val rightBytes = right[i]
            val arrayCompare = Arrays.compareUnsigned(leftBytes, rightBytes)
            if (arrayCompare != 0) {
                return arrayCompare
            }
        }
        return left.size.compareTo(right.size)
    }

    override fun compareTo(other: LowLevelEncodingSpec): Int {
        if (this == other) {
            return 0
        }
        val thisByteArrays = this.byteArrays
        val thisSubArrays = this.subArrays
        val otherByteArrays = other.byteArrays
        val otherSubArrays = other.subArrays
        if (thisByteArrays == null && thisSubArrays == null) {
            return -1
        }
        if (thisByteArrays != null) {
            if (otherSubArrays != null) {
                return -1
            }
            if (otherByteArrays == null) {
                return 1
            }
            return this.compareByteArrayLists(thisByteArrays, otherByteArrays)
        }
        if (otherSubArrays == null) {
            return 1
        }
        val maxLength = thisSubArrays!!.size.coerceAtMost(otherSubArrays.size)
        for (i in 0 until maxLength) {
            val left = thisSubArrays[i]
            val right = otherSubArrays[i]
            val subArrayCompare = left.compareTo(right)
            if (subArrayCompare != 0) {
                return subArrayCompare
            }
        }
        return thisSubArrays.size.compareTo(otherSubArrays.size)
    }
}

internal class Radix64JsonDecoder(bytes: ByteArray) {
    private val lowLevelDecoder: Radix64LowLevelDecoder
    private var currentTagValue: Triple<ByteArray, ByteArray, Int>? = null

    init {
        this.lowLevelDecoder = Radix64LowLevelDecoder(bytes)
    }

    private fun fillCurrentTagValueIfNecessary() {
        if (this.currentTagValue != null) {
            return
        }
        this.lowLevelDecoder.withSubArray { sub ->
            var savedTag: ByteArray? = null
            sub.withByteArray { tag ->
                if (tag.size == 1) {
                    savedTag = tag
                }
            }
            if (savedTag != null) {
                var savedValue: ByteArray? = null
                if (!sub.withByteArray { value ->
                    savedValue = value
                }) {
                    this.currentTagValue = Triple(savedTag!!, byteArrayOf(), 1)
                }
                var byteArrayCount = 2
                while (sub.withByteArray {  }) {
                    byteArrayCount++
                }
                if (sub.withSubArray {  }) {
                    throw Error("Unexpected subArray in JSON encoding")
                }
                this.currentTagValue = Triple(savedTag!!, savedValue ?: byteArrayOf(), byteArrayCount)
            }
        }
    }

    fun withNull(fn: () -> Unit): Boolean {
        this.fillCurrentTagValueIfNecessary()
        val currentTagValue = this.currentTagValue ?: return false
        val (tag) = currentTagValue
        if (tag[0] != Radix64JsonEncoder.NULL_TAG) {
            return false
        }
        fn()
        this.currentTagValue = null
        return true
    }

    fun withBoolean(fn: (b: Boolean) -> Unit): Boolean {
        this.fillCurrentTagValueIfNecessary()
        val currentTagValue = this.currentTagValue ?: return false
        val (tag, value) = currentTagValue
        if (tag[0] != Radix64JsonEncoder.BOOLEAN_TAG) {
            return false
        }
        fn(!value.contentEquals(byteArrayOf(0x00)))
        this.currentTagValue = null
        return true
    }

    fun withNumber(fn: (d: Double) -> Unit): Boolean {
        this.fillCurrentTagValueIfNecessary()
        val currentTagValue = this.currentTagValue ?: return false
        val (tag, value) = currentTagValue
        if (tag[0] != Radix64JsonEncoder.NUMBER_TAG) {
            return false
        }
        if (value.size < 8) {
            return false
        }

        val byte0 = value[0].toLong().and(0xff).shl(56)
        val byte1 = value[1].toLong().and(0xff).shl(48)
        val byte2 = value[2].toLong().and(0xff).shl(40)
        val byte3 = value[3].toLong().and(0xff).shl(32)
        val byte4 = value[4].toLong().and(0xff).shl(24)
        val byte5 = value[5].toLong().and(0xff).shl(16)
        val byte6 = value[6].toLong().and(0xff).shl(8)
        val byte7 = value[7].toLong().and(0xff)

        val byte01 = byte0.or(byte1)
        val byte23 = byte2.or(byte3)
        val byte45 = byte4.or(byte5)
        val byte67 = byte6.or(byte7)
        val byte03 = byte01.or(byte23)
        val byte47 = byte45.or(byte67)
        val almost = byte03.or(byte47)
        val asDouble = if (byte0.shr(56).and(0x80) == 0L) {
            // It's a negative value, we have to xor everything before passing in the full double.
            // But, Kotlin won't let us xor the full 64 set bits without complaining. So we'll instead
            // leverage the two's complement operation of negation, which is:
            // 1. Inverting all the bits (this is what we want)
            // 2. Adding 1 to the number
            //
            // All we have to do to mitigate 2 is subtract 1 from the result.
            Double.fromBits(-almost - 1)
        } else {
            Double.fromBits(almost.and(0x7fffffffffffffff))
        }
        fn(asDouble)
        this.currentTagValue = null
        return true
    }

    fun withString(fn: (s: String, full: Boolean) -> Unit): Boolean {
        this.fillCurrentTagValueIfNecessary()
        val currentTagValue = this.currentTagValue ?: return false
        val (tag, value, arrayCount) = currentTagValue
        if (tag[0] != Radix64JsonEncoder.STRING_TAG) {
            return false
        }

        val valueAsString = value.toString(Charset.forName("utf-8"))
        fn(valueAsString, arrayCount > 2)

        this.currentTagValue = null
        return true
    }
}

class JsonEncodingSpec private constructor(
    val isNull: Boolean,
    val booleanValue: Boolean,
    val numberValue: Double?,
    val stringValue: String?,
    val stringByteLength: Int,
    val stringIsFull: Boolean,
) {
    companion object {
        fun ofNull(): JsonEncodingSpec {
            return JsonEncodingSpec(isNull = true, false, null, null, 0, true)
        }

        fun ofBoolean(v: Boolean): JsonEncodingSpec {
            return JsonEncodingSpec(isNull = false, v, null, null, 0, true)
        }

        fun ofNumber(d: Double): JsonEncodingSpec {
            return JsonEncodingSpec(isNull = false, false, d, null, 0, true)
        }

        fun ofString(s: String, desiredLength: Int): JsonEncodingSpec {
            return ofString(s, desiredLength, desiredLength == s.length)
        }

        private fun ofString(s: String, length: Int, full: Boolean): JsonEncodingSpec {
            val stringAsBytes = s.encodeToByteArray(0, length)
            return JsonEncodingSpec(isNull = false, false, null, s, stringAsBytes.size, full)
        }

        fun decode(b: ByteArray): List<JsonEncodingSpec> {
            val results = mutableListOf<JsonEncodingSpec>()
            val decoder = Radix64JsonDecoder(b)
            var decoding = true
            while (decoding) {
                decoding = false
                if (decoder.withNull {
                    results.add(ofNull())
                }) {
                    decoding = true
                    continue
                }
                if (decoder.withBoolean {
                    results.add(ofBoolean(it))
                }) {
                    decoding = true
                    continue
                }
                if (decoder.withNumber {
                    results.add(ofNumber(it))
                }) {
                    decoding = true
                    continue
                }
                if (decoder.withString { s, full ->
                    results.add(ofString(s, s.length, full))
                }) {
                    decoding = true
                }
            }
            return results
        }
    }

    internal fun encode(enc: Radix64JsonEncoder): JsonEncodingSpec {
        if (this.isNull) {
            enc.addNull()
        } else if (this.numberValue != null) {
            enc.addNumber(this.numberValue)
        } else if (this.stringValue != null) {
            enc.addString(this.stringValue, this.stringByteLength)
        } else {
            enc.addBoolean(this.booleanValue)
        }
        return this
    }

    override fun equals(other: Any?): Boolean {
        if (other !is JsonEncodingSpec) {
            return super.equals(other)
        }
        if (this.isNull != other.isNull) {
            return false
        }
        if (this.booleanValue != other.booleanValue) {
            return false
        }
        if (this.stringByteLength != other.stringByteLength) {
            return false
        }
        if (this.stringIsFull != other.stringIsFull) {
            return false
        }
        if (this.numberValue == null) {
            if (other.numberValue != null) {
                return false
            }
        } else {
            if (other.numberValue == null) {
                return false
            }
            if (this.numberValue != other.numberValue) {
                return false
            }
        }
        return if (this.stringValue == null) {
            other.stringValue == null
        } else if (other.stringValue == null) {
            false
        } else {
            val thisByteString = this.stringValue.encodeToByteArray()
            val otherByteString = other.stringValue.encodeToByteArray()
            Arrays.equals(thisByteString, 0, this.stringByteLength, otherByteString, 0, other.stringByteLength)
        }
    }

    override fun hashCode(): Int {
        var result = isNull.hashCode()
        result = 31 * result + booleanValue.hashCode()
        result = 31 * result + (numberValue?.hashCode() ?: 0)
        val stringBytes = this.stringValue?.encodeToByteArray()?.slice(0 until stringByteLength)
        result = 31 * result + (stringBytes?.hashCode() ?: 0)
        result = 31 * result + stringByteLength.hashCode()
        result = 31 * result + stringIsFull.hashCode()
        return result
    }

    override fun toString(): String {
        return "JsonEncodingSpec(isNull=${this.isNull} booleanValue=${this.booleanValue} numberValue=${this.numberValue} stringValue=\"${this.stringValue}\" stringLength=${this.stringByteLength} fullString=${this.stringIsFull})"
    }
}

class Radix64EncodingTest {
    private val encodedByteArraysArbitrary = Arbitraries.bytes().array(ByteArray::class.java).list().map {
        br -> LowLevelEncodingSpec.ofByteArrays(br)
    }

    private fun lowLevelEncodingSpecWithRecursionGuard(maxDepth: Int): Arbitrary<LowLevelEncodingSpec> {
        return if (maxDepth <= 0) {
            this.encodedByteArraysArbitrary
        } else {
            Arbitraries.oneOf(listOf(
                this.encodedByteArraysArbitrary,
                lowLevelEncodingSpecWithRecursionGuard(maxDepth - 1)
                    .list()
                    .ofMinSize(0)
                    .ofMaxSize(7)
                    .map { subs -> LowLevelEncodingSpec.ofSubArrays(subs) }
            ))
        }
    }

    @Provide
    fun lowLevelEncodingSpec(): Arbitrary<LowLevelEncodingSpec> {
        return Arbitraries.integers().between(0, 5).flatMap { this.lowLevelEncodingSpecWithRecursionGuard(it) }
    }

    @Provide
    fun jsonEncodingSpecs(): Arbitrary<List<JsonEncodingSpec>> {
        return Arbitraries.oneOf(listOf(
            Arbitraries.just(JsonEncodingSpec.ofNull()),
            Arbitraries.oneOf(listOf(Arbitraries.just(true), Arbitraries.just(false))).map { JsonEncodingSpec.ofBoolean(it) },
            Arbitraries.doubles().map { JsonEncodingSpec.ofNumber(it) },
            Arbitraries.strings().flatMap { s -> Arbitraries.integers().between(0, s.length).map { JsonEncodingSpec.ofString(s, it) } }
        )).list()
    }

    @Property
    fun lowLevelEncodingAndDecoding(
        @ForAll @From("lowLevelEncodingSpec") spec: LowLevelEncodingSpec
    ) {
        val encoded = spec.encode()
        // println("byte arrays were ${spec.byteArrays?.map { r -> r.toList() }}")
        val decoded = LowLevelEncodingSpec.decode(encoded)
        assertEquals(spec, decoded)
        assertEquals(decoded, spec)
    }

    @Property
    fun lowLevelStringEncoding(
        @ForAll s: String
    ) {
        val encoder = Radix64LowLevelEncoder()
        encoder.addString(s)
        val bytes = encoder.encode()
        val decoded = LowLevelEncodingSpec.decode(bytes)
        assertEquals(1, decoded.byteArrays?.size)
        assertEquals(s.encodeToByteArray().toList(), decoded.byteArrays!![0].toList())
    }

    @Property
    fun lowLevelCloning(
        @ForAll s1: String,
        @ForAll s2: String
    ) {
        val encoder1 = Radix64LowLevelEncoder()
        encoder1.addString(s1)
        val bytes11 = encoder1.encode()

        val encoder2 = encoder1.clone()
        encoder2.addString(s2)
        val bytes2 = encoder2.encode()

        val bytes12 = encoder1.encode()

        val decoded1 = LowLevelEncodingSpec.decode(bytes11)
        val decoded2 = LowLevelEncodingSpec.decode(bytes2)

        assertEquals(bytes11.toList(), bytes12.toList())
        assertTrue(Arrays.compareUnsigned(bytes11, bytes2) < 0)
        assertTrue(Arrays.equals(bytes11, 0, bytes11.size, bytes2, 0, bytes11.size))

        assertEquals(1, decoded1.byteArrays?.size)
        assertEquals(s1.encodeToByteArray().toList(), decoded1.byteArrays!![0].toList())
        assertEquals(2, decoded2.byteArrays?.size)
        assertEquals(s1.encodeToByteArray().toList(), decoded2.byteArrays!![0].toList())
        assertEquals(s2.encodeToByteArray().toList(), decoded2.byteArrays!![1].toList())
    }

    @Test
    fun emptyByteArrayEncodesAndDecodes() {
        val spec = LowLevelEncodingSpec.ofByteArrays(listOf(byteArrayOf()))
        val encoded = spec.encode()
        val decoded = LowLevelEncodingSpec.decode(encoded)

        assertEquals(spec, decoded)
        assertEquals(decoded, spec)
    }

    @Test
    fun zeroByteArrayEncodesAndDecodes() {
        val spec = LowLevelEncodingSpec.ofByteArrays(listOf(byteArrayOf(0)))
        val encoded = spec.encode()
        val decoded = LowLevelEncodingSpec.decode(encoded)
        assertEquals(spec, decoded)
        assertEquals(decoded, spec)
    }

    @Test
    fun zeroOneByteArrayEncodesAndDecodes() {
        val spec = LowLevelEncodingSpec.ofByteArrays(listOf(byteArrayOf(0, -1)))
        val encoded = spec.encode()
        val decoded = LowLevelEncodingSpec.decode(encoded)

        assertEquals(spec, decoded)
        assertEquals(decoded, spec)
    }

    @Test
    fun twoZeroArrays() {
        val spec = LowLevelEncodingSpec.ofByteArrays(listOf(byteArrayOf(0), byteArrayOf(0)))
        val encoded = spec.encode()
        val decoded = LowLevelEncodingSpec.decode(encoded)

        assertEquals(spec, decoded)
        assertEquals(decoded, spec)
    }

    @Test
    fun zeroTwelveNegativeArray() {
        val spec = LowLevelEncodingSpec.ofByteArrays(listOf(byteArrayOf(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1)))
        val encoded = spec.encode()
        val decoded = LowLevelEncodingSpec.decode(encoded)

        assertEquals(spec, decoded)
        assertEquals(decoded, spec)
    }

    @Test
    fun zeroElevenNegativeArray() {
        val spec = LowLevelEncodingSpec.ofByteArrays(listOf(byteArrayOf(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1)))
        val encoded = spec.encode()
        val decoded = LowLevelEncodingSpec.decode(encoded)

        assertEquals(spec, decoded)
        assertEquals(decoded, spec)
    }

    @Test
    fun zeroTenNegativeArray() {
        val spec = LowLevelEncodingSpec.ofByteArrays(listOf(byteArrayOf(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1)))
        val encoded = spec.encode()
        val decoded = LowLevelEncodingSpec.decode(encoded)

        assertEquals(spec, decoded)
        assertEquals(decoded, spec)
    }

    @Test
    fun zeroThreeNegativeArray() {
        val spec = LowLevelEncodingSpec.ofByteArrays(listOf(byteArrayOf(0, 0, 0, -1)))
        val encoded = spec.encode()
        val decoded = LowLevelEncodingSpec.decode(encoded)

        assertEquals(spec, decoded)
        assertEquals(decoded, spec)
    }

    @Test
    fun zeroTwoNegativeArray() {
        val spec = LowLevelEncodingSpec.ofByteArrays(listOf(byteArrayOf(0, 0, -1)))
        val encoded = spec.encode()
        val decoded = LowLevelEncodingSpec.decode(encoded)

        assertEquals(spec, decoded)
        assertEquals(decoded, spec)
    }

    @Test
    fun justSubArrayOfEmptyArray() {
        val spec = LowLevelEncodingSpec.ofSubArrays(
            listOf(
                LowLevelEncodingSpec.ofByteArrays(listOf())
            )
        )
        val encoded = spec.encode()
        val decoded = LowLevelEncodingSpec.decode(encoded)

        assertEquals(spec, decoded)
        assertEquals(decoded, spec)
    }

    @Property
    fun sortOrderIsPreserved(
        @ForAll @From("lowLevelEncodingSpec") leftSpec: LowLevelEncodingSpec,
        @ForAll @From("lowLevelEncodingSpec") rightSpec: LowLevelEncodingSpec,
    ) {
        val leftEncoded = leftSpec.encode()
        val rightEncoded = rightSpec.encode()
        val specCompare = leftSpec.compareTo(rightSpec)
        val bytesCompare = Arrays.compareUnsigned(leftEncoded, rightEncoded)

        val specNormalized = if (specCompare < 0) { -1 } else if (specCompare > 0) { 1 } else { 0 }
        val bytesNormalized = if (bytesCompare < 0) { -1 } else if (bytesCompare > 0) { 1 } else { 0 }
        assertEquals(specNormalized, bytesNormalized)
    }

    @Test
    fun emptySubArraySortsHigherThanEmptyByteArray() {
        val leftSpec = LowLevelEncodingSpec.ofSubArrays(listOf(LowLevelEncodingSpec.ofByteArrays(listOf())))
        val rightSpec = LowLevelEncodingSpec.ofByteArrays(listOf())

        val leftEncoded = leftSpec.encode()
        val rightEncoded = rightSpec.encode()
        val specCompare = leftSpec.compareTo(rightSpec)
        val bytesCompare = Arrays.compareUnsigned(leftEncoded, rightEncoded)

        val specNormalized = if (specCompare < 0) { -1 } else if (specCompare > 0) { 1 } else { 0 }
        val bytesNormalized = if (bytesCompare < 0) { -1 } else if (bytesCompare > 0) { 1 } else { 0 }
        assertEquals(specNormalized, bytesNormalized)
    }

    @Test
    fun fitStringsIntoByteArraySizes() {
        val (actualSize1, result1) = fitStringIntoRemainingBytes("abc", 4)
        assertEquals(3, result1.size)
        assertEquals(3, actualSize1)
        assertEquals("abc".encodeToByteArray().toList(), result1.toList())

        val (actualSize2, result2) = fitStringIntoRemainingBytes("abc", 3)
        assertEquals(3, result2.size)
        assertEquals(3, actualSize2)
        assertEquals("abc".encodeToByteArray().toList(), result2.toList())

        val (actualSize3, result3) = fitStringIntoRemainingBytes("abc", 2)
        assertEquals(2, result3.size)
        assertEquals(3, actualSize3)
        assertEquals("ab".encodeToByteArray().toList(), result3.toList())

        val (actualSize4, result4) = fitStringIntoRemainingBytes("abc", 1)
        assertEquals(1, result4.size)
        assertEquals(3, actualSize4)
        assertEquals("a".encodeToByteArray().toList(), result4.toList())

        val (actualSize5, result5) = fitStringIntoRemainingBytes("abc", 0)
        assertEquals(0, result5.size)
        assertEquals(3, actualSize5)
    }

    @Property
    fun jsonEncoding(
        @ForAll @From("jsonEncodingSpecs") specs: List<JsonEncodingSpec>
    ) {
        val encoder = Radix64JsonEncoder()
        for (spec in specs) {
            spec.encode(encoder)
        }
        val bytes = encoder.encode()
        val decoded = JsonEncodingSpec.decode(bytes)
        assertEquals(specs, decoded)
    }

    @Test
    fun jsonNumberZero() {
        val spec = JsonEncodingSpec.ofNumber(0.0)
        val encoder = Radix64JsonEncoder()
        spec.encode(encoder)
        val bytes = encoder.encode()
        val decoded = JsonEncodingSpec.decode(bytes)
        assertEquals(listOf(spec), decoded)
    }

    @Test
    fun jsonFullString() {
        val spec = JsonEncodingSpec.ofString("abc", 3)
        val encoder = Radix64JsonEncoder()
        spec.encode(encoder)
        val bytes = encoder.encode()
        val decoded = JsonEncodingSpec.decode(bytes)
        assertEquals(listOf(spec), decoded)
    }

    @Test
    fun jsonPartialString() {
        val spec = JsonEncodingSpec.ofString("abc", 2)
        val encoder = Radix64JsonEncoder()
        spec.encode(encoder)
        val bytes = encoder.encode()
        println(bytes.toList())
        val decoded = JsonEncodingSpec.decode(bytes)
        assertEquals(listOf(spec), decoded)
    }

    @Property
    fun jsonEncoderCloning(
        @ForAll s1: String,
        @ForAll s2: String
    ) {
        val encoder1 = Radix64JsonEncoder()
        encoder1.addString(s1, s1.length * 4)

        val bytes11 = encoder1.encode()

        val encoder2 = encoder1.clone()
        encoder2.addString(s2, s2.length * 4)

        val bytes2 = encoder2.encode()
        val bytes12 = encoder1.encode()

        val decoded1 = JsonEncodingSpec.decode(bytes11)
        val decoded2 = JsonEncodingSpec.decode(bytes2)

        assertEquals(bytes11.toList(), bytes12.toList())
        assertTrue(Arrays.compareUnsigned(bytes12, bytes2) < 0)

        assertEquals(1, decoded1.size)
        assertEquals(2, decoded2.size)

        assertEquals(s1, decoded1[0].stringValue)
        assertEquals(s1, decoded2[0].stringValue)
        assertEquals(s2, decoded2[1].stringValue)
    }
}