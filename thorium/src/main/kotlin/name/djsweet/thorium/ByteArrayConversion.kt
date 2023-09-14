package name.djsweet.thorium

import java.nio.charset.Charset

internal fun convertStringToByteArray(s: String): ByteArray {
    return s.encodeToByteArray()
}

internal fun convertByteArrayToString(ba: ByteArray): String {
    return ba.toString(Charset.forName("UTF-8"))
}

internal fun convertLongIntoGivenByteArray(l: Long, bs: ByteArray) {
    // See the Java Language Specification, section 5.1.3: Narrowing Primitive Conversion
    // https://docs.oracle.com/javase/specs/jls/se16/html/jls-5.html#jls-5.1.3
    // We don't need to perform a preliminary .and(0xff) because the behavior of
    // narrowing conversion is to discard all the bits except the lowest ones,
    // i.e. .and(0xff) without the actual work.
    bs[0] = l.shr(56).toByte()
    bs[1] = l.shr(48).toByte()
    bs[2] = l.shr(40).toByte()
    bs[3] = l.shr(32).toByte()
    bs[4] = l.shr(24).toByte()
    bs[5] = l.shr(16).toByte()
    bs[6] = l.shr(8).toByte()
    bs[7] = l.and(0xff).toByte()
}

internal fun convertLongToByteArray(l: Long): ByteArray {
    val result = ByteArray(8)
    convertLongIntoGivenByteArray(l, result)
    return result
}

internal fun convertByteArrayToLong(b: ByteArray): Long {
    // The .and(0xff) deals with promotion of negative bytes to long: all the upper bits will be 1 if we don't
    // explicitly reset them to 0, and this will interfere with the or operations later
    val byte0 = b[0].toLong().and(0xff).shl(56)
    val byte1 = b[1].toLong().and(0xff).shl(48)
    val byte2 = b[2].toLong().and(0xff).shl(40)
    val byte3 = b[3].toLong().and(0xff).shl(32)
    val byte4 = b[4].toLong().and(0xff).shl(24)
    val byte5 = b[5].toLong().and(0xff).shl(16)
    val byte6 = b[6].toLong().and(0xff).shl(8)
    val byte7 = b[7].toLong().and(0xff)
    val b01 = byte0.or(byte1)
    val b23 = byte2.or(byte3)
    val b45 = byte4.or(byte5)
    val b67 = byte6.or(byte7)
    val b0123 = b01.or(b23)
    val b4567 = b45.or(b67)
    return b0123.or(b4567)
}