package name.djsweet.thorium

import java.security.SecureRandom
import java.util.*

fun generateOutboundEventID(random: Random): String {
    val encodeBytes = ByteArray(15)
    random.nextBytes(encodeBytes)
    return Base64.getEncoder().encodeToString(encodeBytes)
}

fun generateInstanceID(): String {
    return generateOutboundEventID(SecureRandom.getInstanceStrong())
}