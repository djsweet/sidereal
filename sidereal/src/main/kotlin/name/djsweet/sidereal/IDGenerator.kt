// SPDX-FileCopyrightText: 2023 Dani Sweet <thorium@djsweet.name>
//
// SPDX-License-Identifier: MIT

package name.djsweet.sidereal

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