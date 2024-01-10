// SPDX-FileCopyrightText: 2023 Dani Sweet <thorium@djsweet.name>
//
// SPDX-License-Identifier: MIT

package name.djsweet.sidereal

import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

fun monotonicNowMS(): Long {
    return System.nanoTime() / 1_000_000
}

fun wallNow(): OffsetDateTime {
    return LocalDateTime.now().atOffset(ZoneOffset.UTC)
}

fun wallNowAsString(): String {
    return wallNow().format(DateTimeFormatter.ISO_ZONED_DATE_TIME)
}
