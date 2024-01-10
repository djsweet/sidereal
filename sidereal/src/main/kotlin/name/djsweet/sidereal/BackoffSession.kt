// SPDX-FileCopyrightText: 2023 Dani Sweet <thorium@djsweet.name>
//
// SPDX-License-Identifier: MIT

package name.djsweet.sidereal

// FIXME: Document the choice of exponential jitter and polynomial backoff

import java.util.concurrent.ThreadLocalRandom
import kotlin.math.ln
import kotlin.math.pow

internal const val minJitterProbability = 0.001 // This will result in -ln(1-0.001) ~= 0.001x
internal const val maxJitterProbability = 0.999 // This will result in -ln(1-0.999) ~= 6.9x

internal fun randomJitter(): Double {
    val p = ThreadLocalRandom.current().nextDouble(minJitterProbability, maxJitterProbability)
    return -ln(1 - p)
}

internal class BackoffSession(
    val baseSleepTimeMS: Double,
    val attemptExponent: Double = 2.0,
) {
    private var internalAttempt = 0
    val attempt: Int get() = this.internalAttempt

    fun sleepTimeMS(): Double {
        val currentAttempt = (++this.internalAttempt).toDouble()
        return randomJitter() * this.baseSleepTimeMS * currentAttempt.pow(this.attemptExponent)
    }
}