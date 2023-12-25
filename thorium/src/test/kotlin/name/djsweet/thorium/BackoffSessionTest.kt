// SPDX-FileCopyrightText: 2023 Dani Sweet <thorium@djsweet.name>
//
// SPDX-License-Identifier: MIT

package name.djsweet.thorium

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import kotlin.math.ln
import kotlin.math.pow

class BackoffSessionTest {
    private val minJitter = -ln(1 - minJitterProbability)
    private val maxJitter = -ln(1 - maxJitterProbability)

    @Test
    fun jitterWithinProbabilities() {
        for (i in 0 until 1000) {
            val roll = randomJitter()
            assertTrue(this.minJitter <= roll)
            assertTrue(roll <= this.maxJitter)
        }
    }

    @Test
    fun callsToSleepTimeMSIncreaseAttempt() {
        val session = BackoffSession(50.0)
        for (i in 0 until 128) {
            assertEquals(session.attempt, i)
            val sleepTime = session.sleepTimeMS()
            assertTrue(sleepTime >= 0.0)
        }
    }

    @Test
    fun sleepTimeMSWithinJitterRange() {
        val baseSleepTime = 25.0
        val session = BackoffSession(baseSleepTime)
        for (i in 0 until 128) {
            val withoutJitter = session.baseSleepTimeMS * (session.attempt + 1.0).pow(session.attemptExponent)
            val minValue = this.minJitter * withoutJitter
            val maxValue = this.maxJitter * withoutJitter
            val sleepTime = session.sleepTimeMS()
            assertTrue(minValue <= sleepTime)
            assertTrue(sleepTime <= maxValue)
        }
    }
}