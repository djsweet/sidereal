package name.djsweet.thorium

import io.vertx.core.Vertx
import io.vertx.kotlin.coroutines.awaitResult
import kotlinx.coroutines.runBlocking

private const val MIN_POSSIBLE_KEY_VALUE_SIZE = 1024
private const val MAX_POSSIBLE_KEY_VALUE_SIZE = 65536
private const val MAX_POSSIBLE_KEY_VALUE_SIZE_SAFETY_FACTOR = 4
private const val MAX_POSSIBLE_KEY_VALUE_SIZE_WARMUP_ITERATIONS = 10
private const val MAX_POSSIBLE_KEY_VALUE_SIZE_ITERATIONS = 60

// We return `result` to ensure that the JVM doesn't attempt dead code elimination
private fun recurseUntilZero(cur: Int, result: Int): Int {
    // This is admittedly wasted work, but we want to fill the stack with variables to create a more
    // realistic testing environment.
    val curHighest4 = cur.and(0xff.shl(24)).toLong()
    val curHigher4 = cur.and(0xff.shl(16)).toLong()
    val curLower4 = cur.and(0xff.shl(8)).toLong()
    val curLowest4 = cur.and(0xff).toLong()
    val resultHighest4 = result.and(0xff.shl(24)).toLong()
    val resultHigher4 = result.and(0xff.shl(16)).toLong()
    val resultLower4 = result.and(0xff.shl(8)).toLong()
    val resultLowest4 = result.and(0xff).toLong()

    val curReconstituted = curHighest4.or(curHigher4).or(curLower4).or(curLowest4)
    val resultReconstituted = resultHighest4.or(resultHigher4).or(resultLower4).or(resultLowest4)
    return if (cur <= 0) {
        resultReconstituted.toInt()
    } else {
        recurseUntilZero(curReconstituted.toInt() - 1, result)
    }
}

private fun maxSafeKeyValueSizeSingleIteration(startingUpperBound: Int): Int {
    var maxBeforeCrash: Int
    var lowerBound = 0
    var upperBound = startingUpperBound
    try {
        maxBeforeCrash = recurseUntilZero(upperBound, upperBound)
        return maxBeforeCrash
    } catch (e: StackOverflowError) {
        maxBeforeCrash = 0
    }
    while (lowerBound <= upperBound) {
        val testEntry = (upperBound - lowerBound) / 2 + lowerBound
        try {
            maxBeforeCrash = recurseUntilZero(testEntry, testEntry)
            // We've already tried the upper bound, and we know that it crashed, so we won't try it again.
            // We'll deal with the entry we have as the last possible one instead.
            if (testEntry == lowerBound) {
                break
            } else {
                lowerBound = testEntry
            }
        } catch (e: StackOverflowError) {
            upperBound = testEntry
        }
    }
    return maxBeforeCrash
}

private suspend fun maxSafeKeyValueSizeWithIterationsAsync(vertx: Vertx, iterations: Int): Int {
    var maxKeySize = MAX_POSSIBLE_KEY_VALUE_SIZE * MAX_POSSIBLE_KEY_VALUE_SIZE_SAFETY_FACTOR
    for (i in 0 until iterations) {
        maxKeySize = awaitResult { handler ->
            vertx.executeBlocking {
                it.complete(maxKeySize.coerceAtMost(maxSafeKeyValueSizeSingleIteration(maxKeySize)))
            }.andThen(handler)
        }
    }
    return maxKeySize / MAX_POSSIBLE_KEY_VALUE_SIZE_SAFETY_FACTOR
}

private fun maxSafeKeyValueSizeWithIterations(iterations: Int): Int {
    var maxKeySize = MAX_POSSIBLE_KEY_VALUE_SIZE * MAX_POSSIBLE_KEY_VALUE_SIZE_SAFETY_FACTOR
    for (i in 0 until iterations) {
        maxKeySize = maxKeySize.coerceAtMost(maxSafeKeyValueSizeSingleIteration(maxKeySize))
    }
    return maxKeySize / MAX_POSSIBLE_KEY_VALUE_SIZE_SAFETY_FACTOR
}

private fun ensureMinPossibleKeyValueSize(keySize: Int) {
    if (keySize < MIN_POSSIBLE_KEY_VALUE_SIZE) {
        throw Error("Insufficient stack size for search operations: needed $MIN_POSSIBLE_KEY_VALUE_SIZE, got $keySize")
    }
}

internal fun maxSafeKeyValueSizeSync(vertx: Vertx): Int {
    // Boot up seems to impose a much lower stack size than we'll have during actual runtime.
    // We'll sort of work around that by performing some warm-up iterations.
    val maxSafeSizeWarmUp = runBlocking {
        maxSafeKeyValueSizeWithIterationsAsync(vertx, MAX_POSSIBLE_KEY_VALUE_SIZE_WARMUP_ITERATIONS)
    }
    ensureMinPossibleKeyValueSize(maxSafeSizeWarmUp)
    val maxSafeSizeInVertx = runBlocking {
        maxSafeKeyValueSizeWithIterationsAsync(vertx, MAX_POSSIBLE_KEY_VALUE_SIZE_ITERATIONS)
    }
    ensureMinPossibleKeyValueSize(maxSafeSizeInVertx)
    return maxSafeSizeInVertx
}

// This is used in benchmarking, which is why it can't be internal.
// There's also a nonzero chance of this having to run from within Vertx due to a bootup race condition.
fun maxSafeKeyValueSizeSync(): Int {
    val maxSafeKeyValueSize = maxSafeKeyValueSizeWithIterations(MAX_POSSIBLE_KEY_VALUE_SIZE_ITERATIONS)
    ensureMinPossibleKeyValueSize(maxSafeKeyValueSize)
    return maxSafeKeyValueSize
}