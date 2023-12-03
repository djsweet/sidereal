package name.djsweet.thorium

import io.vertx.core.Vertx
import io.vertx.kotlin.coroutines.awaitResult
import kotlinx.coroutines.runBlocking
import kotlin.math.sqrt

// We are unfortunately sometimes extremely stack constrained, especially when running
// tests with code coverage, so we have to allow for lower key/value sizes than 1KB.
private const val MIN_POSSIBLE_KEY_VALUE_SIZE = 256
private const val MAX_POSSIBLE_KEY_VALUE_SIZE = 65536
// See KeyValueSizeLimitsTest.ts for the mechanism by which we derived this.
// We previously did not pass in a callback to recurseUntilZero, and the values before
// that needed to be tuned to prevent test crashes. We started with
// MAX_POSSIBLE_KEY_VALUE_SIZE_SAFETY_FACTOR=4, an expected safe size of 30857, and an actual
// safe size when running without coverage of 13620, so we were off by 127%. That would have put
// us at 10, and when considering the overhead of Radix64Encoding, we'd add another 50% to bring
// us up to 15. However, this would still crash when trying to run tests with code coverage, so
// to give us a more realistic expectation of how many stack frames we have, we started passing
// a callback into recurseUntilZero. This decreased the base number of recursive stack calls,
// as one would expect, so the safety factor was reduced until we started getting StackOverflowError
// again.
//
// GraalVM native builds add another wrinkle -- these builds require an even greater safety factor
// than OpenJDK running a JIT JVM. So, here, we're pulling a build-time trick of static initialization
// combined with an explicit system property to configure this value separately.
private val safetyFactorPropertyString = System.getProperty(buildOnlyKvpSafetyFactorPropertyName)
private val MAX_POSSIBLE_KEY_VALUE_SIZE_SAFETY_FACTOR = when (safetyFactorPropertyString) {
    null -> 13
    else -> Integer.parseInt(safetyFactorPropertyString)
}
private const val MAX_POSSIBLE_KEY_VALUE_SIZE_WARMUP_ITERATIONS = 10
private const val MAX_POSSIBLE_KEY_VALUE_SIZE_ITERATIONS = 60

// We return `result` to ensure that the JVM doesn't attempt dead code elimination.
// We also pass through an "on max value" handler function, to better simulate the
// normal use-case of passing through handler functions in QPTrie and QueryTree.
private fun recurseUntilZero(cur: Int, result: Int, onResult: (result: Int) -> Int): Int {
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
        onResult(resultReconstituted.toInt())
    } else {
        // This elides any potential tail call elimination, which isn't supported by the JVM, but may be supported
        // by GraalVM in the future.
        1 + recurseUntilZero(curReconstituted.toInt() - 1, result, onResult)
    }
}

private fun maxSafeKeyValueSizeSingleIteration(startingUpperBound: Int): Int {
    var maxBeforeCrash: Int
    var lowerBound = 0
    var upperBound = startingUpperBound
    try {
        var maxAsReportedByHandler = 0
        maxBeforeCrash = recurseUntilZero(upperBound, upperBound) {
            maxAsReportedByHandler = it
            it
        }
        // This may seem redundant, but
        // 1. we needed to ensure that the handler actually gets used, and can't be optimized out.
        // 2. recurseUntilZero actually returns 2x the number of recursive calls, because every call adds 1 to
        //    the prior result, which starts with the number of calls to make in the first place.
        maxBeforeCrash = maxBeforeCrash.coerceAtMost(maxAsReportedByHandler)
        return maxBeforeCrash
    } catch (e: StackOverflowError) {
        maxBeforeCrash = 0
    }
    while (lowerBound < upperBound) {
        val testEntry = (upperBound - lowerBound) / 2 + lowerBound
        try {
            var maxAsReportedByHandler = 0
            maxBeforeCrash = recurseUntilZero(testEntry, testEntry) {
                maxAsReportedByHandler = it
                it
            }
            // This may seem redundant, but
            // 1. we needed to ensure that the handler actually gets used, and can't be optimized out.
            // 2. recurseUntilZero actually returns 2x the number of recursive calls, because every call adds 1 to
            //    the prior result, which starts with the number of calls to make in the first place.
            maxBeforeCrash = maxBeforeCrash.coerceAtMost(maxAsReportedByHandler)
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

fun maxKeyValueSizeFromHeapUsage(): Int {
    val availableMemory = Runtime.getRuntime().maxMemory()
    // In order to trigger the worst-case key overflow, assuming that the first unsafe size for key/value byte arrays
    // is N, you would need 2*N arrays stored. The simplest possible case for triggering this is assuming a byte array
    // of all 0, except for one nybble that is non-zero. Storing all such byte arrays in a QPTrie results in the
    // degenerate behavior of every node _not_ having a prefix, and ultimately being 2*N nodes deep, one node for each
    // differing nybble.
    //
    // In order to pull this off, you'd need 2*N^2 bytes allocated, as in (2*N) arrays * N bytes per array. But there's
    // going to be an upper limit of getRuntime().maxMemory() available. And we're also going to want more memory than
    // just for QPTrie nodes, so let's say we want 4*N^2 bytes allocated, or twice as much as we'd need if we were only
    // storing QPTrie nodes. To solve for N, we have
    //
    // 4*N^2 = maxMemory
    // N^2 = maxMemory/4
    // N = sqrt(maxMemory/4)
    //
    // We could also say N = sqrt(maxMemory)/2 but there's a benefit to numerical stability if we divide 4 before
    // performing the sqrt.
    return MAX_POSSIBLE_KEY_VALUE_SIZE.coerceAtMost(sqrt((availableMemory / 4).toDouble()).toInt())
}

private suspend fun maxSafeKeyValueSizeWithIterationsAsync(vertx: Vertx, iterations: Int): Int {
    var maxKeySize = maxKeyValueSizeFromHeapUsage() * MAX_POSSIBLE_KEY_VALUE_SIZE_SAFETY_FACTOR
    for (i in 0 until iterations) {
        // We can't just .await() under runBlocking, because we've blocked the whole main on that .await(), and
        // that blocked main thread is where the Vertx run loop was going to run.
        maxKeySize = awaitResult { handler ->
            val executionTarget = {
                maxKeySize.coerceAtMost(maxSafeKeyValueSizeSingleIteration(maxKeySize))
            }
            vertx.executeBlocking(executionTarget).andThen(handler)
        }
    }
    return maxKeySize / MAX_POSSIBLE_KEY_VALUE_SIZE_SAFETY_FACTOR
}

private fun maxSafeKeyValueSizeWithMaxIterations(): Int {
    var maxKeySize = maxKeyValueSizeFromHeapUsage() * MAX_POSSIBLE_KEY_VALUE_SIZE_SAFETY_FACTOR
    for (i in 0 until MAX_POSSIBLE_KEY_VALUE_SIZE_ITERATIONS) {
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
// There's also a nonzero chance of this having to run from within Vertx due to a boot-up race condition.
fun maxSafeKeyValueSizeSync(): Int {
    val maxSafeKeyValueSize = maxSafeKeyValueSizeWithMaxIterations()
    ensureMinPossibleKeyValueSize(maxSafeKeyValueSize)
    return maxSafeKeyValueSize
}