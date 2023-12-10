package name.djsweet.thorium

import io.vertx.core.Vertx
import io.vertx.kotlin.coroutines.awaitResult
import kotlinx.coroutines.runBlocking
import name.djsweet.query.tree.QPTrie
import kotlin.math.sqrt

// We are unfortunately sometimes extremely stack constrained, especially when running
// tests with code coverage, so we have to allow for lower key/value sizes than 1KB.
private const val MIN_POSSIBLE_KEY_VALUE_SIZE = 256
private const val MAX_POSSIBLE_KEY_VALUE_SIZE = 65536

// This used to be used with a non-allocating recurseUntilZero function, which was a poor simulation of the internals
// of QPTrie, the implementation that required these limits in the first place. We've since switched to a
// StackAllocationError test implemented inside QPTrie itself, triggering the exact code that will have issues
// with stack overflows later. As a result, the safety factor could be reduced to a much smaller float. 1.5x
// fits with the 33% + overhead increase of Radix64Encoding, and seems to be the least common factor that allows
// tests to pass across OpenJDK 17, OpenJDK 18, and GraalVM 21. (In the interest of full disclosure: the tests also
// use an expansion factor of 1.5x for the exact same reason -- this is probably the biggest contributor.)
private const val MAX_POSSIBLE_KEY_VALUE_SIZE_SAFETY_FACTOR = 1.5
private const val MAX_POSSIBLE_KEY_VALUE_SIZE_WARMUP_ITERATIONS = 10
private const val MAX_POSSIBLE_KEY_VALUE_SIZE_ITERATIONS = 60

private fun maxSafeKeyValueSizeSingleIteration(startingUpperBound: Int): Int {
    var maxBeforeCrash = 0
    var lowerBound = 0
    var upperBound = startingUpperBound
    if (QPTrie.keySizeIsSafeFromOverflow(upperBound)) {
        return upperBound
    }
    while (lowerBound < upperBound) {
        val testEntry = (upperBound - lowerBound) / 2 + lowerBound
        if (QPTrie.keySizeIsSafeFromOverflow(testEntry)) {
            maxBeforeCrash = testEntry
            if (testEntry == lowerBound) {
                break
            } else {
                lowerBound = testEntry
            }
        } else {
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
    var maxKeySize = (maxKeyValueSizeFromHeapUsage() * MAX_POSSIBLE_KEY_VALUE_SIZE_SAFETY_FACTOR).toInt()
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
    return (maxKeySize / MAX_POSSIBLE_KEY_VALUE_SIZE_SAFETY_FACTOR).toInt()
}

private fun maxSafeKeyValueSizeWithMaxIterations(): Int {
    var maxKeySize = (maxKeyValueSizeFromHeapUsage() * MAX_POSSIBLE_KEY_VALUE_SIZE_SAFETY_FACTOR).toInt()
    for (i in 0 until MAX_POSSIBLE_KEY_VALUE_SIZE_ITERATIONS) {
        maxKeySize = maxKeySize.coerceAtMost(maxSafeKeyValueSizeSingleIteration(maxKeySize))
    }
    return (maxKeySize / MAX_POSSIBLE_KEY_VALUE_SIZE_SAFETY_FACTOR).toInt()
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