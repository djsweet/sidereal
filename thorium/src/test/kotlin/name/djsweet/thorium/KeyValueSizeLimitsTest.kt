package name.djsweet.thorium

import io.vertx.core.Vertx
import io.vertx.kotlin.coroutines.awaitResult
import kotlinx.coroutines.runBlocking
import name.djsweet.query.tree.QPTrie
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

// If either of these fail,
// 1. Run `gradle run --args="kvp-byte-budget" and take note of the reported byte budget, call the highest size
//    the non-crashing budget (NCB)
// 2. Uncomment the "println" below
// 3. Choose the higher of the two budgets as the original basis, call it the crashing budget (CB)
// 4. Update MAX_POSSIBLE_KEY_VALUE_SIZE_SAFETY_FACTOR so that it is proportionally scaled greater than
//    CB/NCB -- that is, crashing budget divided by non-crashing budget.
//
// As an example, if MAX_POSSIBLE_KEY_VALUE_SIZE_SAFETY_FACTOR=4, CB=30857, NCB=13620, then
// CB/NCB = 2.266, so we set MAX_POSSIBLE_KEY_VALUE_SIZE_SAFETY_FACTOR=ceil(2.266 * 4) = ceil(9.064) = 10.

// Note that we are testing keys that are 50% larger than the byte budget. We're encoding all data
// according to the Radix64 encoding, which at a minimum is 1/3 larger than the data it is encoding,
// plus some overhead for internal tagging. The extra 50% test here ensures that our encoding won't
// cause us to go over the budget.

class KeyValueSizeLimitsTest {
    private fun keyValueSizeLimitImpl(sizeLimit: Int) {
        var trie = QPTrie<Int>()
        for (size in 1..sizeLimit) {
            val lowNybble = ByteArray(size)
            lowNybble[size-1] = 1 // 0x01
            val highNybble = ByteArray(size)
            highNybble[size-1] = 16 // 0x10
            // Note that we have to use highNybble before lowNybble;
            // if we did this the other way around, we would skip the stack allocation
            // for modifying the lowNybble QPTrie node.
            trie = trie.put(highNybble, size * 2).put(lowNybble, size * 2 + 1)
            //println("ok for size=$size budget=$sizeLimit heap=${Runtime.getRuntime().maxMemory()}")
        }
        assertEquals((sizeLimit * 2).toLong(), trie.size)
    }

    @Test
    fun keyValueSizeLimitPreventsStackOverflow() {
        val sizeLimit = maxSafeKeyValueSizeSync()
        this.keyValueSizeLimitImpl(sizeLimit * 3 / 2)
    }

    @Test
    fun keyValueSizeLimitForVertxPreventsStackOverflow() {
        val vertx = Vertx.vertx()
        val sizeLimit = maxSafeKeyValueSizeSync(vertx)
        val self = this
        try {
            runBlocking {
                awaitResult { handler ->
                    vertx.executeBlocking { ->
                        self.keyValueSizeLimitImpl(sizeLimit * 3 / 2)
                    }.andThen(handler)
                }
            }
        } finally {
            vertx.close()
        }
    }
}