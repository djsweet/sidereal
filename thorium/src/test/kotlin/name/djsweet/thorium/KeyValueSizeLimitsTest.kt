// SPDX-FileCopyrightText: 2023 Dani Sweet <thorium@djsweet.name>
//
// SPDX-License-Identifier: MIT

package name.djsweet.thorium

import io.vertx.core.Vertx
import io.vertx.kotlin.coroutines.awaitResult
import kotlinx.coroutines.runBlocking
import name.djsweet.query.tree.QPTrie
import name.djsweet.query.tree.QPTrieKeyValue
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

// If either of these fail,
// 1. Run `gradle run --args="kvp-byte-budget" and take note of the reported byte budget, call the highest size
//    the non-crashing budget (NCB)
// 2. If the crash occurs in trie.put,
//    1. Uncomment the "println" below
//    2. Choose the higher of the two budgets as the original basis, call it the crashing budget (CB)
//    3. Update MAX_POSSIBLE_KEY_VALUE_SIZE_SAFETY_FACTOR so that it is proportionally scaled greater than
//       CB/NCB -- that is, crashing budget divided by non-crashing budget.
// 3. If the crash occurs in validateIterator, adjust MAX_POSSIBLE_KEY_VALUE_SIZE_SAFETY_FACTOR up by 1.
//    These crashes indicate that the safe margin was only barely sufficient for simple updates, and not
//    sufficient for more complex updates.
//
// As an example, if MAX_POSSIBLE_KEY_VALUE_SIZE_SAFETY_FACTOR=1.25, CB=9137, NCB=3619, then
// CB/NCB = 2.525, so we set MAX_POSSIBLE_KEY_VALUE_SIZE_SAFETY_FACTOR=ceil(2.525 * 1.25) = ceil(3.156) = 4.0.

// Note that we are testing keys that are 50% larger than the byte budget. We're encoding all data
// according to the Radix64 encoding, which at a minimum is 1/3 larger than the data it is encoding,
// plus some overhead for internal tagging. The extra 50% test here ensures that our encoding won't
// cause us to go over the budget.

class KeyValueSizeLimitsTest {
    companion object {
        private fun <T> validateIterator(
            iterator: Iterator<QPTrieKeyValue<T>>,
            visitor: (handleEntry: (QPTrieKeyValue<T>) -> Unit) -> Unit
        ) {
            visitor {
                assertTrue(iterator.hasNext())
                assertEquals(iterator.next(), it)
            }
            assertFalse(iterator.hasNext())
        }
    }

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
        }
        assertEquals((sizeLimit * 2).toLong(), trie.size)

        // The visitation methods should similarly not trigger a StackOverflowError when run with the size limit.
        // The functionality below is already semantically tested under the query.tree package, but we're running it
        // again here for the side effect of stack consumption.
        val minKvp = trie.minKeyValueUnsafeSharedKey()
        assertNotNull(minKvp)
        assertEquals(sizeLimit * 2 + 1, minKvp!!.value)
        assertEquals(sizeLimit - 1, minKvp.key.indexOfFirst { it != 0.toByte() })

        val indexMidpoint = sizeLimit / 2
        var kvpAtIndexMidpoint: QPTrieKeyValue<Int>? = null
        var kvpAtLastIndex: QPTrieKeyValue<Int>? = null

        // This is a special case because of how we're complecting the midpoint and last index saves,
        // so it can't use validateIterator directly.
        val fullIteratorUnspecified = trie.iteratorUnsafeSharedKey()
        trie.visitUnsafeSharedKey {
            assertTrue(fullIteratorUnspecified.hasNext())
            assertEquals(fullIteratorUnspecified.next(), it)

            // This is a bit of an overload: we want to perform inequality-based iteration in later tests,
            // so we pick the value at the middle of the input set, and the value at the end of the input set, to use as
            // the basis for these tests.
            if (it.value == indexMidpoint) {
                kvpAtIndexMidpoint = it
            }
            kvpAtLastIndex = it
        }
        assertFalse(fullIteratorUnspecified.hasNext())

        validateIterator(trie.iteratorAscendingUnsafeSharedKey()) {
            trie.visitAscendingUnsafeSharedKey(it)
        }

        validateIterator(trie.iteratorDescendingUnsafeSharedKey()) {
            trie.visitDescendingUnsafeSharedKey(it)
        }

        assertNotNull(kvpAtIndexMidpoint)
        assertNotNull(kvpAtLastIndex)

        validateIterator(trie.iteratorLessThanOrEqualUnsafeSharedKey(kvpAtIndexMidpoint!!.key)) {
            trie.visitLessThanOrEqualUnsafeSharedKey(kvpAtIndexMidpoint!!.key, it)
        }

        validateIterator(trie.iteratorLessThanOrEqualUnsafeSharedKey(kvpAtLastIndex!!.key)) {
            trie.visitLessThanOrEqualUnsafeSharedKey(kvpAtLastIndex!!.key, it)
        }

        validateIterator(trie.iteratorStartsWithUnsafeSharedKey(kvpAtIndexMidpoint!!.key)) {
            trie.visitStartsWithUnsafeSharedKey(kvpAtIndexMidpoint!!.key, it)
        }

        validateIterator(trie.iteratorGreaterThanOrEqualUnsafeSharedKey(kvpAtIndexMidpoint!!.key)) {
            trie.visitGreaterThanOrEqualUnsafeSharedKey(kvpAtIndexMidpoint!!.key, it)
        }

        val emptyByteArray = byteArrayOf()
        validateIterator(trie.iteratorGreaterThanOrEqualUnsafeSharedKey(emptyByteArray)) {
            trie.visitGreaterThanOrEqualUnsafeSharedKey(emptyByteArray, it)
        }
    }

    @Test
    fun keyValueSizeLimitPreventsStackOverflow() {
        val sizeLimit = maxSafeKeyValueSizeSync()
        this.keyValueSizeLimitImpl(sizeLimit * 3 / 2)
    }

    @Test
    fun keyValueSizeLimitForVertxPreventsStackOverflow() {
        val self = this
        withVertxAsync { vertx ->
            // This doesn't need to be sync, but running this here _does_ give us code coverage.
            val sizeLimit = maxSafeKeyValueSizeSync(vertx)
            awaitResult { handler ->
                vertx.executeBlocking { ->
                    self.keyValueSizeLimitImpl(sizeLimit * 3 / 2)
                }.andThen(handler)
            }
        }
    }
}