package name.djsweet.thorium

import name.djsweet.query.tree.QPTrie
import net.jqwik.api.ForAll
import net.jqwik.api.Property
import org.junit.jupiter.api.Assertions.*

fun byteArrayConversionForSignedLongs(l: Long): ByteArray {
    return convertLongToByteArray(if (l < 0) { l.and(Long.MAX_VALUE) } else { l.or(Long.MIN_VALUE) })
}

class BinaryHeapTest {
    @Property
    fun maintainsHeapInvariantPopOnly(
        @ForAll numbers: List<Long>
    ) {
        val heap = ByteArrayKeyedBinaryMinHeap<Long>()
        for (i in numbers.indices) {
            val num = numbers[i]
            heap.push(byteArrayConversionForSignedLongs(num) to num)
            assertEquals(i + 1, heap.size)
        }

        val popped = mutableListOf<Long>()
        var lastPop = heap.pop()
        while (lastPop != null) {
            val (_, priorNum) = lastPop
            popped.add(priorNum)
            val peekResult = heap.peek()
            lastPop = heap.pop()
            assertEquals(peekResult, lastPop)
        }
        assertEquals(0, heap.size)

        val sortedNumbers = numbers.sorted()
        assertEquals(sortedNumbers, popped)
    }

    @Property
    fun maintainsHeapInvariant(
        @ForAll numbers: List<Long>
    ) {
        val heap = ByteArrayKeyedBinaryMinHeap<Long>()
        var currentsTrie = QPTrie<Int>()
        for (i in 0 until numbers.size/2) {
            val num = numbers[i]
            val heapKey = byteArrayConversionForSignedLongs(num)
            heap.push(byteArrayConversionForSignedLongs(num) to num)
            assertEquals(i + 1, heap.size)
            currentsTrie = currentsTrie.update(heapKey) { (it ?: 0) + 1 }
        }

        var endHeapSize = heap.size

        for (i in numbers.size/2 until numbers.size) {
            val newNumber = numbers[i]
            val heapKey = byteArrayConversionForSignedLongs(newNumber)
            val prior = heap.popPush(byteArrayConversionForSignedLongs(newNumber) to newNumber)
            if (prior != null) {
                val (priorHeapKey) = prior
                val minBeforePopPush = currentsTrie.minKeyValueUnsafeSharedKey()
                assertNotNull(minBeforePopPush)
                assertArrayEquals(minBeforePopPush!!.key, priorHeapKey)
                currentsTrie = currentsTrie.update(priorHeapKey) {
                    if (it == null) { null } else if (it == 1) { null } else { it - 1 }
                }
                endHeapSize--
            }
            currentsTrie = currentsTrie.update(heapKey) { (it ?: 0) + 1 }
            endHeapSize++
        }

        assertEquals(endHeapSize, heap.size)
    }
}