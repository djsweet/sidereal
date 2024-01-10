// SPDX-FileCopyrightText: 2023 Dani Sweet <thorium@djsweet.name>
//
// SPDX-License-Identifier: MIT

package name.djsweet.sidereal

import net.jqwik.api.ForAll
import net.jqwik.api.Property
import org.junit.jupiter.api.Assertions.*
import java.util.*

class BinaryHeapTest {
    @Property
    fun maintainsHeapInvariantPopOnly(
        @ForAll numbers: List<Long>
    ) {
        val heap = LongKeyedBinaryMinHeap<Long>()
        for (i in numbers.indices) {
            val num = numbers[i]
            heap.push(num to num)
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
        val heap = LongKeyedBinaryMinHeap<Long>()
        val currents = TreeMap<Long, Int>()
        for (i in 0 until numbers.size/2) {
            val num = numbers[i]
            heap.push(num to num)
            assertEquals(i + 1, heap.size)
            val priorCount = currents[num] ?: 0
            currents[num] = priorCount + 1
        }

        var endHeapSize = heap.size

        for (i in numbers.size/2 until numbers.size) {
            val newNumber = numbers[i]
            val prior = heap.popPush(newNumber to newNumber)
            if (prior != null) {
                val (priorHeapKey) = prior
                val minBeforePopPush = currents.firstEntry()
                assertNotNull(minBeforePopPush)
                assertEquals(minBeforePopPush!!.key, priorHeapKey)
                val priorCount = currents[priorHeapKey] ?: 0
                if (priorCount <= 1) {
                    currents.remove(priorHeapKey)
                } else {
                    currents[priorHeapKey] = priorCount - 1
                }
                endHeapSize--
            }

            val priorNew = currents[newNumber] ?: 0
            currents[newNumber] = priorNew + 1
            endHeapSize++
        }

        assertEquals(endHeapSize, heap.size)
    }
}