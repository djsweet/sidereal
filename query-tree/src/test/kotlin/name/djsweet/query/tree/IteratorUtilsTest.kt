// SPDX-FileCopyrightText: 2023 Dani Sweet <thorium@djsweet.name>
//
// SPDX-License-Identifier: MIT

package name.djsweet.query.tree

import org.junit.jupiter.api.Test

import org.junit.jupiter.api.Assertions.*
import net.jqwik.api.*

class IteratorUtilsTest {
    @Test fun emptyIterator() {
        val firstIt = EmptyIterator<String>()
        assertFalse(firstIt.hasNext())
        assertThrows(NoSuchElementException::class.java) { firstIt.next() }

        for (entry in EmptyIterator<String>()) {
            fail<String>("Should not have received $entry")
        }
    }

    @Test fun singleElementIterator() {
        val firstIt = SingleElementIterator("thing")
        assertTrue(firstIt.hasNext())
        val elem = firstIt.next()
        assertEquals("thing", elem)
        assertFalse(firstIt.hasNext())
        assertThrows(NoSuchElementException::class.java) { firstIt.next() }

        var seenEntries = 0
        for (innerElem in SingleElementIterator("stuff")) {
            assertEquals("stuff", innerElem)
            seenEntries += 1
        }
        assertEquals(1, seenEntries)
    }

    internal class ConcatenatedByteArrayIterator(private val items: Array<ByteArray>) : ConcatenatedIterator<Byte>() {
        override fun iteratorForOffset(offset: Int): Iterator<Byte>? {
            return if (offset >= this.items.size) {
                null
            } else {
                items[offset].iterator()
            }
        }
    }

    @Property
    fun concatenatedIterators(
        @ForAll arrays: List<ByteArray>
    ) {
        val expectedItems = arrays.flatMap { it.toList() }.toTypedArray()
        val seenItems = mutableListOf<Byte>()
        for (byte in ConcatenatedByteArrayIterator(arrays.toTypedArray())) {
            seenItems.add(byte)
        }
        assertArrayEquals(expectedItems, seenItems.toTypedArray())
    }

    @Property
    fun flattenedIterators(
        @ForAll numbers: List<List<Int>>
    ) {
        val fullList = ArrayList<Int>()
        for (sublist in numbers) {
            for (entry in sublist) {
                fullList.add(entry)
            }
        }

        val iteratorList = ArrayList<Iterator<Int>>()
        for (sublist in numbers) {
            iteratorList.add(sublist.iterator())
        }

        val flattened = FlattenIterator(iteratorList.iterator())
        val flattenedList = ArrayList<Int>()
        for (entry in flattened) {
            flattenedList.add(entry)
        }
        assertArrayEquals(fullList.toTypedArray(), flattenedList.toTypedArray())
        assertFalse(flattened.hasNext())
        assertThrows(NoSuchElementException::class.java) { flattened.next() }
    }

    private data class OffsetIterator(val baseOffset: Int, val size: Int): ConcatenatedIterator<Int>() {
        override fun iteratorForOffset(offset: Int): Iterator<Int>? {
            return if (offset >= this.size) {
                null
            } else {
                SingleElementIterator(this.baseOffset + offset)
            }
        }
    }

    private data class DirectFanoutIterator(
        val baseOffset: Int,
        val chunkSize: Int,
        val chunks: Int,
    ): ConcatenatedIterator<Int>() {
        override fun iteratorForOffset(offset: Int): Iterator<Int>? {
            return if (offset >= this.chunks) {
                 null
            } else {
                OffsetIterator(this.baseOffset + this.chunkSize * offset, this.chunkSize)
            }
        }
    }

    private data class IndirectFanoutIterator(
        val topChunks: Int,
        val middleChunks: Int,
        val bottomChunks: Int,
        val chunkSize: Int
    ): ConcatenatedIterator<Int>() {
        override fun iteratorForOffset(offset: Int): Iterator<Int>? {
            return if (offset >= this.topChunks) {
                null
            } else {
                val topOffset = offset * this.chunkSize * this.middleChunks * this.bottomChunks
                val lowestOffsets = (0 until this.middleChunks).asSequence().map { bottomOffset ->
                    val baseOffset = topOffset + this.chunkSize * bottomOffset
                    DirectFanoutIterator(baseOffset, this.chunkSize, this.bottomChunks)
                }.iterator()
                return FlattenIterator(lowestOffsets)
            }
        }
    }

    @Test
    fun concatenatedIteratorChildStackWithoutIntermediaries() {
        val noIntermediariesIt = DirectFanoutIterator(0,4, 4)
        for (i in 0 until 4) {
            var lastStack: ConcatenatedIterator<Int>? = null
            for (j in 0 until 4) {
                assertTrue(noIntermediariesIt.hasNext())
                assertEquals(i * 4 + j, noIntermediariesIt.next())
                val curStack = noIntermediariesIt.copyChildStack()
                // The ConcatenatedIterator root always puts `this` in its child stack, so that it can trivially
                // modify itself as if it's just another child on the stack. So, we expect to see a stack of
                //    DirectFanoutIterator -> OffsetIterator -> END
                // here.
                assertEquals(2, curStack.size)
                if (lastStack != null) {
                    assertTrue(curStack.last() === lastStack)
                } else {
                    lastStack = curStack.last()
                }
            }
        }
        assertFalse(noIntermediariesIt.hasNext())
        val finalStack = noIntermediariesIt.copyChildStack()
        assertEquals(0, finalStack.size)
    }

    @Test
    fun concatenatedIteratorChildStackWithIntermediaries() {
        val intermediariesIt = IndirectFanoutIterator(4, 4, 1, 4)
        for (i in 0 until 4) {
            for (j in 0 until 4) {
                var lastStack: ConcatenatedIterator<Int>? = null
                for (k in 0 until 4) {
                    assertTrue(intermediariesIt.hasNext())
                    val curStack = intermediariesIt.copyChildStack()
                    assertEquals(i * 16 + j * 4 + k, intermediariesIt.next())
                    // Because there's a FlattenIterator, which is not a ConcatenatedIterator between
                    // IndirectFanoutIterator and DirectFanoutIterator, DirectFanoutIterator will be its own "root".
                    // The only item present on the stack will be IndirectFanoutIterator.
                    assertEquals(1, curStack.size)
                    if (lastStack != null) {
                        assertTrue(curStack.last() === lastStack)
                    } else {
                        lastStack = curStack.last()
                    }
                }
            }
        }
        assertFalse((intermediariesIt.hasNext()))
        val finalStack = intermediariesIt.copyChildStack()
        assertEquals(0, finalStack.size)
    }

    @Property
    fun mapSequenceForIterable(
        @ForAll ints: List<Int>
    ) {
        val expectedInts = ArrayList<Int>()
        for (orig in ints) {
            expectedInts.add(orig * 2)
        }
        val givenIntsIterable = mapSequence(ints) { it * 2 }
        val givenInts = ArrayList<Int>()
        for (given in givenIntsIterable) {
            givenInts.add(given)
        }
        assertArrayEquals(expectedInts.toTypedArray(), givenInts.toTypedArray())
    }

    @Property
    fun mapSequenceForIterator(
        @ForAll ints: List<Int>
    ) {
        val expectedInts = ArrayList<Int>()
        for (orig in ints) {
            expectedInts.add(orig * 2)
        }
        val givenIntsIterable = mapSequence(ints.iterator()) { it * 2 }
        val givenInts = ArrayList<Int>()
        for (given in givenIntsIterable) {
            givenInts.add(given)
        }
        assertArrayEquals(expectedInts.toTypedArray(), givenInts.toTypedArray())
    }
}