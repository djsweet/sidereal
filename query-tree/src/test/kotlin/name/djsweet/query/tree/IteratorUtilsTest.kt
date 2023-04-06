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
}