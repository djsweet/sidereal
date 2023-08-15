package name.djsweet.thorium

import io.vertx.core.buffer.Buffer
import name.djsweet.query.tree.QPTrie

import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import net.jqwik.api.*

class ShareableQPTrieTest {
    @Test
    fun nullBufferInReading() {
        val test = ShareableQPTrie()
        val origTrie = test.trie
        val newPos = test.readFromBuffer(12, null)
        assertEquals(12, newPos)
        assertTrue(test.trie === origTrie)
    }

    @Test
    fun nullBufferInWriting() {
        val test = ShareableQPTrie()
        val origTrie = test.trie
        test.writeToBuffer(null)
        assertTrue(test.trie === origTrie)
    }

    @Provide
    fun qpTrie(): Arbitrary<QPTrie<ByteArray>> {
        val entries = Arbitraries.bytes().array(ByteArray::class.java).flatMap {
            key -> Arbitraries.bytes().array(ByteArray::class.java).map { value -> key to value }
        }.list()
        return entries.map { QPTrie(it) }
    }

    @Property
    fun shareableBufferReadingWriting(
        @ForAll @From("qpTrie") trie: QPTrie<ByteArray>
    ) {
        val buf = Buffer.buffer()
        val shareable1 = ShareableQPTrie(trie)
        shareable1.writeToBuffer(buf)

        val shareable2 = ShareableQPTrie()
        val next = shareable2.readFromBuffer(0, buf)
        assertEquals(buf.length(), next)

        val (newTrie) = shareable2
        assertEquals(trie.size, newTrie.size)
        newTrie.visitUnsafeSharedKey { (key, value) ->
            val valueFromOriginal = trie.get(key)
            assertNotNull(valueFromOriginal)
            assertEquals(value.toList(), valueFromOriginal!!.toList())
        }
    }
}