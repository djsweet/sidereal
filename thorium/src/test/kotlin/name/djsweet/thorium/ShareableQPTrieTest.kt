package name.djsweet.thorium

import io.vertx.core.buffer.Buffer
import name.djsweet.query.tree.QPTrie

import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import net.jqwik.api.*

class ShareableQPTrieTest {
    @Test
    fun nullBufferInReading() {
        val test = ShareableQPTrieOfByteArrays()
        val origTrie = test.trie
        val newPos = test.readFromBuffer(12, null)
        assertEquals(12, newPos)
        assertTrue(test.trie === origTrie)
    }

    @Test
    fun nullBufferInWriting() {
        val test = ShareableQPTrieOfByteArrays()
        val origTrie = test.trie
        test.writeToBuffer(null)
        assertTrue(test.trie === origTrie)
    }

    @Provide
    fun qpTrieOfByteArrays(): Arbitrary<QPTrie<ByteArray>> {
        val entries = Arbitraries.bytes().array(ByteArray::class.java).flatMap {
            key -> Arbitraries.bytes().array(ByteArray::class.java).map { value -> key to value }
        }.list()
        return entries.map { QPTrie(it) }
    }

    @Property
    fun shareableBufferReadingWritingByteArrays(
        @ForAll @From("qpTrieOfByteArrays") trie: QPTrie<ByteArray>
    ) {
        val buf = Buffer.buffer()
        val shareable1 = ShareableQPTrieOfByteArrays(trie)
        shareable1.writeToBuffer(buf)

        val shareable2 = ShareableQPTrieOfByteArrays()
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

    @Provide
    fun qpTrieOfByteArrayLists(): Arbitrary<QPTrie<List<ByteArray>>> {
        val entries = Arbitraries.bytes().array(ByteArray::class.java).flatMap { key ->
            Arbitraries.bytes().array(ByteArray::class.java).list().map { value -> key to value }
        }.list()
        return entries.map { QPTrie(it) }
    }

    @Property
    fun shareableBufferReadingWritingByteArrayLists(
        @ForAll @From("qpTrieOfByteArrayLists") trie: QPTrie<List<ByteArray>>
    ) {
        val buf = Buffer.buffer()
        val shareable1 = ShareableQPTrieOfByteArrayLists(trie)
        shareable1.writeToBuffer(buf)

        val shareable2 = ShareableQPTrieOfByteArrayLists()
        val next = shareable2.readFromBuffer(0, buf)
        assertEquals(buf.length(), next)

        val (newTrie) = shareable2
        assertEquals(trie.size, newTrie.size)

        newTrie.visitUnsafeSharedKey { (key, value) ->
            val valueFromOriginal = trie.get(key)
            assertNotNull(valueFromOriginal)
            assertEquals(value.size, valueFromOriginal!!.size)
            for (i in value.indices) {
                val entry = value[i]
                val entryFromOriginal = valueFromOriginal[i]
                assertEquals(entryFromOriginal.toList(), entry.toList())
            }
        }
    }
}