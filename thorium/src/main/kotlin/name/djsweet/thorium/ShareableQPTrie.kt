package name.djsweet.thorium

import io.vertx.core.buffer.Buffer
import io.vertx.core.shareddata.ClusterSerializable
import io.vertx.core.shareddata.Shareable
import name.djsweet.query.tree.QPTrie

data class ShareableQPTrie(var trie: QPTrie<ByteArray>): Shareable, ClusterSerializable {
    constructor(): this(QPTrie())

    override fun writeToBuffer(buffer: Buffer?) {
        if (buffer == null) {
            return
        }
        buffer.appendLong(trie.size)
        trie.visitAscendingUnsafeSharedKey { (key, value) ->
            buffer.appendInt(key.size)
            buffer.appendBytes(key)
            buffer.appendInt(value.size)
            buffer.appendBytes(value)
        }
    }

    override fun readFromBuffer(pos: Int, buffer: Buffer?): Int {
        if (buffer == null) {
            return pos
        }
        val trieSize = buffer.getLong(pos)
        var readPos = pos + 8
        var newTrie = QPTrie<ByteArray>()
        for (i in 0 until trieSize) {
            val keySize = buffer.getInt(readPos)
            readPos += 4
            val key = buffer.getBytes(readPos, readPos + keySize)
            readPos += keySize
            val valueSize = buffer.getInt(readPos)
            readPos += 4
            val value = buffer.getBytes(readPos, readPos + valueSize)
            readPos += valueSize
            newTrie = newTrie.putUnsafeSharedKey(key, value)
        }
        this.trie = newTrie
        return readPos
    }
}