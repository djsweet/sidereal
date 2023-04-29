package name.djsweet.query.tree

internal fun byteArrayForInt(input: Int): ByteArray {
    val byte0 = (input shr 24 and 0xff).toByte()
    val byte1 = (input shr 16 and 0xff).toByte()
    val byte2 = (input shr 8 and 0xff).toByte()
    val byte3 = (input and 0xff).toByte()
    return byteArrayOf(byte0, byte1, byte2, byte3)
}

private fun<T> updateTrieForElem(trie: QPTrie<ListNode<T>>, elem: T): QPTrie<ListNode<T>> {
    return trie.update(byteArrayForInt(elem.hashCode())) {
        if (it == null || !listHasByIdentity(it, elem)) {
            listPrepend(elem, it)
        } else {
            it
        }
    }
}

class IdentitySet<T> private constructor(private val trie: QPTrie<ListNode<T>>): Iterable<T> {
    constructor(): this(QPTrie())
    constructor(elems: Iterable<T>): this(elems.fold(QPTrie()) { trie, elem -> updateTrieForElem(trie, elem) })

    val size: Long get() = this.trie.size

    fun contains(elem: T): Boolean {
        val forHash = this.trie.get(byteArrayForInt(elem.hashCode()))
        return listHasByIdentity(forHash, elem)
    }

    fun add(elem: T): IdentitySet<T> {
        val newTrie = updateTrieForElem(this.trie, elem)
        return if (newTrie === this.trie) {
            this
        } else {
            IdentitySet(newTrie)
        }
    }

    fun remove(elem: T): IdentitySet<T> {
        val hashCode = byteArrayForInt(elem.hashCode())
        val newTrie = this.trie.update(hashCode) {
            if (it == null) {
                null
            } else {
                listRemoveByIdentity(it, elem)
            }
        }
        return if (newTrie === this.trie) {
            this
        } else {
            IdentitySet(newTrie)
        }
    }

    override fun iterator(): Iterator<T> {
        return FlattenIterator(mapSequence(this.trie) {
            listIterator(it.value)
        })
    }
}