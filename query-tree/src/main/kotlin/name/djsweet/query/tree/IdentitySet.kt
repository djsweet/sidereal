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

private fun<T> identitySetForIterable(elements: Iterable<T>): Pair<QPTrie<ListNode<T>>, Long> {
    var trie = QPTrie<ListNode<T>>()
    var size: Long = 0
    for (elem in elements) {
        val nextTrie = updateTrieForElem(trie, elem)
        if (nextTrie !== trie) {
            size += 1
        }
        trie = nextTrie
    }
    return Pair(trie, size)
}

/**
 * A persistent, immutable, unordered collection of items wherein each item is contained exactly once, and duplicate
 * items are not possible.
 *
 * Duplicate detection is based on two aspects:
 * 1. An item's [hashCode] is used to select an appropriate hash bucket
 * 2. Elements of the hash bucket are compared using object identity checks
 *
 * Note that [equals] is not used. Equivalent but non-identical items are treated as distinct in an IdentitySet.
 */
class IdentitySet<T> internal constructor(
    private val trie: QPTrie<ListNode<T>>,
    /**
     * Returns the number of items present in this IdentitySet.
     *
     * This is a Long instead of the standard Int to support more than ~2 billion members.
     */
    val size: Long
): Iterable<T> {
    constructor(): this(QPTrie(), 0)

    private constructor(trieSize: Pair<QPTrie<ListNode<T>>, Long>): this(trieSize.first, trieSize.second)
    constructor(elements: Iterable<T>): this(identitySetForIterable(elements))

    /**
     * Checks if the given [element] is contained in this IdentitySet.
     */
    fun contains(element: T): Boolean {
        val forHash = this.trie.get(byteArrayForInt(element.hashCode()))
        return listHasByIdentity(forHash, element)
    }

    /**
     * Returns a new IdentitySet containing the given [element].
     *
     * The returned IdentitySet will actually be the given IdentitySet if the given IdentitySet already contains
     * the given element. This is permissible because the IdentitySets would otherwise be functionally equivalent.
     */
    fun add(element: T): IdentitySet<T> {
        val newTrie = updateTrieForElem(this.trie, element)
        return if (newTrie === this.trie) {
            this
        } else {
            IdentitySet(newTrie, this.size + 1)
        }
    }

    /**
     * Returns a new IdentitySet that does not contain the given [element].
     *
     * The returned IdentitySet will actually be the given IdentitySet if the given IdentitySet does not contain
     * the given element. This is permissible because the IdentitySets would otherwise be functionally equivalent.
     */
    fun remove(element: T): IdentitySet<T> {
        val hashCode = byteArrayForInt(element.hashCode())
        val newTrie = this.trie.update(hashCode) {
            if (it == null) {
                null
            } else {
                listRemoveByIdentity(it, element)
            }
        }
        return if (newTrie === this.trie) {
            this
        } else {
            IdentitySet(newTrie, this.size - 1)
        }
    }

    override fun iterator(): Iterator<T> {
        return FlattenIterator(mapSequence(this.trie.iteratorUnsafeSharedKey()) {
            listIterator(it.value)
        })
    }

    /**
     * Calls [receiver] for every item contained in this IdentitySet.
     */
    fun visitAll(receiver: (value: T) -> Unit) {
        this.trie.visitUnsafeSharedKey {
            var node: ListNode<T>? = it.value
            while (node != null) {
                receiver(node.value)
                node = node.tail
            }
        }
    }
}