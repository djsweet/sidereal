package name.djsweet.query.tree

import java.util.Arrays

/*
 * This is an immutable, persistent implementation of a QPTrie, as originally described in
 * https://dotat.at/prog/qp/blog-2015-10-04.html. In addition to being persistent, and therefore immutable,
 * this implementation also offers the following additional features:
 *
 * - Full iteration is supported, both with the java.lang.Iterable interface, and with a recursive "visit"
 *   callback interface that is at least 8x faster in microbenchmarks. This iteration is possible both
 *   in ascending and descending key order.
 * - Inequality lookups in the form of "less than or equal" and "greater than or equal" are supported, both with
 *   java.lang.Iterable interfaces and a recursive "visit" interface that performs similarly well to the full
 *   iteration "visit" interface.
 * - "starts-with" prefix matching lookups are supported, again both with java.lang.Iterable interfaces and
 *   with a "visit" interface.
 * - "prefix-of", i.e. "every entry with keys the given lookup starts with", lookups are supported, again both
 *   with java.lang.Iterable interfaces and with a "visit" interface.
 *
 * In order to support inequality lookups, we additionally store a pointer to a full key buffer in addition to
 * lookup offsets and run lengths inside non-leaf nodes. This allows us to sort according to the prefix
 * before the dispatching byte.
 *
 * This implementation does not use `popcnt` instructions for its sparse arrays, but instead stores nybble values
 * in a separate array of an equal length. The mechanism by which a nybble value is converted into an array offset
 * is implemented in `QPTrieUtils.offsetForNybble`. On an Apple M1 Max, `popcnt` methods were slightly slower for
 * lookups, and significantly slower when dealing with key removals, than the optimized "test every possible value"
 * implementation used here. The worst-case RAM overhead is 20 bytes more per node than if an Int were used. (Up to 16
 * bytes for the byte array, 8 bytes for the pointer on a 64-bit machine, vs 4 bytes.)
 */

/*
 * A QPTrie operates over nybbles of a ByteArray. A nybble is half a byte, which means that for a given array of bytes
 *   0x01 0x23 0x45 0x67 ...
 *
 * we have a node entry for each of 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07 ...
 *
 * If we suppose that we logically have an array of nybbles, and the array is 0 indexed, we'll have
 * a logical array of nybbles
 *   [0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, ...]
 *
 * The QPTrie is always rooted at an "empty" nybble, without an entry in the 0-indexed array. This root
 * nybble would logically have an array offset of -1.
 *
 * As a result of this, a QPTrie has a lookup path that can be broken up into odd nybbles and even nybbles,
 * like so:
 *   -1,   0,    1,    2,    3,    4,    5,    6,    7, ...
 *     [0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, ...]
 *
 * Every nybble at an odd index (including the empty root nybble) is represented by an OddNybble node,
 * and every nybble at an even index is represented by an EvenNybble node. This simplifies several aspects
 * of the QPTrie implementation:
 *
 * - Every invocation of `dispatchByte` is implicitly aware of which nybble in the byte it needs to address
 * - Values are stored at integral byte boundaries, specifically in OddNybble nodes, so EvenNybble does not
 *   even need to consider storing values.
 * - Prefixes are also stored at integral byte boundaries, again specifically in OddNybble nodes, so EvenNybble
 *   does not need to consider prefixes.
 */

// Here's hoping either the Kotlin compiler or the JVM running this
// optimizes out these casts...
internal fun oddNybbleFromByte(b: Byte): Byte {
    return b.toInt().and(0xf).toByte()
}

/*
 * A funny discontinuity exists in the transformed result of
 *  evenNybbleFromByte if the highest bit is set, i.e. the byte
 * is actually negative. The following argument assumes that the
 * hardware endianness is Big Endian, but in practice, Little Endian
 * architectures act as if they were Big Endian when performing
 * bit-shift operations.
 *
 * In this case, we transform
 *   1xxxyyyy -> 1111111111111111111111111xxxyyyy
 *
 * So the right shift becomes
 *   00001111111111111111111111111xxx
 *
 * And when we convert that back into a byte we get
 *   11111xxx
 *
 * Even though we _would_ have expected
 *   00001xxx
 *
 * if this were an actual unsigned byte.
 *
 * However, this does not harm the correctness of nybblesToBytes, because
 * the reverse-transform, assuming the odd nybble is 0000yyyy, is
 *   11111xxx -> 11111111111111111111111111111xxx // Promotion of Byte to Int
 *   11111111111111111111111111111xxx -> 1111111111111111111111111xxx0000 // Shift left
 *   1111111111111111111111111xxx0000 -> 1111111111111111111111111xxxyyyy // Or with odd nybble
 *   1111111111111111111111111xxxyyyy -> 1xxxyyyy // Conversion back to byte
 *
 * So we end up with 1xxxyyyy again, regardless of the discontinuity.
 */
internal fun evenNybbleFromByte(b: Byte): Byte {
    return b.toInt().shr(4).toByte()
}

// We end up working with empty byte arrays often enough that it makes sense to keep a single
// instance around for this purpose. It's only ever used in immutable contexts, particularly
// as a potential prefix array in OddNybble, so it's perfectly safe to use in multiple places.
private val emptyByteArray = byteArrayOf()

// Same as above, with one caveat: we'll need to cast this to an Array<EvenNybble<V>> wherever we use it.
// We used to avoid the cast by passing around a properly typed empty Array<EmptyNybble<V>>, but not only did this
// incur extra unnecessary heap allocation, it also incurred unnecessary stack allocation, too. This reduced the maximum
// key size enough that we had to pull the array out as a global.
//
// The cast to Array<EvenNybble<V>> would be unsafe, except for how this is an empty java.lang.Array. A java.lang.Array
// has a static size, that cannot be changed after instantiation. This means that an empty java.lang.Array will always
// be empty. All methods that try to mutate elements inside the array will necessarily throw. Additionally, because the
// java.lang.Array is empty, all methods attempting to access elements inside the array will throw. The type parameter
// of a java.lang.Array only comes into play when dealing with any elements inside the array, which,
// in this case, is impossible, because the array is empty. Consequently, the type parameter of
// an empty array simply does not matter.
private val emptyEvenNybbleArray = arrayOf<EvenNybble<*>>()

/**
 * Represents a key/value pair held by a [QPTrie].
 *
 * The returned `key` property may be shared with the supplying QPTrie if
 * it was obtained from a method ending in `UnsafeSharedKey`.
 */
data class QPTrieKeyValue<V> internal constructor(
    val key: ByteArray,
    val value: V
) {
    internal fun withKeyCopy(): QPTrieKeyValue<V> {
        return this.copy(key=this.key.copyOf())
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is QPTrieKeyValue<*>) return false
        if (!this.key.contentEquals(other.key)) return false
        return this.value == other.value
    }

    override fun hashCode(): Int {
        var result = this.key.contentHashCode()
        result = 31 * result + this.value.hashCode()
        return result
    }
}

/**
 * Signature for any callback that can receive a [QPTrieKeyValue] as its sole argument.
 */
typealias VisitReceiver<V> = (value: QPTrieKeyValue<V>) -> Unit

private fun<V> byteSliceBackingForOddNybble(
    valuePair: QPTrieKeyValue<V>?,
    evenNybbles: Array<EvenNybble<V>>
): ByteArray {
    return if (valuePair != null) {
        valuePair.key
    } else {
        val oddNybbles = evenNybbles[0].nybbleDispatch
        oddNybbles[0].prefixBacking
    }
}

private class OddNybble<V>(
    val prefixBacking: ByteArray,
    val prefixOffset: Int,
    val prefixSize: Int,
    val valuePair: QPTrieKeyValue<V>?,
    val size: Long,
    val nybbleValues: ByteArray,
    val nybbleDispatch: Array<EvenNybble<V>>,
) {
    fun dispatchByte(target: Byte): EvenNybble<V>? {
        val offset = QPTrieUtils.offsetForNybble(this.nybbleValues, evenNybbleFromByte(target))
        return if (offset < 0) { null } else { this.nybbleDispatch[offset] }
    }

    fun compareLookupSliceToCurrentPrefix(compareTo: ByteArray, compareOffset: Int): Int {
        val size = this.prefixSize
        val offset = this.prefixOffset
        val byteArrayEnd = (compareOffset + size).coerceAtMost(compareTo.size)
        return Arrays.compareUnsigned(this.prefixBacking, offset, offset + size, compareTo, compareOffset, byteArrayEnd)
    }

    fun testEqualLookupSliceToCurrentPrefixForStartsWith(compareTo: ByteArray, compareOffset: Int): Boolean {
        val size = this.prefixSize
        val offset = this.prefixOffset
        val compareEnd = compareTo.size - compareOffset
        val compareSize = size.coerceAtMost(compareEnd)
        return Arrays.equals(
            this.prefixBacking,
            offset,
            offset + compareSize,
            compareTo,
            compareOffset,
            compareOffset + compareSize
        )
    }

    fun prefixElementAt(index: Int): Byte {
        return this.prefixBacking[this.prefixOffset + index]
    }

    private fun updateEvenOddFromNext(
        evenNode: EvenNybble<V>,
        evenOffset: Int,
        oddNode: OddNybble<V>,
        oddOffset: Int,
        nextOddNode: OddNybble<V>?,
    ): OddNybble<V>? {
        val nybbleValues = this.nybbleValues
        val nybbleDispatch = this.nybbleDispatch

        // No change, just return ourselves
        if (nextOddNode === oddNode) {
            return this
        }
        // We're now down one node, and have to perform a removal.
        if (nextOddNode == null) {
            if (this.size == 1L && this.valuePair == null) {
                // That odd node we just removed was our only node, and we didn't have any
                // values ourselves, so we can remove ourselves as well.
                return null
            }
            if (this.size == 2L && this.valuePair == null) {
                // We can promote the only remaining child to be the resulting
                // node, as there's no reason for this node to exist anymore.
                val returnedValue = if (nybbleDispatch.size == 1) {
                    if (evenNode.nybbleDispatch[0] === oddNode) {
                        evenNode.nybbleDispatch[1]
                    } else {
                        evenNode.nybbleDispatch[0]
                    }
                } else if (nybbleDispatch[0] === evenNode) {
                    val remainingEvenNybble = nybbleDispatch[1]
                    remainingEvenNybble.nybbleDispatch[0]
                } else {
                    val remainingEvenNybble = nybbleDispatch[0]
                    remainingEvenNybble.nybbleDispatch[0]
                }
                // The prefix of the resulting node will be the concatenation of:
                // 1. Our prefix
                // 2. The dispatching byte
                // 3. Their prefix
                // Since we're keeping slices, and the returned value strictly shares a prefix
                // with the node we're removing, all we have to do is update our slice to use
                // their backing but our offset.
                return OddNybble(
                    //ByteSlice(returnedPrefix.backing, thisPrefix.offset, thisPrefix.size + returnedPrefix.size + 1),
                    returnedValue.prefixBacking,
                    this.prefixOffset,
                    this.prefixSize + returnedValue.prefixSize + 1,
                    returnedValue.valuePair,
                    this.size - 1,
                    returnedValue.nybbleValues,
                    returnedValue.nybbleDispatch
                )
            }

            val nextEvenNode = if (evenNode.nybbleDispatch.size == 1) {
                null
            } else {
                val nextOddDispatch = Array(evenNode.nybbleDispatch.size - 1) { oddNode }
                removeArray(evenNode.nybbleDispatch, nextOddDispatch, oddOffset)
                EvenNybble(
                    removeByteArray(evenNode.nybbleValues, oddOffset),
                    nextOddDispatch
                )
            }
            val thisValuePair = this.valuePair
            if (nextEvenNode == null) {
                val nextEvenValues = removeByteArray(nybbleValues, evenOffset)
                val nextEvenDispatch = Array(nybbleDispatch.size - 1) { evenNode }
                removeArray(nybbleDispatch, nextEvenDispatch, evenOffset)
                return OddNybble(
                    byteSliceBackingForOddNybble(thisValuePair, nextEvenDispatch),
                    this.prefixOffset,
                    this.prefixSize,
                    thisValuePair,
                    this.size - 1,
                    nextEvenValues,
                    nextEvenDispatch
                )
            } else {
                val nextEvenDispatch = nybbleDispatch.clone()
                nextEvenDispatch[evenOffset] = nextEvenNode
                return OddNybble(
                    byteSliceBackingForOddNybble(thisValuePair, nextEvenDispatch),
                    this.prefixOffset,
                    this.prefixSize,
                    this.valuePair,
                    this.size - 1,
                    this.nybbleValues,
                    nextEvenDispatch
                )
            }
        } else {
            val nextOddDispatch = evenNode.nybbleDispatch.clone()
            nextOddDispatch[oddOffset] = nextOddNode

            val nextEvenNode = EvenNybble(
                evenNode.nybbleValues,
                nextOddDispatch
            )

            val nextEvenDispatch = nybbleDispatch.clone()
            nextEvenDispatch[evenOffset] = nextEvenNode

            val nextSize = if (nextOddNode.size < oddNode.size) {
                this.size - 1
            } else if (nextOddNode.size == oddNode.size) {
                this.size
            } else {
                this.size + 1
            }

            val thisValuePair = this.valuePair
            return OddNybble(
                byteSliceBackingForOddNybble(thisValuePair, nextEvenDispatch),
                this.prefixOffset,
                this.prefixSize,
                this.valuePair,
                nextSize,
                this.nybbleValues,
                nextEvenDispatch
            )
        }
    }

    // -1 means "access this"
    // >= 0 means "access the key at this offset for the dispatch".
    // < -1 means "add 2 then negate to get the size of the remaining key slice starting from offset" where
    // the keys would be equal. There may be remaining aspects of the key.
    private fun maybeKeyOffsetForAccess(key: ByteArray, offset: Int): Int {
        val remainder = key.size - offset
        val prefixSize = this.prefixSize
        // There's not enough bytes in the array to compare as equal.
        // This might indicate that we need to introduce a new node.
        val equalUpTo = byteArraysEqualUpTo(this.prefixBacking, this.prefixOffset, prefixSize, key, offset, prefixSize)
        if (equalUpTo < prefixSize) {
            return -(equalUpTo + 2) // equalUpTo == 0 -> -2, 1 -> -3, etc
        }
        // If the prefix is equal to the remainder of the key, we are going to be performing access at this object.
        return if (remainder == prefixSize) { -1 } else { offset + prefixSize }
    }

    fun update(
        key: ByteArray,
        offset: Int,
        copyKey: Boolean,
        updater: (prev: V?) -> V?
    ): OddNybble<V>? {
        val keyOffset = this.maybeKeyOffsetForAccess(key, offset)
        // We're updating ourselves here.
        if (keyOffset == -1) {
            val valuePair = this.valuePair
            val thisValue = valuePair?.value
            val result = updater(thisValue)
            if (result === thisValue) {
                return this
            }
            // this.valuePair == null && result == null is already handled
            // in the identity check above, so we don't need to worry about it
            // here.
            val nextSize = if (this.valuePair != null && result != null) {
                this.size
            } else if (this.valuePair == null) { // result != null
                this.size + 1
            } else { // this.value != null, result == null
                this.size - 1
            }
            if (nextSize < this.size) {
                if (this.nybbleValues.isEmpty()) {
                    // We've just removed the only reason this node exists,
                    // so we can remove the node itself.
                    return null
                }
                if (this.nybbleValues.size == 1) {
                    // There's not be a reason to keep this node around if there's exactly one child.
                    val evenNode = this.nybbleDispatch[0]
                    if (evenNode.nybbleDispatch.size == 1) {
                        val oddNode = evenNode.nybbleDispatch[0]
                        return OddNybble(
                            oddNode.prefixBacking,
                            this.prefixOffset,
                            this.prefixSize + oddNode.prefixSize + 1,
                            oddNode.valuePair,
                            oddNode.size,
                            oddNode.nybbleValues,
                            oddNode.nybbleDispatch
                        )
                    }
                }
            }
            return if (result == null) {
                val evenNode = this.nybbleDispatch[0]
                val oddNode = evenNode.nybbleDispatch[0]
                OddNybble(
                    oddNode.prefixBacking,
                    this.prefixOffset,
                    this.prefixSize,
                    null,
                    nextSize,
                    this.nybbleValues,
                    this.nybbleDispatch
                )
            } else {
                val keySave = valuePair?.key ?: if (copyKey) { key.copyOf() } else { key }
                OddNybble(
                    keySave,
                    this.prefixOffset,
                    keySave.size - this.prefixOffset,
                    QPTrieKeyValue(keySave, result),
                    nextSize,
                    this.nybbleValues,
                    this.nybbleDispatch
                )
            }
        }
        // We didn't match, but the equal portion of the key is shorter than our prefix.
        // If this is an addition we'll need to introduce a new node.
        if (keyOffset < -1) {
            val result = updater(null)
                ?: // null -> null means nothing to update.
                return this
            val topOffset = -(keyOffset + 2)
            val startOffset = offset + topOffset
            val incumbentSliceOffset = topOffset + 1
            val incumbentNode = OddNybble(
                // Note that target is already a dispatched byte, so we skip the very
                // first prefix byte.
                this.prefixBacking,
                this.prefixOffset + incumbentSliceOffset,
                this.prefixSize - incumbentSliceOffset,
                this.valuePair,
                this.size,
                this.nybbleValues,
                this.nybbleDispatch
            )
            val keySave = if (copyKey) { key.copyOf() } else { key }
            // If the startOffset is just beyond the actual key length, we have to insert a node "above" us.
            if (startOffset >= key.size) {
                val target = this.prefixElementAt(topOffset)
                val evenNode = EvenNybble(
                    byteArrayOf(oddNybbleFromByte(target)),
                    arrayOf(incumbentNode)
                )
                return OddNybble(
                    keySave,
                    offset,
                    startOffset - offset,
                    QPTrieKeyValue(keySave, result),
                    this.size + 1,
                    byteArrayOf(evenNybbleFromByte(target)),
                    arrayOf(evenNode)
                )
            } else {
                val target = key[startOffset]
                // We now need an intermediate node that dispatches to both new nodes.
                val priorByte = this.prefixElementAt(topOffset)
                val prefixOffset = startOffset + 1

                // See the definition of emptyEvenNybbleArray above for a rationale behind this suppression.
                @Suppress("UNCHECKED_CAST")
                val newNode = OddNybble(
                    keySave,
                    prefixOffset,
                    keySave.size - prefixOffset,
                    QPTrieKeyValue(keySave, result),
                    1,
                    emptyByteArray,
                    emptyEvenNybbleArray as Array<EvenNybble<V>>
                )
                val newHighNybble = evenNybbleFromByte(target)
                val priorHighNybble = evenNybbleFromByte(priorByte)
                val newEvenDispatch: Array<EvenNybble<V>>
                val newEvenValues: ByteArray
                if (newHighNybble == priorHighNybble) {
                    // Only one EvenNybble dispatching to two odd nodes
                    val priorLowNybble = oddNybbleFromByte(priorByte)
                    val oddValuesOnlyNew = byteArrayOf(oddNybbleFromByte(target))
                    // findByteInSortedArray will necessarily be negative here.
                    val insertOffset = -(findByteInSortedArray(oddValuesOnlyNew, priorLowNybble) + 1)
                    val oddValues = insertByteArray(oddValuesOnlyNew, insertOffset, priorLowNybble)
                    val oddDispatch = Array(2) { newNode }
                    insertArray(arrayOf(newNode), oddDispatch, insertOffset, incumbentNode)
                    newEvenValues = byteArrayOf(newHighNybble)
                    newEvenDispatch = arrayOf(EvenNybble(oddValues, oddDispatch))
                } else {
                    // Two EvenNybbles dispatching to one odd node each
                    val evenValuesOnlyNew = byteArrayOf(newHighNybble)
                    // findByteInSortedArray will necessarily be negative here.
                    val insertOffset = -(findByteInSortedArray(evenValuesOnlyNew, priorHighNybble) + 1)
                    newEvenValues = insertByteArray(evenValuesOnlyNew, insertOffset, priorHighNybble)
                    val newEvenNode = EvenNybble(byteArrayOf(oddNybbleFromByte(target)), arrayOf(newNode))
                    newEvenDispatch = Array(2) { newEvenNode }
                    insertArray(
                        arrayOf(newEvenNode),
                        newEvenDispatch,
                        insertOffset,
                        EvenNybble(byteArrayOf(oddNybbleFromByte(priorByte)), arrayOf(incumbentNode))
                    )
                }
                val firstNewEven = newEvenDispatch[0]
                val firstNewOdd = firstNewEven.nybbleDispatch[0]
                return OddNybble(
                    firstNewOdd.prefixBacking,
                    offset,
                    startOffset - offset,
                    null,
                    this.size + 1,
                    newEvenValues,
                    newEvenDispatch
                )
            }
        }

        // At this point, the key is longer than our prefix supports, but the prefix matches fully.
        // Let's now figure out if we already have a child node to which we can delegate this update.
        val childTarget = key[keyOffset]
        val evenOffset = QPTrieUtils.offsetForNybble(this.nybbleValues, evenNybbleFromByte(childTarget))
        var evenNode: EvenNybble<V>? = null
        if (evenOffset > -1) {
            evenNode = this.nybbleDispatch[evenOffset]
            val oddOffset = QPTrieUtils.offsetForNybble(evenNode.nybbleValues, oddNybbleFromByte(childTarget))
            if (oddOffset > -1) {
                val oddNode = evenNode.nybbleDispatch[oddOffset]
                return this.updateEvenOddFromNext(
                    evenNode,
                    evenOffset,
                    oddNode,
                    oddOffset,
                    oddNode.update(key, keyOffset + 1, copyKey, updater)
                )
            }
        }

        // This would be a new value, even though we already have data to work with.
        // Note that by the time we're here, we're always inserting a new node.
        val result = updater(null) ?: return this

        val keySave = if (copyKey) { key.copyOf() } else { key }
        val prefixOffset = keyOffset + 1

        // See the definition of emptyEvenNybbleArray above for a rationale behind this suppression.
        @Suppress("UNCHECKED_CAST")
        val bottomNode = OddNybble(
            keySave,
            prefixOffset,
            keySave.size - prefixOffset,
            QPTrieKeyValue(keySave, result),
            1,
            emptyByteArray,
            emptyEvenNybbleArray as Array<EvenNybble<V>>
        )
        val newEvenNode: EvenNybble<V> = if (evenNode == null) {
            EvenNybble(
                byteArrayOf(oddNybbleFromByte(childTarget)),
                arrayOf(bottomNode)
            )
        } else {
            val targetLowNybble = oddNybbleFromByte(childTarget)
            val foundOffset = findByteInSortedArray(evenNode.nybbleValues, targetLowNybble)
            // foundOffset is necessarily negative here. If it weren't,
            // we would have `oddOffset` > -1 above, and would have returned.
            val oddOffset = -(foundOffset + 1)
            val nextValues = insertByteArray(evenNode.nybbleValues, oddOffset, targetLowNybble)
            val nextDispatch = Array(evenNode.nybbleDispatch.size + 1) { bottomNode }
            insertArray(evenNode.nybbleDispatch, nextDispatch, oddOffset, bottomNode)
            EvenNybble(
                nextValues,
                nextDispatch
            )
        }
        val thisValuePair = this.valuePair
        return if (this.nybbleValues.isEmpty()) {
            val nextEvenDispatch = arrayOf(newEvenNode)

            OddNybble(
                byteSliceBackingForOddNybble(thisValuePair, nextEvenDispatch),
                this.prefixOffset,
                this.prefixSize,
                thisValuePair,
                this.size + 1,
                byteArrayOf(evenNybbleFromByte(childTarget)),
                nextEvenDispatch
            )
        } else {
            val targetHighNybble = evenNybbleFromByte(childTarget)
            val foundOffset = findByteInSortedArray(this.nybbleValues, targetHighNybble)
            if (foundOffset < 0) {
                val insertOffset = -(foundOffset + 1)
                val newNybbleValues = insertByteArray(this.nybbleValues, insertOffset, targetHighNybble)
                val newNybbleDispatch = Array(this.nybbleDispatch.size + 1) { newEvenNode }
                insertArray(this.nybbleDispatch, newNybbleDispatch, insertOffset, newEvenNode)

                OddNybble(
                    byteSliceBackingForOddNybble(thisValuePair, newNybbleDispatch),
                    this.prefixOffset,
                    this.prefixSize,
                    this.valuePair,
                    this.size + 1,
                    newNybbleValues,
                    newNybbleDispatch
                )
            } else {
                val newNybbleDispatch = this.nybbleDispatch.clone()
                newNybbleDispatch[foundOffset] = newEvenNode

                return OddNybble(
                    byteSliceBackingForOddNybble(thisValuePair, newNybbleDispatch),
                    this.prefixOffset,
                    this.prefixSize,
                    thisValuePair,
                    this.size + 1,
                    this.nybbleValues,
                    newNybbleDispatch
                )
            }
        }
    }

    fun fullIteratorAscending(): Iterator<QPTrieKeyValue<V>> {
        val currentValue = this.valuePair
        return if (this.nybbleValues.isEmpty()) {
            // Note that currentValue != null by necessity here, so we don't need
            // an extra path for EmptyIterator.
            SingleElementIterator(currentValue!!)
        } else if (currentValue != null) {
            FullAscendingOddNybbleIteratorWithValue(this)
        } else {
            FullAscendingOddNybbleIteratorWithoutValue(this)
        }
    }

    fun fullIteratorDescending(): Iterator<QPTrieKeyValue<V>> {
        val currentValue = this.valuePair
        return if (this.nybbleValues.isEmpty()) {
            // Note that currentValue != null by necessity here, so we don't need
            // an extra path for EmptyIterator.
            SingleElementIterator(currentValue!!)
        } else if (currentValue != null) {
            FullDescendingOddNybbleIteratorWithValue(this)
        } else {
            FullDescendingOddNybbleIteratorWithoutValue(this)
        }
    }

    fun iteratorForLessThanOrEqual(
        compareTo: ByteArray,
        compareOffset: Int
    ): Iterator<QPTrieKeyValue<V>> {
        val comparison = this.compareLookupSliceToCurrentPrefix(compareTo, compareOffset)
        val endCompareOffset = compareOffset + this.prefixSize
        return if (comparison < 0) {
            // Our prefix was fully less than the comparison slice, so all members are less than the
            // comparison, and we can just iterate descending.
            this.fullIteratorDescending()
        } else if (comparison > 0) {
            // Our prefix was fully greater than the comparison slice, and all of our members
            // are greater than us and thus greater than the comparison slice, so we're not
            // returning anything.
            EmptyIterator()
        } else if (nybbleValues.isEmpty() || compareTo.size <= endCompareOffset) {
            // We didn't have anything to dispatch, so we can possibly just return ourselves.
            // OR The compared value was fully identified by our prefix, so all of our members
            // are greater and should be skipped.
            if (this.valuePair !== null) {
                SingleElementIterator(this.valuePair)
            } else {
                EmptyIterator()
            }
        } else {
            // We now have to figure out which is the "greater or equal" nybble.
            var greaterNybbleOffset = 0
            var equalNybbleOffset = this.nybbleValues.size
            val targetUpperNybble = evenNybbleFromByte(compareTo[endCompareOffset])
            for (i in 0 until this.nybbleValues.size) {
                val elementComparison = compareBytesUnsigned(this.nybbleValues[i], targetUpperNybble)
                if (elementComparison < 0) {
                    greaterNybbleOffset += 1
                } else if (elementComparison == 0) {
                    equalNybbleOffset = i
                    greaterNybbleOffset += 1
                    break
                } else {
                    break
                }
            }

            return LessThanOrEqualOddNybbleIterator(
                this,
                compareTo,
                compareOffset + this.prefixSize,
                greaterNybbleOffset,
                equalNybbleOffset
            )
        }
    }

    fun iteratorForGreaterThanOrEqual(
        compareTo: ByteArray,
        compareOffset: Int,
    ): Iterator<QPTrieKeyValue<V>> {
        val comparison = this.compareLookupSliceToCurrentPrefix(compareTo, compareOffset)
        val endCompareOffset = compareOffset + this.prefixSize
        return if (comparison < 0) {
            // If our prefix was fully less than the inspected slice, all of our members will
            // also be less than the full comparison
            EmptyIterator()
        } else if (comparison > 0 || endCompareOffset >= compareTo.size) {
            // If our prefix was fully greater than the inspected slice, all of our members will
            // also be greater than the full comparison
            // OR the lookup key was fully equal to all the prefixes up until this node's point,
            // and all the children will necessarily be greater than the lookup key.
            this.fullIteratorAscending()
        } else if (this.nybbleValues.isEmpty()) {
            // We didn't have anything to dispatch. At this point we know that our path
            // was actually less than the lookup, so we're completely less than the lookup
            // and shouldn't be reported.
            EmptyIterator()
        } else {
            var greaterEqualOffset = 0
            var equalOffset = this.nybbleValues.size
            val targetUpperNybble = evenNybbleFromByte(compareTo[endCompareOffset])
            for (i in 0 until this.nybbleValues.size) {
                val elementComparison = compareBytesUnsigned(this.nybbleValues[i], targetUpperNybble)
                if (elementComparison < 0) {
                    greaterEqualOffset += 1
                } else {
                    if (elementComparison == 0) {
                        equalOffset = i
                    }
                    break
                }
            }
            return GreaterThanOddNybbleIterator(
                this,
                compareTo,
                endCompareOffset,
                greaterEqualOffset,
                equalOffset
            )
        }
    }

    fun iteratorForStartsWith(
        compareTo: ByteArray,
        compareOffset: Int
    ): Iterator<QPTrieKeyValue<V>> {
        val compareResult = this.testEqualLookupSliceToCurrentPrefixForStartsWith(compareTo, compareOffset)
        val endCompareOffset = compareOffset + this.prefixSize
        return if (!compareResult) {
            EmptyIterator()
        } else if (endCompareOffset >= compareTo.size) {
            this.fullIteratorAscending()
        } else {
            val target = compareTo[endCompareOffset]
            val evenNode = this.dispatchByte(target) ?: return EmptyIterator()
            evenNode.iteratorForStartsWith(
                target,
                compareTo,
                endCompareOffset,
            )
        }
    }
}

private class FullAscendingOddNybbleIteratorWithValue<V>(
    private val node: OddNybble<V>,
): ConcatenatedIterator<QPTrieKeyValue<V>>() {
    override fun iteratorForOffset(offset: Int): Iterator<QPTrieKeyValue<V>>? {
        val node = this.node
        if (offset == 0) {
            return SingleElementIterator(node.valuePair!!)
        }
        val evenOffset = offset - 1
        val nybbleDispatch = node.nybbleDispatch
        return if (nybbleDispatch.size <= evenOffset) {
             null
        } else {
            nybbleDispatch[evenOffset].fullIteratorAscending()
        }
    }
}

private class FullAscendingOddNybbleIteratorWithoutValue<V>(
    private val node: OddNybble<V>,
): ConcatenatedIterator<QPTrieKeyValue<V>>() {
    override fun iteratorForOffset(offset: Int): Iterator<QPTrieKeyValue<V>>? {
        val node = this.node
        val nybbleDispatch = node.nybbleDispatch
        return if (nybbleDispatch.size <= offset) {
            null
        } else {
            nybbleDispatch[offset].fullIteratorAscending()
        }
    }
}

private class FullDescendingOddNybbleIteratorWithValue<V>(
    private val node: OddNybble<V>,
): ConcatenatedIterator<QPTrieKeyValue<V>>() {
    override fun iteratorForOffset(offset: Int): Iterator<QPTrieKeyValue<V>>? {
        val node = this.node
        val nybbleDispatch = node.nybbleDispatch
        val maxOffset = nybbleDispatch.size
        return if (offset == maxOffset) {
            SingleElementIterator(node.valuePair!!)
        } else if (offset < maxOffset) {
            val reverseOffset = maxOffset - offset - 1
            nybbleDispatch[reverseOffset].fullIteratorDescending()
        } else {
            null
        }
    }
}

private class FullDescendingOddNybbleIteratorWithoutValue<V>(
    private val node: OddNybble<V>,
): ConcatenatedIterator<QPTrieKeyValue<V>>() {
    override fun iteratorForOffset(offset: Int): Iterator<QPTrieKeyValue<V>>? {
        val node = this.node
        val nybbleDispatch = node.nybbleDispatch
        val maxOffset = nybbleDispatch.size
        return if (offset < maxOffset) {
            val reverseOffset = maxOffset - offset - 1
            nybbleDispatch[reverseOffset].fullIteratorDescending()
        } else {
            null
        }
    }
}

private class LessThanOrEqualOddNybbleIterator<V>(
    private val node: OddNybble<V>,
    private val compareTo: ByteArray,
    private val compareOffset: Int,
    private val greaterNybbleOffset: Int,
    private val equalNybbleOffset: Int
): ConcatenatedIterator<QPTrieKeyValue<V>>() {
    override fun iteratorForOffset(offset: Int): Iterator<QPTrieKeyValue<V>>? {
        val reverseOffset = this.greaterNybbleOffset - offset - 1
        val node = this.node
        val nybbleDispatch = node.nybbleDispatch

        if (reverseOffset == this.equalNybbleOffset) {
            return nybbleDispatch[reverseOffset].iteratorForLessThanOrEqual(this.compareTo, this.compareOffset)
        }
        val value = this.node.valuePair
        return if (reverseOffset >= 0) {
            nybbleDispatch[reverseOffset].fullIteratorDescending()
        } else if (reverseOffset == -1 && value != null) {
            SingleElementIterator(value)
        } else {
            null
        }
    }
}

private class GreaterThanOddNybbleIterator<V>(
    private val node: OddNybble<V>,
    private val compareTo: ByteArray,
    private val compareOffset: Int,
    private val greaterOrEqualNybbleOffset: Int,
    private val equalNybbleOffset: Int
): ConcatenatedIterator<QPTrieKeyValue<V>>() {
    override fun iteratorForOffset(offset: Int): Iterator<QPTrieKeyValue<V>>? {
        val nodeOffset = offset + this.greaterOrEqualNybbleOffset
        val node = this.node

        val nybbleDispatch = node.nybbleDispatch
        return if (nodeOffset >= nybbleDispatch.size) {
            null
        } else if (nodeOffset == this.equalNybbleOffset) {
            nybbleDispatch[nodeOffset].iteratorForGreaterThanOrEqual(
                this.compareTo,
                this.compareOffset
            )
        } else {
            nybbleDispatch[nodeOffset].fullIteratorAscending()
        }
    }
}

private class EvenNybble<V>(
    val nybbleValues: ByteArray,
    val nybbleDispatch: Array<OddNybble<V>>
) {
    fun dispatchByte(target: Byte): OddNybble<V>? {
        val offset = QPTrieUtils.offsetForNybble(this.nybbleValues, oddNybbleFromByte(target))
        return if (offset < 0) { null } else { this.nybbleDispatch[offset] }
    }

    fun fullIteratorAscending(): FullAscendingEvenIterator<V> {
        return FullAscendingEvenIterator(this)
    }

    fun fullIteratorDescending(): FullDescendingEvenIterator<V> {
        return FullDescendingEvenIterator(this)
    }

    fun iteratorForLessThanOrEqual(
        compareTo: ByteArray,
        compareByteOffset: Int
    ): LessThanOrEqualEvenNybbleIterator<V> {
        // compareByteOffset should be <= compareTo.size when calling this.
        return LessThanOrEqualEvenNybbleIterator(
            this,
            oddNybbleFromByte(compareTo[compareByteOffset]),
            compareTo,
            compareByteOffset + 1
        )
    }

    fun iteratorForGreaterThanOrEqual(
        compareTo: ByteArray,
        compareByteOffset: Int
    ): ConcatenatedIterator<QPTrieKeyValue<V>> {
        // compareByteOffset should be <= compareTo.size when calling this.
        val targetNybble = oddNybbleFromByte(compareTo[compareByteOffset])
        var greaterOrEqualNybbleOffset = 0
        var equalNybbleOffset = this.nybbleValues.size
        for (i in 0 until this.nybbleValues.size) {
            val elementComparison = compareBytesUnsigned(this.nybbleValues[i], targetNybble)
            if (elementComparison < 0) {
                greaterOrEqualNybbleOffset += 1
            } else {
                if (elementComparison == 0) {
                    equalNybbleOffset = i
                }
                break
            }
        }

        return GreaterThanOrEqualToEvenNybbleIterator(
            this,
            compareTo,
            compareByteOffset + 1,
            greaterOrEqualNybbleOffset,
            equalNybbleOffset
        )
    }

    fun iteratorForStartsWith(
        target: Byte,
        compareTo: ByteArray,
        compareOffset: Int
    ): Iterator<QPTrieKeyValue<V>> {
        val oddNode = this.dispatchByte(target) ?: return EmptyIterator()
        return oddNode.iteratorForStartsWith(
            compareTo,
            compareOffset + 1,
        )
    }
}

private class FullAscendingEvenIterator<V>(
    private val node: EvenNybble<V>
) : ConcatenatedIterator<QPTrieKeyValue<V>>() {
    override fun iteratorForOffset(offset: Int): Iterator<QPTrieKeyValue<V>>? {
        val node = this.node
        val nybbleDispatch = node.nybbleDispatch
        if (nybbleDispatch.size <= offset) {
            return null
        }
        return nybbleDispatch[offset].fullIteratorAscending()
    }
}

private class FullDescendingEvenIterator<V>(
    private val node: EvenNybble<V>
): ConcatenatedIterator<QPTrieKeyValue<V>>() {
    override fun iteratorForOffset(offset: Int): Iterator<QPTrieKeyValue<V>>? {
        val node = this.node
        val nybbleDispatch = node.nybbleDispatch
        val dispatchSize = nybbleDispatch.size
        if (dispatchSize <= offset) {
            return null
        }
        val reverseOffset = dispatchSize - offset - 1
        return nybbleDispatch[reverseOffset].fullIteratorDescending()
    }
}

private class LessThanOrEqualEvenNybbleIterator<V>(
    private val node: EvenNybble<V>,
    private val compareBottom: Byte,
    private val compareTo: ByteArray,
    private val nextCompareOffset: Int
) : ConcatenatedIterator<QPTrieKeyValue<V>>() {
    private val greaterThanOffset: Int

    init {
        var currentGreaterThanOffset = 0
        for (element in node.nybbleValues) {
            if (compareBytesUnsigned(element, compareBottom) <= 0) {
                currentGreaterThanOffset += 1
            } else {
                break
            }
        }
        greaterThanOffset = currentGreaterThanOffset
    }

    override fun iteratorForOffset(offset: Int): Iterator<QPTrieKeyValue<V>>? {
        val reverseOffset = this.greaterThanOffset - offset - 1
        if (reverseOffset < 0) {
            return null
        }
        val node = this.node
        val nybbleValue = node.nybbleValues[reverseOffset]
        return if (offset == 0 && compareBytesUnsigned(nybbleValue, this.compareBottom) >= 0) {
            // Note that even at the zero offset, if the nybble value is actually less than or equal to
            // the comparison, we can perform a full iteration.
            node.nybbleDispatch[reverseOffset].iteratorForLessThanOrEqual(this.compareTo, this.nextCompareOffset)
        } else {
            node.nybbleDispatch[reverseOffset].fullIteratorDescending()
        }
    }
}

private class GreaterThanOrEqualToEvenNybbleIterator<V>(
    private val node: EvenNybble<V>,
    private val compareTo: ByteArray,
    private val compareOffset: Int,
    private val greaterOrEqualNybbleOffset: Int,
    private val equalNybbleOffset: Int
): ConcatenatedIterator<QPTrieKeyValue<V>>() {
    private val dispatchSize = node.nybbleDispatch.size

    override fun iteratorForOffset(offset: Int): Iterator<QPTrieKeyValue<V>>? {
        val nodeOffset = offset + this.greaterOrEqualNybbleOffset
        if (nodeOffset >= this.dispatchSize) {
            return null
        }
        val node = this.node
        return if (nodeOffset == this.equalNybbleOffset) {
            node.nybbleDispatch[nodeOffset].iteratorForGreaterThanOrEqual(this.compareTo, this.compareOffset)
        } else {
            node.nybbleDispatch[nodeOffset].fullIteratorAscending()
        }
    }
}

private tailrec fun<V> getValue(node: OddNybble<V>, key: ByteArray, offset: Int): V? {
    val endOffset = offset + node.prefixSize
    val keySize = key.size
    if (endOffset < keySize) {
        val target = key[endOffset]
        val child = node.dispatchByte(target)?.dispatchByte(target) ?: return null
        return getValue(child, key, endOffset + 1)
    } else if (endOffset == keySize) {
        val valuePair = node.valuePair
        return if (valuePair == null || !key.contentEquals(valuePair.key)) {
            null
        } else {
            valuePair.value
        }
    } else {
        // endOffset > keySize
        return null
    }
}

private tailrec fun<V> minKeyValue(node: OddNybble<V>): QPTrieKeyValue<V> {
    val valuePair = node.valuePair
    return if (valuePair != null) {
        // If the valuePair != null, the key is necessarily less than all keys
        // present in nybbleDispatch, because it is a strict prefix of them.
        valuePair
    } else {
        // If node.valuePair == null, this node _must_ have children in the
        // nybbleDispatch. Completely empty nodes are never stored as part
        // of the update process.
        val nextNode = node.nybbleDispatch[0].nybbleDispatch[0]
        minKeyValue(nextNode)
    }
}

private fun<V> visitAscendingUnsafeSharedKeyImpl(node: OddNybble<V>, receiver: VisitReceiver<V>) {
    val valuePair = node.valuePair
    val evenDispatch = node.nybbleDispatch
    if (valuePair != null) {
        receiver(valuePair)
    }
    for (evenNybble in evenDispatch) {
        val oddDispatch = evenNybble.nybbleDispatch
        for (oddNybble in oddDispatch) {
            visitAscendingUnsafeSharedKeyImpl(oddNybble, receiver)
        }
    }
}

private fun<V> visitDescendingUnsafeSharedKeyImpl(node: OddNybble<V>, receiver: VisitReceiver<V>) {
    val evenDispatch = node.nybbleDispatch
    for (i in evenDispatch.size - 1 downTo 0) {
        val oddDispatch = evenDispatch[i].nybbleDispatch
        for (j in oddDispatch.size - 1 downTo 0) {
            visitDescendingUnsafeSharedKeyImpl(oddDispatch[j], receiver)
        }
    }
    val valuePair = node.valuePair
    if (valuePair != null) {
        receiver(valuePair)
    }
}

private fun<V> visitLessThanOrEqualUnsafeSharedKeyImpl(
    node: OddNybble<V>,
    compareTo: ByteArray,
    compareOffset: Int,
    receiver: VisitReceiver<V>
) {
    val prefixComparison = node.compareLookupSliceToCurrentPrefix(
        compareTo,
        compareOffset - node.prefixSize
    )
    if (prefixComparison < 0) {
        return visitDescendingUnsafeSharedKeyImpl(node, receiver)
    }
    if (prefixComparison > 0) {
        return
    }
    if (compareOffset >= compareTo.size) {
        if (node.valuePair != null) {
            receiver(node.valuePair)
        }
        return
    }

    val compareByte = compareTo[compareOffset]
    val compareHigh = evenNybbleFromByte(compareByte)
    val compareLow = oddNybbleFromByte(compareByte)
    val nybbleValues = node.nybbleValues
    val nybbleDispatch = node.nybbleDispatch
    var i = nybbleValues.size - 1
    while (i >= 0 && compareBytesUnsigned(nybbleValues[i], compareHigh) > 0) {
        i--
    }
    if (i < 0) {
        if (node.valuePair != null) {
            receiver(node.valuePair)
        }
        return
    }
    if (nybbleValues[i] == compareHigh) {
        val evenNybble = nybbleDispatch[i]
        val oddValues = evenNybble.nybbleValues
        val oddDispatch = evenNybble.nybbleDispatch
        var j = oddValues.size - 1
        while (j >= 0 && compareBytesUnsigned(oddValues[j], compareLow) > 0) {
            j--
        }
        if (j >= 0) {
            if (oddValues[j] == compareLow) {
                val oddNybble = oddDispatch[j]
                visitLessThanOrEqualUnsafeSharedKeyImpl(
                    oddNybble,
                    compareTo,
                    oddNybble.prefixSize + compareOffset + 1,
                    receiver
                )
                j -= 1
            }
        }
        while (j >= 0) {
            visitDescendingUnsafeSharedKeyImpl(oddDispatch[j], receiver)
            j -= 1
        }
        i -= 1
    }
    while (i >= 0) {
        val oddDispatch = nybbleDispatch[i].nybbleDispatch
        for (j in oddDispatch.size - 1 downTo 0) {
            visitDescendingUnsafeSharedKeyImpl(oddDispatch[j], receiver)
        }
        i -= 1
    }
    if (node.valuePair != null) {
        receiver(node.valuePair)
    }
}

private fun<V> visitGreaterThanOrEqualUnsafeSharedKeyImpl(
    node: OddNybble<V>,
    compareTo: ByteArray,
    compareOffset: Int,
    receiver: VisitReceiver<V>
) {
    val prefixComparison = node.compareLookupSliceToCurrentPrefix(
        compareTo,
        compareOffset - node.prefixSize
    )
    if (prefixComparison < 0) {
        return
    }
    if (prefixComparison > 0 || compareOffset >= compareTo.size) {
        return visitAscendingUnsafeSharedKeyImpl(node, receiver)
    }

    val compareByte = compareTo[compareOffset]
    val compareHigh = evenNybbleFromByte(compareByte)
    val compareLow = oddNybbleFromByte(compareByte)
    val nybbleValues = node.nybbleValues
    val nybbleDispatch = node.nybbleDispatch
    var i = 0
    while (i < nybbleValues.size && compareBytesUnsigned(nybbleValues[i], compareHigh) < 0) {
        i++
    }
    if (i >= nybbleValues.size) {
        return
    }
    if (nybbleValues[i] == compareHigh) {
        val evenNybble = nybbleDispatch[i]
        val evenValues = evenNybble.nybbleValues
        val evenDispatch = evenNybble.nybbleDispatch
        var j = 0
        while (j < evenValues.size && compareBytesUnsigned(evenValues[j], compareLow) < 0) {
            j++
        }
        if (j < evenValues.size) {
            if (evenValues[j] == compareLow) {
                val oddNybble = evenDispatch[j]
                visitGreaterThanOrEqualUnsafeSharedKeyImpl(
                    oddNybble,
                    compareTo,
                    compareOffset + oddNybble.prefixSize + 1,
                    receiver
                )
                j++
            }
            while (j < evenValues.size) {
                visitAscendingUnsafeSharedKeyImpl(evenDispatch[j], receiver)
                j++
            }
        }
        i++
    }
    while (i < nybbleDispatch.size) {
        val oddDispatch = nybbleDispatch[i].nybbleDispatch
        for (element in oddDispatch) {
            visitAscendingUnsafeSharedKeyImpl(element, receiver)
        }
        i++
    }
}

private tailrec fun<V> visitStartsWithUnsafeSharedKeyImpl(
    node: OddNybble<V>,
    compareTo: ByteArray,
    compareOffset: Int,
    receiver: VisitReceiver<V>
) {
    val compareResult = node.testEqualLookupSliceToCurrentPrefixForStartsWith(
        compareTo,
        compareOffset - node.prefixSize
    )
    if (!compareResult) {
        return
    }
    if (compareOffset >= compareTo.size) {
        return visitAscendingUnsafeSharedKeyImpl(node, receiver)
    }

    val compareByte = compareTo[compareOffset]
    val child = node.dispatchByte(compareByte)?.dispatchByte(compareByte) ?: return
    return visitStartsWithUnsafeSharedKeyImpl(
        child,
        compareTo,
        compareOffset + child.prefixSize + 1,
        receiver
    )
}

private tailrec fun<V> visitPrefixOfOrEqualToUnsafeSharedKeyImpl(
    node: OddNybble<V>,
    compareTo: ByteArray,
    compareOffset: Int,
    receiver: VisitReceiver<V>
) {
    if (compareOffset > compareTo.size) {
        return
    }
    val prefixComparison = node.compareLookupSliceToCurrentPrefix(
        compareTo,
        compareOffset - node.prefixSize
    )
    if (prefixComparison != 0) {
        return
    }
    if (node.valuePair != null) {
        receiver(node.valuePair)
    }
    if (compareOffset == compareTo.size) {
        return
    }
    val compareByte = compareTo[compareOffset]
    val child = node.dispatchByte(compareByte)?.dispatchByte(compareByte) ?: return
    return visitPrefixOfOrEqualToUnsafeSharedKeyImpl(
        child,
        compareTo,
        compareOffset + child.prefixSize + 1,
        receiver
    )
}

private fun<V> keysInto(node: OddNybble<V>, result: ArrayList<ByteArray>) {
    visitAscendingUnsafeSharedKeyImpl(node) {
        result.add(it.key)
    }
}

/**
 * An immutable, persistent, CPU and memory efficient ordered associative map from ByteArray keys to arbitrary values.
 *
 * This collection holds pairs of objects, keys mapping to values, and supports efficiently:
 * - Adding, removing, and updated values corresponding to a given key
 * - Retrieving the value corresponding to each key
 * - Retrieving keys and values where the key is less than or equal to a given value
 * - Retrieving keys and values where the key is greater than or equal to a given value
 * - Retrieving keys and values where the key starts with a given value
 * - Retrieving keys and values where the given value starts with the key
 * - Retrieving the key/value pair with the minimum key value
 *
 * Despite the immutable nature of these collections, the internals attempt to avoid memory allocation unless it is
 * strictly necessary. There are a few implications to this allocation avoidance:
 * - Any method whose name ends with UnsafeSharedKey will return a `QPTrieKeyValue<V>` where the `key` is internally
 *   used by `get`, so callers must not mutate this value.
 * - `update` and most methods whose name starts with `visit` are implemented recursively. Sparse sets of keys might
 *   not run into stack overflow situations, but dense sets of keys over 1KB in size do run the risk of stack
 *   overflow. It may be necessary to tune the available stack size, or limit the maximum size of the stored keys.
 *
 * The name "QPTrie" is derived from "quadbit popcount patricia trie" as per its initial description in
 * https://dotat.at/prog/qp/blog-2015-10-04.html. It is a radix-16 trie where much of the lookup operations occur
 * at subsets of the lookup key.
 *
 */
class QPTrie<V>: Iterable<QPTrieKeyValue<V>> {
    companion object {
        /**
         * Reports whether the given [keySize] might ever cause a [StackOverflowError] during updates or visitation
         *
         * The internal design of QPTrie relies heavily on call stack allocation, which tends to be very limited
         * compared to heap allocation. Whenever we've exhausted the allocation on the stack, our executing environment
         * will raise a [StackOverflowError], which we will often need to avoid in the real world. This function exists
         * to verify whether a worst-case [keySize] can be used for both updating a QPTrie and calling
         * [QPTrie.visitUnsafeSharedKey] without risk of a [StackOverflowError].
         */
        fun keySizeIsSafeFromOverflow(keySize: Int): Boolean {
            if (keySize < 1) {
                return true
            }

            val basePair = QPTrieKeyValue(emptyByteArray, 1)

            @Suppress("UNCHECKED_CAST")
            var oddNybble = OddNybble(
                emptyByteArray,
                0,
                0,
                basePair,
                1,
                emptyByteArray,
                emptyEvenNybbleArray as Array<EvenNybble<Int>>,
            )
            for (i in 2..keySize * 2) {
                val evenNybble = EvenNybble(
                    byteArrayOf(0),
                    arrayOf(oddNybble)
                )
                val headPair = QPTrieKeyValue(emptyByteArray, i)
                oddNybble = OddNybble(
                    emptyByteArray,
                    0,
                    0,
                    headPair,
                    i.toLong(),
                    byteArrayOf(0),
                    arrayOf(evenNybble)
                )
            }

            var trie = QPTrie(oddNybble)

            try {
                // First test: recursive update
                val keyArray = ByteArray(keySize + 1)
                keyArray[keySize] = 1
                // This should be prone to StackOverflowError.
                trie = trie.update(keyArray) { it ?: 0 }

                // Second test: full iteration
                var seenEntries = 0
                // This should be prone to StackOverflowError.
                trie.visitUnsafeSharedKey {
                    seenEntries++
                }
                return seenEntries > 0
            } catch (e: StackOverflowError) {
                return false
            }
        }
    }

    private val root: OddNybble<V>?

    /**
     * Returns the number of key/value pairs present in this [QPTrie].
     *
     * This is a Long instead of the standard Int to support more than ~2 billion members.
     */
    val size: Long

    private constructor(baseRoot: OddNybble<V>?) {
        this.root = baseRoot
        this.size = baseRoot?.size ?: 0
    }

    constructor() {
        this.root = null
        this.size = 0
    }

    constructor(items: Iterable<Pair<ByteArray, V>>) {
        this.root = oddNybbleFromIterator(items)
        this.size = this.root?.size ?: 0
    }

    /**
     * Returns the value corresponding to the given [key], or `null` if such a key is not present in this [QPTrie].
     */
    fun get(key: ByteArray): V? {
        val root = this.root ?: return null
        return getValue(root, key, 0)
    }

    private fun updateMaybeWithKeyCopy(key: ByteArray, updater: (prev: V?) -> V?, copyKey: Boolean): QPTrie<V> {
        val root = this.root
        return if (root == null) {
            val value = updater(null) ?: return this
            val keyCopy = if (copyKey) { key.copyOf() } else { key }
            QPTrie(
                OddNybble(
                    keyCopy,
                    0,
                    keyCopy.size,
                    QPTrieKeyValue(keyCopy, value),
                    1,
                    emptyByteArray,
                    arrayOf()
                )
            )
        } else {
            val newRoot = root.update(key, 0, copyKey, updater)
            if (newRoot === root) {
                this
            } else {
                QPTrie(newRoot)
            }
        }
    }

    /**
     * Returns a new [QPTrie], wherein the value for the given [key] is updated by the [updater] callback.
     *
     * This method is implemented recursively, so care should be taken to ensure that the keys present in the QPTrie
     * are short enough, or sparse enough, to prevent [StackOverflowError]s.
     *
     * The new QPTrie will actually be the given QPTrie if the resulting `value` from `updater` is identity-equal to the
     * value already present in the given QPTrie corresponding to the given key. This means that a `null` update to a
     * QPTrie without a value corresponding to the given key will return the same QPTrie, and an update to a QPTrie
     * where the value is identity-equal to the value corresponding to the given key will also return the same QPTrie.
     * This is permissible because the QPTries would otherwise be functionally equivalent.
     *
     * @param key
     * @param updater A function that takes the prior value for the given [key], or `null` if such a value was not
     *   present, and returns either a new value for the given key, or `null` if the entry should be removed.
     * @return A possibly new QPTrie; if [updater] returns a non-null value, it will be the value for the given [key] in
     *   the resulting QPTrie; if `updater` returns `null` then the entry for the given key will be removed from the
     *   resulting QPTrie.
     */
    fun update(key: ByteArray, updater: (prev: V?) -> V?): QPTrie<V> {
        return this.updateMaybeWithKeyCopy(key, updater, true)
    }

    /**
     * Returns a new [QPTrie], wherein the value corresponding to the given [key] is now [value]. If the current QPTrie
     * already has a corresponding value for the given key, this value is effectively replaced in the new QPTrie.
     *
     * This method is implemented recursively, so care should be taken to ensure that the keys present in the QPTrie
     * are short enough, or sparse enough, to prevent [StackOverflowError]s.
     *
     * The new QPTrie will actually be the given QPTrie if the given value is identity-equal to the value already
     * present in the given QPTrie corresponding to the given key. This is permissible because the QPTries would
     * otherwise be functionally equivalent.
     */
    fun put(key: ByteArray, value: V): QPTrie<V> {
        return this.update(key) { value }
    }

    /**
     * Returns a new [QPTrie], wherein the value corresponding to the given [key] is now [value]. If the current QPTrie
     * already has a corresponding value for the given key, this value is effectively replaced in the new QPTrie.
     *
     * The `key` will be stored directly inside this QPTrie for future usage. This does elide a potentially expensive
     * allocation and copy operation, but callers must not modify the `key` array after this method has been called.
     * If the `key` may be modified later, callers should use [QPTrie.put] instead, which copies the `key` for internal
     * usage.
     *
     * This method is implemented recursively, so care should be taken to ensure that the keys present in the QPTrie
     * are short enough, or sparse enough, to prevent [StackOverflowError]s.
     *
     * The new QPTrie will actually be the given QPTrie if the given value is identity-equal to the value already
     * present in the given QPTrie corresponding to the given key. This is permissible because the QPTries would
     * otherwise be functionally equivalent.
     */
    fun putUnsafeSharedKey(key: ByteArray, value: V): QPTrie<V> {
        return this.updateMaybeWithKeyCopy(key, { value }, false)
    }

    /**
     * Returns a new [QPTrie], wherein there is no value corresponding to the given [key].
     *
     * This method is implemented recursively, so care should be taken to ensure that the keys present in the QPTrie
     * are short enough, or sparse enough, to prevent [StackOverflowError]s.
     *
     * The new QPTrie will actually be the given QPTrie if there was not already a value corresponding to the given
     * key. This is permissible because the QPTries would otherwise be functionally equivalent.
     */
    fun remove(key: ByteArray): QPTrie<V> {
        return this.update(key) { null }
    }

    override fun iterator(): Iterator<QPTrieKeyValue<V>> {
        return mapSequence(this.iteratorUnsafeSharedKey()) {
            it.withKeyCopy()
        }
    }

    /**
     * Returns an iterator over the key/value pairs present in this [QPTrie].
     *
     * The `key` member of the resulting pairs are internally used by the QPTrie and must not be modified. Sharing this
     * key in such an unsafe way reduces the number of memory allocations, and consequently CPU time and GC pressure,
     * which is why such an unsafe method is exposed.
     */
    fun iteratorUnsafeSharedKey(): Iterator<QPTrieKeyValue<V>> {
        return this.iteratorAscendingUnsafeSharedKey()
    }

    /**
     * Calls [receiver] for every key/value pair present in this [QPTrie].
     *
     * This method of obtaining key/value pairs is significantly faster than using the equivalent
     *  [iteratorUnsafeSharedKey] mechanism, but is implemented recursively, so care should be taken
     * to ensure that the keys present in the QPTrie are short enough, or sparse enough, to prevent
     * [StackOverflowError]s.
     *
     * The `key` member of the resulting pairs are internally used by the QPTrie and must not be modified. Sharing this
     * key in such an unsafe way reduces the number of memory allocations, and consequently CPU time and GC pressure,
     * which is why such an unsafe method is exposed.
     */
    fun visitUnsafeSharedKey(receiver: VisitReceiver<V>) {
        return this.visitAscendingUnsafeSharedKey(receiver)
    }

    /**
     * Returns an iterator over the key/value pairs present in this [QPTrie], where the results are ordered by the
     * internal keys sorted in ascending order.
     *
     * The `key` member of the resulting pairs are internally used by the QPTrie and must not be modified. Sharing this
     * key in such an unsafe way reduces the number of memory allocations, and consequently CPU time and GC pressure,
     * which is why such an unsafe method is exposed.
     */
    fun iteratorAscendingUnsafeSharedKey(): Iterator<QPTrieKeyValue<V>> {
        val root = this.root ?: return EmptyIterator()
        return root .fullIteratorAscending()
    }

    /**
     * Calls [receiver] for every key/value pair present in this [QPTrie], in ascending key order.
     *
     * This method of obtaining key/value pairs is significantly faster than using the equivalent
     * [iteratorAscendingUnsafeSharedKey] mechanism, but is implemented recursively, so care should be taken
     * to ensure that the keys present in the QPTrie are short enough, or sparse enough, to prevent
     * [StackOverflowError]s.
     *
     * The `key` member of the resulting pairs are internally used by the QPTrie and must not be modified. Sharing this
     * key in such an unsafe way reduces the number of memory allocations, and consequently CPU time and GC pressure,
     * which is why such an unsafe method is exposed.
     */
    fun visitAscendingUnsafeSharedKey(receiver: VisitReceiver<V>) {
        val root = this.root ?: return
        return visitAscendingUnsafeSharedKeyImpl(root, receiver)
    }

    /**
     * Returns an iterator over the key/value pairs present in this [QPTrie], where the results are ordered by the
     * internal keys sorted in descending order.
     *
     * The `key` member of the resulting pairs are internally used by the QPTrie and must not be modified. Sharing this
     * key in such an unsafe way reduces the number of memory allocations, and consequently CPU time and GC pressure,
     * which is why such an unsafe method is exposed.
     */
    fun iteratorDescendingUnsafeSharedKey(): Iterator<QPTrieKeyValue<V>> {
        val root = this.root ?: return EmptyIterator()
        return root.fullIteratorDescending()
    }

    /**
     * Calls [receiver] for every key/value pair present in this [QPTrie], in descending key order.
     *
     * This method of obtaining key/value pairs is significantly faster than using the equivalent
     * [iteratorDescendingUnsafeSharedKey] mechanism, but is implemented recursively, so care should be taken
     * to ensure that the keys present in the QPTrie are short enough, or sparse enough, to prevent
     * [StackOverflowError]s.
     *
     * The `key` member of the resulting pairs are internally used by the QPTrie and must not be modified. Sharing this
     * key in such an unsafe way reduces the number of memory allocations, and consequently CPU time and GC pressure,
     * which is why such an unsafe method is exposed.
     */
    fun visitDescendingUnsafeSharedKey(receiver: VisitReceiver<V>) {
        val root = this.root ?: return
        return visitDescendingUnsafeSharedKeyImpl(root, receiver)
    }

    /**
     * Returns an iterator over the key/value pairs present in this [QPTrie], where the results have keys less than or
     * equal to the given [key], and are ordered by the internal keys sorted in descending order.
     *
     * The `key` member of the resulting pairs are internally used by the QPTrie and must not be modified. Sharing this
     * key in such an unsafe way reduces the number of memory allocations, and consequently CPU time and GC pressure,
     * which is why such an unsafe method is exposed.
     */
    fun iteratorLessThanOrEqualUnsafeSharedKey(key: ByteArray): Iterator<QPTrieKeyValue<V>> {
        val root = this.root ?: return EmptyIterator()
        return root.iteratorForLessThanOrEqual(key, 0)
    }

    /**
     * Calls [receiver] for every key/value pair present in this [QPTrie] which is less than or equal to the given
     * [key], in descending key order.
     *
     * This method of obtaining key/value pairs is significantly faster than using the equivalent
     * [iteratorLessThanOrEqualUnsafeSharedKey] mechanism, but is implemented recursively, so care should be taken
     * to ensure that the keys present in the QPTrie are short enough, or sparse enough, to prevent
     * [StackOverflowError]s.
     *
     * The `key` member of the resulting pairs are internally used by the QPTrie and must not be modified. Sharing this
     * key in such an unsafe way reduces the number of memory allocations, and consequently CPU time and GC pressure,
     * which is why such an unsafe method is exposed.
     */
    fun visitLessThanOrEqualUnsafeSharedKey(key: ByteArray, receiver: VisitReceiver<V>) {
        val root = this.root ?: return
        visitLessThanOrEqualUnsafeSharedKeyImpl(root, key, root.prefixSize, receiver)
    }

    /**
     * Returns an iterator over the key/value pairs present in this [QPTrie], where the results have keys greater than
     * or equal to the given [key], and are ordered by the internal keys sorted in ascending order.
     *
     * The `key` member of the resulting pairs are internally used by the QPTrie and must not be modified. Sharing this
     * key in such an unsafe way reduces the number of memory allocations, and consequently CPU time and GC pressure,
     * which is why such an unsafe method is exposed.
     */
    fun iteratorGreaterThanOrEqualUnsafeSharedKey(key: ByteArray): Iterator<QPTrieKeyValue<V>> {
        val root = this.root ?: return EmptyIterator()
        return root.iteratorForGreaterThanOrEqual(key, 0)
    }

    /**
     * Calls [receiver] for every key/value pair present in this [QPTrie] which is greater than or equal to the given
     * [key], in ascending key order.
     *
     * This method of obtaining key/value pairs is significantly faster than using the equivalent
     * [iteratorGreaterThanOrEqualUnsafeSharedKey] mechanism, but is implemented recursively, so care should be taken
     * to ensure that the keys present in the QPTrie are short enough, or sparse enough, to prevent
     * [StackOverflowError]s.
     *
     * The `key` member of the resulting pairs are internally used by the QPTrie and must not be modified. Sharing this
     * key in such an unsafe way reduces the number of memory allocations, and consequently CPU time and GC pressure,
     * which is why such an unsafe method is exposed.
     */
    fun visitGreaterThanOrEqualUnsafeSharedKey(key: ByteArray, receiver: VisitReceiver<V>) {
        val root = this.root ?: return
        visitGreaterThanOrEqualUnsafeSharedKeyImpl(root, key, root.prefixSize, receiver)
    }

    /**
     * Returns an iterator over the key/value pairs present in this [QPTrie], where the results have keys which start
     * with, or are equal to, the given [key], and are ordered by the internal keys sorted in ascending order.
     *
     * The `key` member of the resulting pairs are internally used by the QPTrie and must not be modified. Sharing this
     * key in such an unsafe way reduces the number of memory allocations, and consequently CPU time and GC pressure,
     * which is why such an unsafe method is exposed.
     */
    fun iteratorStartsWithUnsafeSharedKey(key: ByteArray): Iterator<QPTrieKeyValue<V>> {
        val root = this.root ?: return EmptyIterator()
        return root.iteratorForStartsWith(key, 0)
    }

    /**
     * Calls [receiver] for every key/value pair present in this [QPTrie] which starts with, or is equal to, the given
     * [key].
     *
     * This method of obtaining key/value pairs is significantly faster than using the equivalent
     * [iteratorStartsWithUnsafeSharedKey] mechanism.
     *
     * The `key` member of the resulting pairs are internally used by the QPTrie and must not be modified. Sharing this
     * key in such an unsafe way reduces the number of memory allocations, and consequently CPU time and GC pressure,
     * which is why such an unsafe method is exposed.
     */
    fun visitStartsWithUnsafeSharedKey(key: ByteArray, receiver: VisitReceiver<V>) {
        val root = this.root ?: return
        return visitStartsWithUnsafeSharedKeyImpl(root, key, root.prefixSize, receiver)
    }

    /**
     * Returns an iterator over the key/value pairs present in this [QPTrie], where the results have keys which are
     * prefixes of, or are equal to, the given [key], and are ordered by the internal keys sorted in ascending order.
     *
     * The `key` member of the resulting pairs are internally used by the QPTrie and must not be modified. Sharing this
     * key in such an unsafe way reduces the number of memory allocations, and consequently CPU time and GC pressure,
     * which is why such an unsafe method is exposed.
     */
    fun iteratorPrefixOfOrEqualToUnsafeSharedKey(key: ByteArray): Iterator<QPTrieKeyValue<V>> {
        val root = this.root ?: return EmptyIterator()
        return LookupPrefixOfOrEqualToIterator(key, root)
    }

    /**
     * Calls [receiver] for every key/value pair present in this [QPTrie] whose key is a prefix of, or equal to,
     * the given [key].
     *
     * This method of obtaining key/value pairs is significantly faster than using the equivalent
     * [iteratorPrefixOfOrEqualToUnsafeSharedKey] mechanism.
     *
     * The `key` member of the resulting pairs are internally used by the QPTrie and must not be modified. Sharing this
     * key in such an unsafe way reduces the number of memory allocations, and consequently CPU time and GC pressure,
     * which is why such an unsafe method is exposed.
     */
    fun visitPrefixOfOrEqualToUnsafeSharedKey(key: ByteArray, receiver: VisitReceiver<V>) {
        val root = this.root ?: return
        return visitPrefixOfOrEqualToUnsafeSharedKeyImpl(root, key, root.prefixSize, receiver)
    }

    /**
     * Returns the key/value pair with the minimum key if any key/value pairs are present in this [QPTrie], or `null`
     * if no key/value pairs are present in this QPTrie.
     *
     * The `key` member of the resulting pair is internally used by the QPTrie and must not be modified. Sharing this
     * key in such an unsafe way reduces the number of memory allocations, and consequently CPU time and GC pressure,
     * which is why such an unsafe method is exposed.
     */
    fun minKeyValueUnsafeSharedKey(): QPTrieKeyValue<V>? {
        val root = this.root ?: return null
        return minKeyValue(root)
    }

    /**
     * Copies references to the keys present in this [QPTrie] into [result].
     *
     * The byte arrays referred to in [result] are internally used by the QPTrie and must not be modified. Sharing these
     * byte arrays in such an unsafe way reduces the number of memory allocations, and consequently CPU time and GC
     * pressure, which is why such an unsafe method is exposed.
     */
    fun keysIntoUnsafeSharedKey(result: ArrayList<ByteArray>): ArrayList<ByteArray> {
        result.ensureCapacity(this.size.toInt())
        val root = this.root ?: return result
        keysInto(root, result)
        return result
    }
}

private class LookupPrefixOfOrEqualToIterator<V>(
    private val compareTo: ByteArray,
    private var currentNode: OddNybble<V>?
) : Iterator<QPTrieKeyValue<V>> {
    private var currentValue: QPTrieKeyValue<V>? = null
    private var compareOffset = 0
    private var lastTarget: Byte? = null

    private fun skipCurrentNodeToValue() {
        while (this.currentValue == null && this.currentNode != null) {
            val currentNode = this.currentNode!!
            val comparison = currentNode.compareLookupSliceToCurrentPrefix(this.compareTo, compareOffset)
            if (comparison != 0) {
                this.currentNode = null
                break
            }
            this.compareOffset += currentNode.prefixSize
            if (currentNode.valuePair != null) {
                this.currentValue = currentNode.valuePair
            }
            if (this.compareOffset >= this.compareTo.size) {
                this.currentNode = null
                break
            }
            val target = this.compareTo[this.compareOffset]
            this.compareOffset += 1
            this.lastTarget = target
            this.currentNode = currentNode.dispatchByte(target)?.dispatchByte(target)
        }
    }

    override fun hasNext(): Boolean {
        this.skipCurrentNodeToValue()
        return this.currentValue != null
    }

    override fun next(): QPTrieKeyValue<V> {
        this.skipCurrentNodeToValue()
        val value = this.currentValue ?: throw NoSuchElementException()
        this.currentValue = null
        return value
    }
}

private fun <V> oddNybbleFromIterator(items: Iterable<Pair<ByteArray, V>>): OddNybble<V>? {
    val it = items.iterator()
    var root: OddNybble<V>
    if (it.hasNext()) {
        val (key, value) = it.next()
        val keyCopy = key.copyOf()
        root = OddNybble(
            keyCopy,
            0,
            keyCopy.size,
            QPTrieKeyValue(keyCopy, value),
            1,
            emptyByteArray,
            arrayOf()
        )
    } else {
        return null
    }
    for (item in it) {
        val (key, value) = item
        root = (root.update(key, 0, true) { value })!!
    }
    return root
}