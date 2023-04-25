package name.djsweet.query.tree

import kotlinx.collections.immutable.PersistentList
import kotlinx.collections.immutable.persistentListOf
import java.util.Arrays

// Here's hoping either the Kotlin compiler or the JVM running this
// optimizes out these casts...
internal fun evenNybbleFromByte(b: Byte): Byte {
    return b.toInt().shr(4).toByte()
}

internal fun oddNybbleFromByte(b: Byte): Byte {
    return b.toInt().and(0xf).toByte()
}

internal fun evenNybbleToInt(nybble: Byte): Int {
    return nybble.toInt().shl(4)
}

internal fun nybblesToBytes(highNybble: Byte, lowNybble: Byte): Byte {
    return highNybble.toInt().shl(4).or(lowNybble.toInt()).toByte()
}

internal fun nybblesToBytesPreShifted(highNybble: Int, lowNybble: Byte): Byte {
    return highNybble.or(lowNybble.toInt()).toByte()
}

data class QPTrieKeyValue<V> internal constructor(
    val key: ByteArray,
    val value: V
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as QPTrieKeyValue<*>

        if (!key.contentEquals(other.key)) return false
        return value == other.value
    }

    override fun hashCode(): Int {
        var result = key.contentHashCode()
        result = 31 * result + (value?.hashCode() ?: 0)
        return result
    }
}

internal typealias RegisterChildIterator<V> = (it: ConcatenatedIterator<QPTrieKeyValue<V>>) -> ConcatenatedIterator<QPTrieKeyValue<V>>

internal class OddNybble<V>(
    val prefix: ByteArray,
    val value: V?,
    val size: Long,
    val nybbleValues: ByteArray?,
    val nybbleDispatch: Array<EvenNybble<V>>?,
) {
    fun dispatchByte(target: Byte): EvenNybble<V>? {
        if (this.nybbleDispatch == null) {
            return null
        }
        val offset = QPTrieUtils.offsetForNybble(this.nybbleValues, evenNybbleFromByte(target))
        return if (offset < 0) { null } else { this.nybbleDispatch[offset] }
    }

    fun compareLookupSliceToCurrentPrefix(compareTo: ByteArray, compareOffset: Int): Int {
        return Arrays.compareUnsigned(
            this.prefix,
            0,
            this.prefix.size,
            compareTo,
            compareOffset,
            (compareOffset + this.prefix.size).coerceAtMost(compareTo.size)
        )
    }

    fun get(key: ByteArray, offset: Int): V? {
        val compare = this.compareLookupSliceToCurrentPrefix(key, offset)
        val endOffset = offset + this.prefix.size
        if (compare != 0) {
            return null
        }
        if (key.size == endOffset) {
            return this.value
        }
        val target = key[endOffset]
        val child = this.dispatchByte(target)?.dispatchByte(target)
        return if (child == null || endOffset + 1 > key.size) {
            null
        } else {
            child.get(key, endOffset + 1)
        }
    }

    private fun updateEvenOdd(
        key: ByteArray,
        keyOffset: Int,
        updater: (prev: V?) -> V?,
        childTarget: Byte,
        evenNode: EvenNybble<V>,
        evenOffset: Int,
        oddNode: OddNybble<V>,
        oddOffset: Int
    ): OddNybble<V>? {
        val nybbleValues = this.nybbleValues!!
        val nybbleDispatch = this.nybbleDispatch!!

        val nextOddNode = oddNode.update(key, keyOffset + 1, updater)
        // No change, just return ourselves
        if (nextOddNode === oddNode) {
            return this
        }
        // We're now down one node, and have to perform a removal.
        if (nextOddNode == null) {
            if (this.size == 1L && this.value == null) {
                // That odd node we just removed was our only node, and we didn't have any
                // values ourselves, so we can remove ourselves as well.
                return null
            }
            if (this.size == 2L && this.value == null) {
                // We can promote the only remaining child to be the resulting
                // node, as there's no reason for this node to exist anymore.
                val returnedValue: OddNybble<V>
                val highNybble: Byte
                val lowNybble: Byte
                if (nybbleDispatch.size == 1) {
                    highNybble = evenNybbleFromByte(childTarget)
                    if (evenNode.nybbleDispatch[0] === oddNode) {
                        lowNybble = evenNode.nybbleValues[1]
                        returnedValue = evenNode.nybbleDispatch[1]
                    } else {
                        lowNybble = evenNode.nybbleValues[0]
                        returnedValue = evenNode.nybbleDispatch[0]
                    }
                } else if (nybbleDispatch[0] === evenNode) {
                    highNybble = nybbleValues[1]
                    val remainingEvenNybble = nybbleDispatch[1]
                    lowNybble = remainingEvenNybble.nybbleValues[0]
                    returnedValue = remainingEvenNybble.nybbleDispatch[0]
                } else {
                    highNybble = nybbleValues[0]
                    val remainingEvenNybble = nybbleDispatch[0]
                    lowNybble = remainingEvenNybble.nybbleValues[0]
                    returnedValue = remainingEvenNybble.nybbleDispatch[0]
                }
                // The prefix of the resulting node will be the concatenation of:
                // 1. Our prefix
                // 2. The dispatching byte
                // 3. Their prefix
                return OddNybble(
                    concatByteArraysWithMiddleByte(
                        this.prefix,
                        nybblesToBytes(highNybble, lowNybble),
                        returnedValue.prefix
                    ),
                    returnedValue.value,
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
            if (nextEvenNode == null) {
                val nextEvenValues = if (nybbleValues.size == 1) {
                    null
                } else {
                    removeByteArray(nybbleValues, evenOffset)
                }
                val nextEvenDispatch = if (nybbleDispatch.size == 1) {
                    null
                } else {
                    val next = Array(nybbleDispatch.size - 1) { evenNode }
                    removeArray(nybbleDispatch, next, evenOffset)
                    next
                }
                return OddNybble(
                    this.prefix,
                    this.value,
                    this.size - 1,
                    nextEvenValues,
                    nextEvenDispatch
                )
            } else {
                val nextEvenDispatch = nybbleDispatch.clone()
                nextEvenDispatch[evenOffset] = nextEvenNode
                return OddNybble(
                    this.prefix,
                    this.value,
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
            return OddNybble(
                this.prefix,
                this.value,
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
        val prefixSize = this.prefix.size
        // There's not enough bytes in the array to compare as equal.
        // This might indicate that we need to introduce a new node.
        val equalUpTo = byteArraysEqualUpTo(this.prefix, key, offset, prefixSize)
        if (equalUpTo < prefixSize) {
            return -(equalUpTo + 2) // equalUpTo == 0 -> -2, 1 -> -3, etc
        }
        // If the prefix is equal to the remainder of the key, we are going to be performing access at this object.
        return if (remainder == prefixSize) { -1 } else { offset + prefixSize }
    }

    fun update(key: ByteArray, offset: Int, updater: (prev: V?) -> V?): OddNybble<V>? {
        val keyOffset = this.maybeKeyOffsetForAccess(key, offset)
        // We're updating ourselves here.
        if (keyOffset == -1) {
            val result = updater(this.value)
            if (result === this.value) {
                return this
            }
            val nextSize = if (this.value == null && result == null) {
                this.size
            } else if (this.value != null && result != null) {
                this.size
            } else if (this.value == null) { // result != null
                this.size + 1
            } else { // this.value != null, result == null
                this.size - 1
            }
            if (nextSize < this.size) {
                if (this.nybbleDispatch.isNullOrEmpty() || this.nybbleValues == null) {
                    // We've just removed the only reason this node exists,
                    // so we can remove the node itself.
                    return null
                }
                if (this.nybbleDispatch.size == 1) {
                    // There's not be a reason to keep this node around if there's exactly one child.
                    val evenNode = this.nybbleDispatch[0]
                    if (evenNode.nybbleDispatch.size == 1) {
                        val oddNode = evenNode.nybbleDispatch[0]
                        val childTarget = nybblesToBytes(this.nybbleValues[0],  evenNode.nybbleValues[0])
                        return OddNybble(
                            concatByteArraysWithMiddleByte(this.prefix, childTarget, oddNode.prefix),
                            oddNode.value,
                            oddNode.size,
                            oddNode.nybbleValues,
                            oddNode.nybbleDispatch
                        )
                    }
                }
            }
            return OddNybble(
                this.prefix,
                result,
                nextSize,
                this.nybbleValues,
                this.nybbleDispatch
            )
        }
        // We didn't match, but the key is shorter than our prefix supports.
        // If this is an addition we'll need to introduce a new node.
        if (keyOffset < -1) {
            val result = updater(null)
                ?: // null -> null means nothing to update.
                return this
            val topOffset = -(keyOffset + 2)
            val startOffset = offset + topOffset
            val incumbentNode = OddNybble(
                // Note that target is already a dispatched byte, so we skip the very
                // first prefix byte.
                this.prefix.copyOfRange(topOffset + 1, this.prefix.size),
                this.value,
                this.size,
                this.nybbleValues,
                this.nybbleDispatch
            )
            // If the startOffset is just beyond the actual key length, we have to insert a node "above" us.
            if (startOffset >= key.size) {
                val target = this.prefix[topOffset]
                val evenNode = EvenNybble(
                    byteArrayOf(oddNybbleFromByte(target)),
                    arrayOf(incumbentNode)
                )
                return OddNybble(
                    key.copyOfRange(offset, startOffset),
                    result,
                    this.size + 1,
                    byteArrayOf(evenNybbleFromByte(target)),
                    arrayOf(evenNode)
                )
            } else {
                val target = key[startOffset]
                // We now need an intermediate node that dispatches to both new nodes.
                val priorByte = this.prefix[topOffset]
                val newNode = OddNybble<V>(
                    key.copyOfRange(startOffset + 1, key.size),
                    result,
                    1,
                    null,
                    null
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
                return OddNybble(
                    key.copyOfRange(offset, startOffset),
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
            evenNode = this.nybbleDispatch!![evenOffset]
            val oddOffset = QPTrieUtils.offsetForNybble(evenNode.nybbleValues, oddNybbleFromByte(childTarget))
            if (oddOffset > -1) {
                val oddNode = evenNode.nybbleDispatch[oddOffset]
                return this.updateEvenOdd(key, keyOffset, updater, childTarget, evenNode, evenOffset, oddNode, oddOffset)
            }
        }

        // This would be a new value, even though we already have data to work with.
        // Note that by the time we're here, we're always inserting a new node.
        val result = updater(null) ?: return this

        val bottomNode = OddNybble<V>(
            key.copyOfRange(keyOffset + 1, key.size),
            result,
            1,
            null,
            null
        )
        val newEvenNode: EvenNybble<V> = if (evenNode == null) {
            EvenNybble(
                byteArrayOf(oddNybbleFromByte(childTarget)),
                arrayOf(bottomNode)
            )
        } else {
            val targetLowNybble = oddNybbleFromByte(childTarget)
            val foundOffset = findByteInSortedArray(evenNode.nybbleValues, targetLowNybble)
            if (foundOffset < 0) {
                val oddOffset = -(foundOffset + 1)
                val nextValues = insertByteArray(evenNode.nybbleValues, oddOffset, targetLowNybble)
                val nextDispatch = Array(evenNode.nybbleDispatch.size + 1) { bottomNode }
                insertArray(evenNode.nybbleDispatch, nextDispatch, oddOffset, bottomNode)
                EvenNybble(
                    nextValues,
                    nextDispatch
                )
            } else {
                val nextDispatch = evenNode.nybbleDispatch.clone()
                nextDispatch[foundOffset] = bottomNode
                EvenNybble(
                    evenNode.nybbleValues,
                    nextDispatch
                )
            }
        }
        return if (this.nybbleValues == null || this.nybbleDispatch == null) {
            OddNybble(
                this.prefix,
                this.value,
                this.size + 1,
                byteArrayOf(evenNybbleFromByte(childTarget)),
                arrayOf(newEvenNode)
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
                    this.prefix,
                    this.value,
                    this.size + 1,
                    newNybbleValues,
                    newNybbleDispatch
                )
            } else {
                val newNybbleDispatch = this.nybbleDispatch.clone()
                newNybbleDispatch[foundOffset] = newEvenNode
                return OddNybble(
                    this.prefix,
                    this.value,
                    this.size + 1,
                    this.nybbleValues,
                    newNybbleDispatch
                )
            }
        }
    }

    fun fullIteratorAscending(
        precedingPrefixes: PersistentList<ByteArray>,
        registerIteratorAsChild: RegisterChildIterator<V>?
    ): Iterator<QPTrieKeyValue<V>> {
        val it = FullAscendingOddNybbleIterator(this, precedingPrefixes.add(0, this.prefix))
        return if (registerIteratorAsChild != null) {
            registerIteratorAsChild(it)
        } else {
            it
        }
    }

    fun fullIteratorDescending(
        precedingPrefixes: PersistentList<ByteArray>,
        registerIteratorAsChild: RegisterChildIterator<V>?
    ): Iterator<QPTrieKeyValue<V>> {
        val it = FullDescendingOddNybbleIterator(this, precedingPrefixes.add(0, this.prefix))
        return if (registerIteratorAsChild != null) {
            registerIteratorAsChild(it)
        } else {
            it
        }
    }

    fun iteratorForLessThanOrEqual(
        precedingPrefixes: PersistentList<ByteArray>,
        compareTo: ByteArray,
        compareOffset: Int,
        registerIteratorAsChild: RegisterChildIterator<V>?
    ): Iterator<QPTrieKeyValue<V>> {
        val comparison = this.compareLookupSliceToCurrentPrefix(compareTo, compareOffset)
        val endCompareOffset = compareOffset + this.prefix.size
        return if (comparison < 0) {
            // Our prefix was fully less than the comparison slice, so all members are less than the
            // comparison, and we can just iterate descending.
            this.fullIteratorDescending(precedingPrefixes, registerIteratorAsChild)
        } else if (comparison > 0) {
            // Our prefix was fully greater than the comparison slice, and all of our members
            // are greater than us and thus greater than the comparison slice, so we're not
            // returning anything.
            EmptyIterator()
        } else if (this.nybbleValues == null || this.nybbleDispatch == null || compareTo.size <= endCompareOffset) {
            // We didn't have anything to dispatch, so we can possibly just return ourselves.
            // OR The compared value was fully identified by our prefix, so all of our members
            // are greater and should be skipped.
            if (this.value !== null) {
                SingleElementIterator(
                    QPTrieKeyValue(
                        concatByteArraysFromReverseList(precedingPrefixes.add(0, this.prefix)),
                        this.value
                    )
                )
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

            val it = LessThanOrEqualOddNybbleIterator(
                this,
                precedingPrefixes.add(0, this.prefix),
                compareTo,
                compareOffset + this.prefix.size,
                greaterNybbleOffset,
                equalNybbleOffset
            )
            if (registerIteratorAsChild != null) {
                registerIteratorAsChild(it)
            } else {
                it
            }
        }
    }

    fun iteratorForGreaterThanOrEqual(
        precedingPrefixes: PersistentList<ByteArray>,
        compareTo: ByteArray,
        compareOffset: Int,
        registerIteratorAsChild: RegisterChildIterator<V>?
    ): Iterator<QPTrieKeyValue<V>> {
        val comparison = this.compareLookupSliceToCurrentPrefix(compareTo, compareOffset)
        val endCompareOffset = compareOffset + this.prefix.size
        return if (comparison < 0) {
            // If our prefix was fully less than the inspected slice, all of our members will
            // also be less than the full comparison
            EmptyIterator()
        } else if (comparison > 0 || endCompareOffset >= compareTo.size) {
            // If our prefix was fully greater than the inspected slice, all of our members will
            // also be greater than the full comparison
            // OR the lookup key was fully equal to all the prefixes up until this node's point,
            // and all the children will necessarily be greater than the lookup key.
            this.fullIteratorAscending(precedingPrefixes, registerIteratorAsChild)
        } else if (this.nybbleValues == null || this.nybbleDispatch == null) {
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
            val it = GreaterThanOrEqualOddNybbleIterator(
                this,
                precedingPrefixes.add(0, this.prefix),
                compareTo,
                compareOffset + this.prefix.size,
                greaterEqualOffset,
                equalOffset
            )
            return if (registerIteratorAsChild != null) {
                registerIteratorAsChild(it)
            } else {
                it
            }
        }
    }

    fun iteratorForStartsWith(
        precedingPrefixes: PersistentList<ByteArray>,
        compareTo: ByteArray,
        compareOffset: Int,
        registerIteratorAsChild: RegisterChildIterator<V>?
    ): Iterator<QPTrieKeyValue<V>> {
        val comparison = this.compareLookupSliceToCurrentPrefix(compareTo, compareOffset)
        val endCompareOffset = compareOffset + this.prefix.size
        return if (comparison >= 0 && endCompareOffset >= compareTo.size) {
            this.fullIteratorAscending(precedingPrefixes, registerIteratorAsChild)
        } else if (comparison != 0) {
            EmptyIterator()
        } else {
            val target = compareTo[endCompareOffset]
            val evenNode = this.dispatchByte(target) ?: return EmptyIterator()
            evenNode.iteratorForStartsWith(
                precedingPrefixes.add(0, this.prefix),
                target,
                compareTo,
                endCompareOffset,
                registerIteratorAsChild
            )
        }
    }
}

internal class FullAscendingOddNybbleIterator<V>(
    private val node: OddNybble<V>,
    private val precedingPrefixes: PersistentList<ByteArray>
): ConcatenatedIterator<QPTrieKeyValue<V>>() {
    override fun iteratorForOffset(offset: Int): Iterator<QPTrieKeyValue<V>>? {
        val value = this.node.value
        var evenOffset = offset
        if (value != null) {
            if (offset == 0) {
                val fullPrefix = concatByteArraysFromReverseList(this.precedingPrefixes)
                return SingleElementIterator(QPTrieKeyValue(fullPrefix, value))
            }
            evenOffset -= 1
        }

        val dispatch = this.node.nybbleDispatch ?: return null
        val nybbleValues = this.node.nybbleValues
        return if (nybbleValues == null || dispatch.size <= evenOffset) {
             null
        } else {
            dispatch[evenOffset].fullIteratorAscending(
                this.precedingPrefixes,
                nybbleValues[evenOffset]
            ) {
                this.registerChild(it)
            }
        }
    }
}

internal class FullDescendingOddNybbleIterator<V>(
    private val node: OddNybble<V>,
    private val precedingPrefixes: PersistentList<ByteArray>
): ConcatenatedIterator<QPTrieKeyValue<V>>() {
    override fun iteratorForOffset(offset: Int): Iterator<QPTrieKeyValue<V>>? {
        val dispatch = this.node.nybbleDispatch
        val nybbleValues = this.node.nybbleValues
        var maxOffset = 0
        if (dispatch != null && nybbleValues != null) {
            maxOffset = dispatch.size
            if (offset < maxOffset) {
                val reverseOffset = maxOffset - offset - 1
                return dispatch[reverseOffset].fullIteratorDescending(
                    this.precedingPrefixes,
                    nybbleValues[reverseOffset]
                ) {
                    this.registerChild(it)
                }
            }
        }

        if (offset > maxOffset) {
            return null
        }
        val value = this.node.value ?: return null
        return SingleElementIterator(QPTrieKeyValue(concatByteArraysFromReverseList(this.precedingPrefixes), value))
    }
}

internal class LessThanOrEqualOddNybbleIterator<V>(
    private val node: OddNybble<V>,
    private val precedingPrefixes: PersistentList<ByteArray>,
    private val compareTo: ByteArray,
    private val compareOffset: Int,
    private val greaterNybbleOffset: Int,
    private val equalNybbleOffset: Int
): ConcatenatedIterator<QPTrieKeyValue<V>>() {
    override fun iteratorForOffset(offset: Int): Iterator<QPTrieKeyValue<V>>? {
        val reverseOffset = this.greaterNybbleOffset - offset - 1
        val nybbleValues = this.node.nybbleValues!!
        val nybbleDispatch = this.node.nybbleDispatch!!
        val value = this.node.value
        if (reverseOffset == this.equalNybbleOffset) {
            return nybbleDispatch[reverseOffset].iteratorForLessThanOrEqual(
                this.precedingPrefixes,
                nybbleValues[reverseOffset],
                this.compareTo,
                this.compareOffset,
            ) {
                this.registerChild(it)
            }
        }
        return if (reverseOffset >= 0) {
            nybbleDispatch[reverseOffset].fullIteratorDescending(this.precedingPrefixes, nybbleValues[reverseOffset]) {
                this.registerChild(it)
            }
        } else if (reverseOffset == -1 && value != null) {
            SingleElementIterator(QPTrieKeyValue(concatByteArraysFromReverseList(this.precedingPrefixes), value))
        } else {
            null
        }
    }
}

internal class GreaterThanOrEqualOddNybbleIterator<V>(
    private val node: OddNybble<V>,
    private val precedingPrefixes: PersistentList<ByteArray>,
    private val compareTo: ByteArray,
    private val compareOffset: Int,
    private val greaterOrEqualNybbleOffset: Int,
    private val equalNybbleOffset: Int
): ConcatenatedIterator<QPTrieKeyValue<V>>() {
    override fun iteratorForOffset(offset: Int): Iterator<QPTrieKeyValue<V>>? {
        var nodeOffset = offset
        val value = node.value
        if (compareOffset >= compareTo.size && value != null) {
            if (offset == 0) {
                return SingleElementIterator(
                    QPTrieKeyValue(
                        concatByteArraysFromReverseList(this.precedingPrefixes),
                        value
                    )
                )
            } else {
                nodeOffset -= 1
            }
        }
        nodeOffset += this.greaterOrEqualNybbleOffset
        val nybbleValues = this.node.nybbleValues!!
        val nybbleDispatch = this.node.nybbleDispatch!!

        return if (nodeOffset >= nybbleValues.size) {
            null
        } else if (nodeOffset == this.equalNybbleOffset) {
            nybbleDispatch[nodeOffset].iteratorForGreaterThanOrEqual(
                this.precedingPrefixes,
                nybbleValues[nodeOffset],
                this.compareTo,
                this.compareOffset
            ) {
                this.registerChild(it)
            }
        } else {
            nybbleDispatch[nodeOffset].fullIteratorAscending(this.precedingPrefixes, nybbleValues[nodeOffset]) {
                this.registerChild(it)
            }
        }
    }
}

internal class EvenNybble<V>(
    val nybbleValues: ByteArray,
    val nybbleDispatch: Array<OddNybble<V>>,
) {
    fun dispatchByte(target: Byte): OddNybble<V>? {
        val offset = QPTrieUtils.offsetForNybble(this.nybbleValues, oddNybbleFromByte(target))
        return if (offset < 0) { null } else { this.nybbleDispatch[offset] }
    }

    fun fullIteratorAscending(
        precedingPrefixes: PersistentList<ByteArray>,
        upperNybble: Byte,
        registerChildIterator: RegisterChildIterator<V>?
    ): Iterator<QPTrieKeyValue<V>> {
        val it = FullAscendingEvenIterator(this, precedingPrefixes, evenNybbleToInt(upperNybble))
        return if (registerChildIterator != null) {
            registerChildIterator(it)
        } else {
            it
        }
    }

    fun fullIteratorDescending(
        precedingPrefixes: PersistentList<ByteArray>,
        upperNybble: Byte,
        registerChildIterator: RegisterChildIterator<V>?
    ): Iterator<QPTrieKeyValue<V>> {
        val it = FullDescendingEvenIterator(this, precedingPrefixes, evenNybbleToInt(upperNybble))
        return if (registerChildIterator != null) {
            registerChildIterator(it)
        } else {
            it
        }
    }

    fun iteratorForLessThanOrEqual(
        precedingPrefixes: PersistentList<ByteArray>,
        upperNybble: Byte,
        compareTo: ByteArray,
        compareByteOffset: Int,
        registerChildIterator: RegisterChildIterator<V>?
    ): Iterator<QPTrieKeyValue<V>> {
        val it = LessThanOrEqualEvenNybbleIterator(
            this,
            precedingPrefixes,
            evenNybbleToInt(upperNybble),
            oddNybbleFromByte(compareTo[compareByteOffset]),
            compareTo,
            compareByteOffset + 1
        )
        return if (registerChildIterator != null) {
            registerChildIterator(it)
        } else {
            it
        }
    }

    fun iteratorForGreaterThanOrEqual(
        precedingPrefixes: PersistentList<ByteArray>,
        upperNybble: Byte,
        compareTo: ByteArray,
        compareByteOffset: Int,
        registerChildIterator: RegisterChildIterator<V>?
    ): Iterator<QPTrieKeyValue<V>> {
        if (compareByteOffset >= compareTo.size) {
            // We are necessarily greater than compareTo if the comparison byte offset
            // is beyond the size of the actual lookup key.
            return this.fullIteratorAscending(precedingPrefixes, upperNybble, registerChildIterator)
        }
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

        val it = GreaterThanOrEqualToEvenNybbleIterator(
            this,
            precedingPrefixes,
            evenNybbleToInt(upperNybble),
            compareTo,
            compareByteOffset + 1,
            greaterOrEqualNybbleOffset,
            equalNybbleOffset
        )
        return if (registerChildIterator != null) {
            registerChildIterator(it)
        } else {
            it
        }
    }

    fun iteratorForStartsWith(
        precedingPrefixes: PersistentList<ByteArray>,
        target: Byte,
        compareTo: ByteArray,
        compareOffset: Int,
        registerChildIterator: RegisterChildIterator<V>?
    ): Iterator<QPTrieKeyValue<V>> {
        val oddNode = this.dispatchByte(target) ?: return EmptyIterator()
        return oddNode.iteratorForStartsWith(
            precedingPrefixes.add(0, byteArrayOf(target)),
            compareTo,
            compareOffset + 1,
            registerChildIterator
        )
    }
}

private class FullAscendingEvenIterator<V>(
    private val node: EvenNybble<V>,
    private val precedingPrefixes: PersistentList<ByteArray>,
    private val upperNybble: Int
) : ConcatenatedIterator<QPTrieKeyValue<V>>() {
    override fun iteratorForOffset(offset: Int): Iterator<QPTrieKeyValue<V>>? {
        val dispatch = this.node.nybbleDispatch
        val nybbleValues = this.node.nybbleValues
        if (dispatch.size <= offset) {
            return null
        }
        val nextBytes = byteArrayOf(nybblesToBytesPreShifted(this.upperNybble, nybbleValues[offset]))
        return dispatch[offset].fullIteratorAscending(
            this.precedingPrefixes.add(0, nextBytes)
        ) { this.registerChild(it) }
    }
}

private class FullDescendingEvenIterator<V>(
    private val node: EvenNybble<V>,
    private val precedingPrefixes: PersistentList<ByteArray>,
    private val upperNybble: Int
): ConcatenatedIterator<QPTrieKeyValue<V>>() {
    override fun iteratorForOffset(offset: Int): Iterator<QPTrieKeyValue<V>>? {
        val dispatch = this.node.nybbleDispatch
        val nybbleValues = this.node.nybbleValues
        if (dispatch.size <= offset) {
            return null
        }
        val reverseOffset = dispatch.size - offset - 1
        val nextBytes = byteArrayOf(nybblesToBytesPreShifted(this.upperNybble,nybbleValues[reverseOffset]))
        return dispatch[reverseOffset].fullIteratorDescending(this.precedingPrefixes.add(0, nextBytes)) {
            this.registerChild(it)
        }
    }
}

private class LessThanOrEqualEvenNybbleIterator<V>(
    private val node: EvenNybble<V>,
    private val precedingPrefixes: PersistentList<ByteArray>,
    private val upperNybble: Int,
    private val compareBottom: Byte,
    private val compareTo: ByteArray,
    private val nextCompareOffset: Int
) : ConcatenatedIterator<QPTrieKeyValue<V>>() {
    private val greaterThanOffset: Int

    init {
        var currentGreaterThanOffset = 0
        val nybbleValues = node.nybbleValues
        for (element in nybbleValues) {
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
        val node = this.node
        if (reverseOffset < 0) {
            return null
        }
        val nybbleValue = node.nybbleValues[reverseOffset]
        val nextPrecedingPrefixes = this.precedingPrefixes.add(
            0,
            byteArrayOf(nybblesToBytesPreShifted(this.upperNybble, nybbleValue))
        )
        return if (offset == 0 && compareBytesUnsigned(nybbleValue, this.compareBottom) >= 0) {
            // Note that even at the zero offset, if the nybble value is actually less than or equal to
            // the comparison, we can perform a full iteration.
            node.nybbleDispatch[reverseOffset].iteratorForLessThanOrEqual(
                nextPrecedingPrefixes,
                this.compareTo,
                this.nextCompareOffset
            ) {
                this.registerChild(it)
            }
        } else {
            node.nybbleDispatch[reverseOffset].fullIteratorDescending(nextPrecedingPrefixes) {
                this.registerChild(it)
            }
        }
    }
}

private class GreaterThanOrEqualToEvenNybbleIterator<V>(
    private val node: EvenNybble<V>,
    private val precedingPrefixes: PersistentList<ByteArray>,
    private val upperNybble: Int,
    private val compareTo: ByteArray,
    private val compareOffset: Int,
    private val greaterOrEqualNybbleOffset: Int,
    private val equalNybbleOffset: Int
): ConcatenatedIterator<QPTrieKeyValue<V>>() {
    override fun iteratorForOffset(offset: Int): Iterator<QPTrieKeyValue<V>>? {
        val nodeOffset = offset + this.greaterOrEqualNybbleOffset
        val nybbleValues = this.node.nybbleValues
        val nybbleDispatch = this.node.nybbleDispatch
        if (nodeOffset >= nybbleValues.size) {
            return null
        }
        val fullByte = nybblesToBytesPreShifted(this.upperNybble, nybbleValues[nodeOffset])
        val newPrecedingPrefixes = this.precedingPrefixes.add(0, byteArrayOf(fullByte))
        return if (nodeOffset == this.equalNybbleOffset) {
            nybbleDispatch[nodeOffset].iteratorForGreaterThanOrEqual(
                newPrecedingPrefixes,
                this.compareTo,
                this.compareOffset
            ) {
                this.registerChild(it)
            }
        } else {
            nybbleDispatch[nodeOffset].fullIteratorAscending(newPrecedingPrefixes) { this.registerChild(it) }
        }
    }
}

class QPTrie<V>: Iterable<QPTrieKeyValue<V>> {
    private val root: OddNybble<V>?
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
        this.root = sizeNodeFromIterable(items)
        this.size = this.root?.size ?: 0
    }

    fun get(key: ByteArray): V? {
        return this.root?.get(key, 0)
    }

    fun update(key: ByteArray, updater: (prev: V?) -> V?): QPTrie<V> {
        if (this.root == null) {
            val value = updater(null) ?: return this
            return QPTrie(OddNybble(key, value, 1, null, null))
        }
        val newRoot = this.root.update(key, 0, updater)
        if (newRoot === this.root) {
            return this
        }
        return QPTrie(newRoot)
    }

    fun put(key: ByteArray, value: V): QPTrie<V> {
        return this.update(key) { value }
    }

    fun remove(key: ByteArray): QPTrie<V> {
        return this.update(key) { null }
    }

    override fun iterator(): Iterator<QPTrieKeyValue<V>> {
        return this.iteratorAscending()
    }

    fun iteratorAscending(): Iterator<QPTrieKeyValue<V>> {
        return if (this.root == null) {
            EmptyIterator()
        } else {
            this.root.fullIteratorAscending(persistentListOf(), null)
        }
    }

    fun iteratorDescending(): Iterator<QPTrieKeyValue<V>> {
        return if (this.root == null) {
            EmptyIterator()
        } else {
            this.root.fullIteratorDescending(persistentListOf(), null)
        }
    }

    fun iteratorLessThanOrEqual(key: ByteArray): Iterator<QPTrieKeyValue<V>> {
        return if (this.root == null) {
            EmptyIterator()
        } else {
            this.root.iteratorForLessThanOrEqual(persistentListOf(), key, 0, null)
        }
    }

    fun iteratorGreaterThanOrEqual(key: ByteArray): Iterator<QPTrieKeyValue<V>> {
        return if (this.root == null) {
            EmptyIterator()
        } else {
            this.root.iteratorForGreaterThanOrEqual(persistentListOf(), key, 0, null)
        }
    }

    fun iteratorStartsWith(key: ByteArray): Iterator<QPTrieKeyValue<V>> {
        return if (this.root == null) {
            EmptyIterator()
        } else {
            this.root.iteratorForStartsWith(persistentListOf(), key, 0, null)
        }
    }

    fun iteratorPrefixOfOrEqualTo(key: ByteArray): Iterator<QPTrieKeyValue<V>> {
        return if (this.root == null) {
            EmptyIterator()
        } else {
            LookupPrefixOfOrEqualToIterator(key, this.root)
        }
    }
}

private class LookupPrefixOfOrEqualToIterator<V>(
    private val compareTo: ByteArray,
    private var currentNode: OddNybble<V>?
) : Iterator<QPTrieKeyValue<V>> {
    var currentValue: V? = null
    var reversePrefixList = persistentListOf<ByteArray>()
    var compareOffset = 0
    var lastTarget: Byte? = null

    private fun skipCurrentNodeToValue() {
        while (this.currentValue == null && this.currentNode != null) {
            val currentNode = this.currentNode!!
            val comparison = currentNode.compareLookupSliceToCurrentPrefix(this.compareTo, compareOffset)
            if (comparison != 0) {
                this.currentNode = null
                break
            }
            if (this.lastTarget != null) {
                this.reversePrefixList = this.reversePrefixList.add(0, byteArrayOf(this.lastTarget!!))
            }
            this.reversePrefixList = this.reversePrefixList.add(0, currentNode.prefix)
            this.compareOffset += currentNode.prefix.size
            if (currentNode.value != null) {
                this.currentValue = currentNode.value
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
        return QPTrieKeyValue(concatByteArraysFromReverseList(this.reversePrefixList), value)
    }
}

private fun <V> sizeNodeFromIterable(items: Iterable<Pair<ByteArray, V>>): OddNybble<V>? {
    val it = items.iterator()
    var root: OddNybble<V>
    if (it.hasNext()) {
        val (key, value) = it.next()
        root = OddNybble(key, value, 1, null, null)
    } else {
        return null
    }
    for (item in it) {
        val (key, value) = item
        root = (root.update(key, 0) { value })!!
    }
    return root
}