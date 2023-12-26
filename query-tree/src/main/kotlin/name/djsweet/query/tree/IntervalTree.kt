// SPDX-FileCopyrightText: 2023 Dani Sweet <thorium@djsweet.name>
//
// SPDX-License-Identifier: MIT

package name.djsweet.query.tree

import kotlin.NoSuchElementException

internal fun <T : Comparable<T>>compareRanges(leftLeft: T, leftRight: T, rightLeft: T, rightRight: T): Int {
    val leftCompare = leftLeft.compareTo(rightLeft)
    if (leftCompare != 0) {
        return leftCompare
    }
    return leftRight.compareTo(rightRight)
}

// Our ranges are closed on both ends, so first == second is perfectly acceptable.
internal fun <T: Comparable<T>>enforceRangeInvariants(range: Pair<T, T>): Pair<T, T> {
    val rangeCompare = range.first.compareTo(range.second)
    return if (rangeCompare > 0) {
        Pair(range.second, range.first)
    } else {
        range
    }
}

internal fun <T: Comparable<T>>rangeOverlaps(leftLow: T, leftHigh: T, rightLow: T, rightHigh: T): Boolean {
    val rangeLowNodeLow = leftLow.compareTo(rightLow)
    val rangeLowNodeHigh = leftLow.compareTo(rightHigh)
    val rangeHighNodeLow = leftHigh.compareTo(rightLow)
    val rangeHighNodeHigh = leftHigh.compareTo(rightHigh)

    // This low between other range
    var overlaps = rangeLowNodeLow >= 0 && rangeLowNodeHigh <= 0
    // This high between other range
    overlaps = overlaps || (rangeHighNodeLow >= 0 && rangeHighNodeHigh <= 0)
    // Other range low between this
    overlaps = overlaps || (rangeLowNodeLow <= 0 && rangeHighNodeLow >= 0)
    // Other range high between this
    overlaps = overlaps || (rangeHighNodeLow <= 0 && rangeHighNodeHigh >= 0)
    return overlaps
}

/**
 * Represents an ordered pair of two values, interpreted as a closed-interval range.
 *
 * This type is used extensively inside an [IntervalTree] to encode keys.
 */
data class IntervalRange<T: Comparable<T>> internal constructor(
    /**
     * One of the two values in an [IntervalRange], either equal to or less than [upperBound].
     */
    val lowerBound: T,
    /**
     * One of the two values in an [IntervalRange], either equal to or greater than [lowerBound].
     */
    val upperBound: T
): Comparable<IntervalRange<T>> {
    companion object {
        /**
         * Creates an [IntervalRange] from a [Pair].
         *
         * The values of the pair are automatically reordered in the resulting
         * IntervalRange such that [lowerBound] is always less than or equal to
         * [upperBound], which is to say that if [Pair.first] is greater than
         * [Pair.second], `lowerBound` will be set to `Pair.second` and `upperBound`
         * will be set to `Pair.first`.
         */
        fun <T: Comparable<T>> fromPair(p: Pair<T, T>): IntervalRange<T> {
            val (lowerBound, upperBound) = enforceRangeInvariants(p)
            return IntervalRange(lowerBound, upperBound)
        }

        /**
         * Creates an [IntervalRange] given potential bounds.
         *
         * The values of the given bounds are automatically reordered in the resulting
         * IntervalRange such that [lowerBound] is always less than or equal to
         * [upperBound], which is to say that if [maybeLower] is greater than [maybeUpper],
         * `lowerBound` will be set to `maybeUpper` and `upperBound` will be set to
         * `maybeLower`.
         */
        fun <T: Comparable<T>> fromBounds(maybeLower: T, maybeUpper: T): IntervalRange<T> {
            val rangeCompare = maybeLower.compareTo(maybeUpper)
            return if (rangeCompare > 0) {
                IntervalRange(maybeUpper, maybeLower)
            } else {
                IntervalRange(maybeLower, maybeUpper)
            }
        }
    }

    /**
     * Converts this [IntervalRange] to a [Pair].
     *
     * [Pair.first] is set to [lowerBound] and [Pair.second] is set to [upperBound].
     */
    fun toPair(): Pair<T, T> {
        return Pair(this.lowerBound, this.upperBound)
    }

    /**
     * Checks whether this [IntervalRange] overlaps with the [other] IntervalRange.
     *
     * Overlapping is closed-interval, meaning that
     * - If both bounds of this interval range are between or equal to either bound
     *   of the other IntervalRange, the ranges overlap.
     * - If both bounds of the other interval range are between or equal to either bound
     *   of this IntervalRange, the ranges overlap.
     * - If either bound of this IntervalRange is between or equal to either bound
     *   of the other IntervalRange, the ranges overlap.
     *
     * If none of these conditions hold, the ranges do not overlap.
     */
    fun overlaps(other: IntervalRange<T>): Boolean {
        return rangeOverlaps(this.lowerBound, this.upperBound, other.lowerBound, other.upperBound)
    }

    override fun compareTo(other: IntervalRange<T>): Int {
        return compareRanges(this.lowerBound, this.upperBound, other.lowerBound, other.upperBound)
    }
}

/**
 * Represents a key/value pair held by an [IntervalTree].
 */
data class IntervalTreeKeyValue<T: Comparable<T>, V> internal constructor(val key: IntervalRange<T>, val value: V)

/**
 * Represents a key/value pair held by an [IntervalTree], with an additional [balance] member.
 *
 * The semantics of [key] and [value] in this type are identical to those of [IntervalTreeKeyValue]. The [balance]
 * member indicates the relative "weight" of the node corresponding to this key/value pair in the IntervalTree.
 * A balance of 0 indicates that both the left and right children of the corresponding node have the same weight,
 * a balance less than 0 indicates that the left child of the corresponding node has a greater weight than the
 * right child, and a balance greater than 0 indicates that the right child of the corresponding node has a balance
 * greater than the left child.
 *
 * Consumers typically will not need to consider the `balance` during normal operation. It is exposed for testing
 * purposes, to verify that no node in the tree ever has a balance with an absolute value greater than 1, but can
 * otherwise be safely ignored.
 */
data class IntervalTreeKeyValueWithNodeBalance<T: Comparable<T>, V> internal constructor(
    val key: IntervalRange<T>,
    val value: V,
    val balance: Int
)

/*
 * IntervalTree is an AVL tree, augmented according to the description of an augmented Red-Black Tree in
 *    Introduction to Algorithms (3rd Ed.) (2009)
 *    Cormen, Thomas H.; Leiserson, Charles E.; Rivest, Ronald L.; Stein, Clifford
 */
internal class TreeNode<T : Comparable<T>, V>(
    val leftKey: T,
    val rightKey: T,
    val maxRightKey: T,
    val value: IntervalTreeKeyValue<T, V>,
    val leftNode: TreeNode<T, V>?,
    val rightNode: TreeNode<T, V>?,
    val height: Byte
) {
    companion object {
        fun <T : Comparable<T>, V>newInstance(
            keys: IntervalRange<T>,
            value: V,
            leftNode: TreeNode<T, V>?,
            rightNode: TreeNode<T, V>?
        ): TreeNode<T, V> {
            val (leftKey, rightKey) = keys
            return newInstance(leftKey, rightKey, value, leftNode, rightNode)
        }

        fun <T: Comparable<T>, V>newInstance(
            leftKey: T,
            rightKey: T,
            value: IntervalTreeKeyValue<T, V>,
            leftNode: TreeNode<T, V>?,
            rightNode: TreeNode<T, V>?
        ): TreeNode<T, V> {
            var maxRightKey = rightKey
            if (leftNode != null) {
                val rightKeyCompared = maxRightKey.compareTo(leftNode.maxRightKey)
                maxRightKey = if (rightKeyCompared > 0) maxRightKey else leftNode.maxRightKey
            }
            if (rightNode != null) {
                val rightKeyCompared = maxRightKey.compareTo(rightNode.maxRightKey)
                maxRightKey = if (rightKeyCompared > 0) maxRightKey else rightNode.maxRightKey
            }
            val leftHeight = leftNode?.height ?: 0
            val rightHeight = rightNode?.height ?: 0
            val height = (leftHeight.toInt()).coerceAtLeast(rightHeight.toInt()) + 1
            return TreeNode(
                leftKey,
                rightKey,
                maxRightKey,
                value,
                leftNode,
                rightNode,
                height.toByte()
            )
        }

        fun <T : Comparable<T>, V>newInstance(
            leftKey: T,
            rightKey: T,
            value: V,
            leftNode: TreeNode<T, V>?,
            rightNode: TreeNode<T, V>?
        ): TreeNode<T, V> {
            return newInstance(
                leftKey,
                rightKey,
                IntervalTreeKeyValue(
                    IntervalRange(leftKey, rightKey),
                    value,
                ),
                leftNode,
                rightNode
            )
        }

        fun <T: Comparable<T>, V>rotateLeftLeft(node: TreeNode<T, V>): TreeNode<T, V> {
            // node.leftNode must not be null when this is called.
            val leftNode = node.leftNode!!
            val leftRight = leftNode.rightNode
            val newRight = newInstance(
                node.leftKey,
                node.rightKey,
                node.value,
                leftRight,
                node.rightNode
            )
            return newInstance(
                leftNode.leftKey,
                leftNode.rightKey,
                leftNode.value,
                leftNode.leftNode,
                newRight
            )
        }
        fun <T: Comparable<T>, V>rotateLeftRight(node: TreeNode<T, V>): TreeNode<T, V> {
            // node.leftNode must not be null when this is called.
            val leftNode = node.leftNode!!
            val leftRightNode = leftNode.rightNode ?: return node
            val leftRightLeft = leftRightNode.leftNode
            val leftRightRight = leftRightNode.rightNode
            val newLeftNode = newInstance(
                leftNode.leftKey,
                leftNode.rightKey,
                leftNode.value,
                leftNode.leftNode,
                leftRightLeft
            )
            val newRightNode = newInstance(
                node.leftKey,
                node.rightKey,
                node.value,
                leftRightRight,
                node.rightNode
            )
            return newInstance(
                leftRightNode.leftKey,
                leftRightNode.rightKey,
                leftRightNode.value,
                newLeftNode,
                newRightNode
            )
        }
        private fun <T: Comparable<T>, V>rotateRightLeft(node: TreeNode<T, V>): TreeNode<T, V> {
            // node.rightNode must not be null when this is called.
            val rightNode = node.rightNode!!
            val rightLeftNode = rightNode.leftNode ?: return node
            val rightLeftLeft = rightLeftNode.leftNode
            val rightLeftRight = rightLeftNode.rightNode
            val newLeftNode = newInstance(
                node.leftKey,
                node.rightKey,
                node.value,
                node.leftNode,
                rightLeftLeft
            )
            val newRightNode = newInstance(
                rightNode.leftKey,
                rightNode.rightKey,
                rightNode.value,
                rightLeftRight,
                rightNode.rightNode
            )
            return newInstance(
                rightLeftNode.leftKey,
                rightLeftNode.rightKey,
                rightLeftNode.value,
                newLeftNode,
                newRightNode
            )
        }
        private fun<T: Comparable<T>, V>rotateRightRight(node: TreeNode<T, V>): TreeNode<T, V> {
            // node.rightNode must not be null when this is called.
            val rightNode = node.rightNode!!
            val rightLeft = rightNode.leftNode
            val newLeft = newInstance(
                node.leftKey,
                node.rightKey,
                node.value,
                node.leftNode,
                rightLeft
            )
            return newInstance(
                rightNode.leftKey,
                rightNode.rightKey,
                rightNode.value,
                newLeft,
                rightNode.rightNode
            )
        }

        fun<T: Comparable<T>, V>rotateIfNecessary(node: TreeNode<T, V>): TreeNode<T, V> {
            val topWeight = node.weight()
            if (-1 <= topWeight && topWeight <= 1) {
                return node
            }
            if (topWeight < -1) {
                val leftNode = node.leftNode ?: return node
                return if (leftNode.weight() <= 0) {
                    rotateLeftLeft(node)
                } else {
                    rotateLeftRight(node)
                }
            } else {
                // We're right-heavy
                val rightNode = node.rightNode ?: return node
                return if (rightNode.weight() >= 0) {
                    rotateRightRight(node)
                } else {
                    rotateRightLeft(node)
                }
            }
        }
    }

    fun weight(): Int {
        val leftHeight = this.leftNode?.height ?: 0
        val rightHeight = this.rightNode?.height ?: 0
        return rightHeight - leftHeight
    }

    fun lookupExactRange(rangeLow: T, rangeHigh: T): V? {
        val rangeCompare = compareRanges(this.leftKey, this.rightKey, rangeLow, rangeHigh)
        if (rangeCompare == 0) {
            return this.value.value
        }
        if (rangeCompare > 0) {
            // We were greater than range, so look left
            val leftNode = this.leftNode ?: return null
            return leftNode.lookupExactRange(rangeLow, rangeHigh)
        } else {
            // We were less than range, so look right
            val rightNode = this.rightNode ?: return null
            return rightNode.lookupExactRange(rangeLow, rangeHigh)
        }
    }

    fun put(rangeLow: T, rangeHigh: T, value: V): TreeNode<T, V> {
        val rangeCompare = compareRanges(this.leftKey, this.rightKey, rangeLow, rangeHigh)
        if (rangeCompare == 0) {
            // We'll have to replace ourselves. Since the keys are equal
            // no re-balancing is necessary.
            return newInstance(rangeLow, rangeHigh, value, this.leftNode, this.rightNode)
        } else if (rangeCompare > 0) {
            // Range is less than us, look left.
            val newLeft = if (this.leftNode == null) {
                newInstance(rangeLow, rangeHigh, value, null, null)
            } else {
                this.leftNode.put(rangeLow, rangeHigh, value)
            }
            return rotateIfNecessary(newInstance(
                this.leftKey,
                this.rightKey,
                this.value,
                newLeft,
                this.rightNode
            ))
        } else {
            // Range is greater than us, look right.
            val newRight = if (this.rightNode == null) {
                newInstance(rangeLow, rangeHigh, value, null, null)
            } else {
                this.rightNode.put(rangeLow, rangeHigh, value)
            }
            return rotateIfNecessary(newInstance(
                this.leftKey,
                this.rightKey,
                this.value,
                this.leftNode,
                newRight
            ))
        }
    }

    private fun extractRightmostChild(): Pair<TreeNode<T, V>?, TreeNode<T, V>> {
        if (this.rightNode == null) {
            // We are the rightmost child.
            // We don't need to re-balance here; right was null
            // and left was "balanced enough", so we can only ever
            // start from a position of -1 and consequently end up
            // at a position of 0.
            return Pair(this.leftNode, this)
        } else {
            val result = this.rightNode.extractRightmostChild()
            val newRightNode = result.first
            val rightmostChild = result.second
            // We might need to re-balance here. However, we
            // only need to rotate in either the left-left or
            // left-right case, as only the right subtree will
            // be possibly reduced in height.
            val maybeReplacementNode = newInstance(
                this.leftKey,
                this.rightKey,
                this.value,
                this.leftNode,
                newRightNode
            )
            if (maybeReplacementNode.weight() >= -1 || maybeReplacementNode.leftNode == null) {
                return Pair(maybeReplacementNode, rightmostChild)
            }
            return if (maybeReplacementNode.leftNode.weight() <= 0) {
                Pair(rotateLeftLeft(maybeReplacementNode), rightmostChild)
            } else {
                Pair(rotateLeftRight(maybeReplacementNode), rightmostChild)
            }
        }
    }

    fun remove(rangeLow: T, rangeHigh: T): TreeNode<T, V>? {
        val rangeCompare = compareRanges(this.leftKey, this.rightKey, rangeLow, rangeHigh)
        val leftNode = this.leftNode
        val rightNode = this.rightNode
        if (rangeCompare == 0) {
            if (leftNode != null) {
                val liftRightmostResult = leftNode.extractRightmostChild()
                val newLeft = liftRightmostResult.first
                val newTop = liftRightmostResult.second
                return rotateIfNecessary(newInstance(
                    newTop.leftKey,
                    newTop.rightKey,
                    newTop.value,
                    newLeft,
                    rightNode
                ))
            } else {
                // We were balanced to begin with, so if leftNode is null, rightNode can have a height
                // of at most 1. Note that rightNode can also be null, which means that the result
                // should be null anyway.
                return rightNode
            }
        } else if (rangeCompare > 0) {
            if (leftNode == null) {
                return this
            }
            val newLeft = leftNode.remove(rangeLow, rangeHigh)
            if (newLeft === leftNode) {
                return this
            }
            return rotateIfNecessary(newInstance(
                this.leftKey,
                this.rightKey,
                this.value,
                newLeft,
                rightNode
            ))
        } else {
            if (rightNode == null) {
                return this
            }
            val newRight = rightNode.remove(rangeLow, rangeHigh)
            if (newRight === rightNode) {
                return this
            }
            return rotateIfNecessary(newInstance(
                this.leftKey,
                this.rightKey,
                this.value,
                leftNode,
                newRight
            ))
        }
    }
}

private enum class IntervalTreeIteratorNextStep { SELF, RIGHT }

private data class IntervalTreeIteratorState<T: Comparable<T>, V> (
    val node: TreeNode<T, V>,
    var nextStep: IntervalTreeIteratorNextStep
)

private abstract class IntervalTreeCommonIterator<T : Comparable<T>, V, R>(
    private val t: IntervalTree<T, V>
): Iterator<R> {
    protected val iterationStack: ArrayListStack<IntervalTreeIteratorState<T, V>> = ArrayListStack()
    private var nextUp: R? = null
    private var didInitialPush = false

    private fun initialPushIfNecessary() {
        // Note that we can't do this in the constructor;
        // subclasses implement functionality that we'd call before _their_ constructor ran.
        if (this.didInitialPush) {
            return
        }
        if (this.t.root != null) {
            this.pushIterationEntry(this.t.root)
            this.computeNextUp()
        }
        this.didInitialPush = true
    }

    private fun pushIterationEntry(n: TreeNode<T, V>) {
        var curNode: TreeNode<T, V>? = n
        while (curNode != null && this.iterateLeft(curNode)) {
            this.iterationStack.push(IntervalTreeIteratorState(curNode, IntervalTreeIteratorNextStep.SELF))
            curNode = curNode.leftNode
        }
    }

    private fun computeNextUp() {
        // We can't quite do this in .next() because of how .next()
        // is expected to work. .next() should always return and never
        // throw if .hasNext() == true, but there are definitely cases
        // where we're deep in the iteration stack and have no idea
        // whether we're returning or not
        var cur = this.iterationStack.peek()
        while (cur != null) {
            val curNode = cur.node
            if (cur.nextStep == IntervalTreeIteratorNextStep.SELF) {
                if (this.yieldNode(curNode)) {
                    cur.nextStep = IntervalTreeIteratorNextStep.RIGHT
                    this.nextUp = this.transformNode(curNode)
                    return
                }
            }
            // cur.nextStep == IntervalTreeIteratorNextStep.RIGHT
            this.iterationStack.pop()
            if (curNode.rightNode != null && this.iterateRight(curNode)) {
                this.pushIterationEntry(curNode.rightNode)
            }
            cur = this.iterationStack.peek()
        }
        this.nextUp = null
    }

    override fun hasNext(): Boolean {
        this.initialPushIfNecessary()
        return this.nextUp != null
    }

    abstract fun yieldNode(n: TreeNode<T, V>): Boolean
    abstract fun transformNode(n: TreeNode<T, V>): R
    abstract fun iterateRight(n: TreeNode<T, V>): Boolean
    abstract fun iterateLeft(n: TreeNode<T, V>): Boolean

    override fun next(): R {
        this.initialPushIfNecessary()
        val ret = this.nextUp ?: throw NoSuchElementException()
        this.computeNextUp()
        return ret
    }
}

private abstract class IntervalTreePairIterator<T : Comparable<T>, V>(
    t: IntervalTree<T, V>
) : IntervalTreeCommonIterator<T, V, IntervalTreeKeyValue<T, V>>(t) {
    override fun transformNode(n: TreeNode<T, V>): IntervalTreeKeyValue<T, V> {
        return n.value
    }
}

private class IntervalTreePointLookupIterator<T: Comparable<T>, V>(
    t: IntervalTree<T, V>,
    private val lookup: T
) : IntervalTreePairIterator<T, V>(t) {
    override fun iterateLeft(n: TreeNode<T, V>): Boolean {
        return this.lookup <= n.maxRightKey
    }

    override fun iterateRight(n: TreeNode<T, V>): Boolean {
        return this.lookup <= n.maxRightKey
    }

    override fun yieldNode(n: TreeNode<T, V>): Boolean {
        return n.leftKey <= this.lookup && this.lookup <= n.rightKey
    }
}

private class IntervalTreeRangeLookupIterator<T: Comparable<T>, V>(
    t: IntervalTree<T, V>,
    val lookupLow: T,
    val lookupHigh: T
) : IntervalTreePairIterator<T, V>(t) {
    private fun rangeOverlaps(otherLow: T, otherHigh: T): Boolean {
        return rangeOverlaps(this.lookupLow, this.lookupHigh, otherLow, otherHigh)
    }

    override fun iterateLeft(n: TreeNode<T, V>): Boolean {
        return this.lookupLow <= n.maxRightKey
    }

    override fun iterateRight(n: TreeNode<T, V>): Boolean {
        return this.rangeOverlaps(n.leftKey, n.maxRightKey)
    }

    override fun yieldNode(n: TreeNode<T, V>): Boolean {
        return this.rangeOverlaps(n.leftKey, n.rightKey)
    }
}

private class IntervalTreeIterator<T : Comparable<T>, V>(
    t: IntervalTree<T, V>
) : IntervalTreeCommonIterator<T, V, IntervalTreeKeyValueWithNodeBalance<T, V>>(t) {
    override fun iterateLeft(n: TreeNode<T, V>): Boolean {
        return true
    }

    override fun iterateRight(n: TreeNode<T ,V>): Boolean {
        return true
    }

    override fun yieldNode(n: TreeNode<T, V>): Boolean {
        return true
    }

    override fun transformNode(n: TreeNode<T, V>): IntervalTreeKeyValueWithNodeBalance<T, V> {
        val (key, value) = n.value
        return IntervalTreeKeyValueWithNodeBalance(key, value, n.weight())
    }
}

private fun <T: Comparable<T>, V> sizeTreePairFromIterable(
    entries: Iterable<Pair<IntervalRange<T>, V>>
): Pair<TreeNode<T, V>?, Long> {
    val it = entries.iterator()
    var root: TreeNode<T, V>
    var size: Long = 1
    if (it.hasNext()) {
        val (key, value) = it.next()
        root = TreeNode.newInstance(
            key,
            value,
            null,
            null
        )
    } else {
        return Pair(null, 0)
    }
    for ((key, value) in it) {
        val (lowerBound, upperBound) = key
        val prior = root.lookupExactRange(lowerBound, upperBound)
        root = root.put(lowerBound, upperBound, value)
        if (prior == null) {
            size += 1
        }
    }
    return Pair(root, size)
}

private enum class IntervalTreeInOrderIteratorNextStep { SELF, LEFT, RIGHT, DONE }

private data class IntervalTreeInOrderIteratorState<T: Comparable<T>, V> (
    val node: TreeNode<T, V>,
    val nextStep: IntervalTreeInOrderIteratorNextStep,
    val fromDirection: String
)

private class InOrderIterator<T: Comparable<T>, V>(
    tree: IntervalTree<T, V>
): Iterator<Triple<IntervalRange<T>, V, Pair<Int, String>>> {
    private val iterationStack: ArrayListStack<IntervalTreeInOrderIteratorState<T, V>> = ArrayListStack()

    private var nextUp: Triple<IntervalRange<T>, V, Pair<Int, String>>? = null

    init {
        if (tree.root != null) {
            this.iterationStack.push(
                IntervalTreeInOrderIteratorState(
                    tree.root,
                    IntervalTreeInOrderIteratorNextStep.SELF,
                    "root"
                )
            )
            this.computeNextUp()
        }
    }

    private fun computeNextUp() {
        // We can't quite do this in .next() because of how .next()
        // is expected to work. .next() should always return and never
        // throw if .hasNext() == true, but there are definitely cases
        // where we're deep in the iteration stack and have no idea
        // whether we're returning or not
        while (this.iterationStack.size > 0) {
            val cur = this.iterationStack.pop()!!
            val curNode = cur.node
            if (cur.nextStep == IntervalTreeInOrderIteratorNextStep.DONE) {
                continue
            }
            if (cur.nextStep == IntervalTreeInOrderIteratorNextStep.SELF) {
                val (key, value) = curNode.value
                this.nextUp = Triple(key, value, Pair(this.iterationStack.size, cur.fromDirection))
                this.iterationStack.push(
                    IntervalTreeInOrderIteratorState(
                        curNode,
                        IntervalTreeInOrderIteratorNextStep.LEFT,
                        cur.fromDirection
                    )
                )
                return
            }
            if (cur.nextStep == IntervalTreeInOrderIteratorNextStep.LEFT && curNode.leftNode != null) {
                this.iterationStack.push(
                    IntervalTreeInOrderIteratorState(
                        curNode,
                        IntervalTreeInOrderIteratorNextStep.RIGHT,
                        cur.fromDirection
                    )
                )
                this.iterationStack.push(
                    IntervalTreeInOrderIteratorState(
                        curNode.leftNode,
                        IntervalTreeInOrderIteratorNextStep.SELF,
                        "left"
                    )
                )
                continue
            }
            // cur.nextStep == IntervalTreeInOrderIteratorNextStep.RIGHT
            if (curNode.rightNode != null) {
                this.iterationStack.push(
                    IntervalTreeInOrderIteratorState(
                        curNode,
                        IntervalTreeInOrderIteratorNextStep.DONE,
                        cur.fromDirection
                    )
                )
                this.iterationStack.push(
                    IntervalTreeInOrderIteratorState(
                        curNode.rightNode,
                        IntervalTreeInOrderIteratorNextStep.SELF,
                        "right"
                    )
                )
                continue
            }
        }
        this.nextUp = null
    }

    override fun hasNext(): Boolean {
        return this.nextUp != null
    }

    override fun next(): Triple<IntervalRange<T>, V, Pair<Int, String>> {
        val ret = this.nextUp ?: throw NoSuchElementException()
        this.computeNextUp()
        return ret
    }
}

private fun<T: Comparable<T>, V> visitFromIterator(
    node: TreeNode<T, V>,
    iterator: IntervalTreePairIterator<T, V>,
    receiver: (result: IntervalTreeKeyValue<T, V>) -> Unit
) {
    val left = node.leftNode
    if (left != null && iterator.iterateLeft(node)) {
        visitFromIterator(left, iterator, receiver)
    }
    if (iterator.yieldNode(node)) {
        receiver(iterator.transformNode(node))
    }
    val right = node.rightNode ?: return
    if (iterator.iterateRight(node)) {
        visitFromIterator(right, iterator, receiver)
    }
}

private fun<T: Comparable<T>, V> visitFromIteratorForFull(
    node: TreeNode<T, V>,
    iterator: IntervalTreeIterator<T, V>,
    receiver: (result: IntervalTreeKeyValueWithNodeBalance<T, V>) -> Unit
) {
    val left = node.leftNode
    if (left != null) {
        // For a full iterator, we always iterate left.
        visitFromIteratorForFull(left, iterator, receiver)
    }
    // For a full iterator, we always yield.
    receiver(iterator.transformNode(node))
    val right = node.rightNode ?: return
    // For a full iterator, we always iterate right.
    visitFromIteratorForFull(right, iterator, receiver)
}

/**
 * A persistent, immutable associative map from keys of ordered pairs to arbitrary values.
 *
 * An IntervalTree stores values for ranges of keys, and supports lookups either by key ranges overlapping
 * a point, or key ranges overlapping another range. The semantics of "overlapping" are described in
 * [IntervalRange.overlaps].
 */
class IntervalTree<T: Comparable<T>, V> private constructor(
    internal val root: TreeNode<T, V>?,
    val size: Long,
): Iterable<IntervalTreeKeyValueWithNodeBalance<T, V>> {
    constructor(): this(null, 0)

    private constructor(treeSizePair: Pair<TreeNode<T, V>?, Long>): this(treeSizePair.first, treeSizePair.second)

    constructor(entries: Iterable<Pair<IntervalRange<T>, V>>) : this(sizeTreePairFromIterable(entries))

    /**
     * Returns an iterator over all key/value pairs contained in this [IntervalTree] whose key ranges
     * contain [at].
     *
     * [at] can be considered an [IntervalRange] where both the [IntervalRange.lowerBound] and
     * [IntervalRange.upperBound] are equal to [at], and "contains" is a synonym for
     * [IntervalRange.overlaps].
     */
    fun lookupPoint(at: T): Iterator<IntervalTreeKeyValue<T, V>> {
        return IntervalTreePointLookupIterator(this, at)
    }

    /**
     * Calls [receiver] with every key/value pair contained in this [IntervalTree] whose key ranges
     * contain [at].
     *
     * [at] can be considered an [IntervalRange] where both the [IntervalRange.lowerBound] and
     * [IntervalRange.upperBound] are equal to [at], and "contains" is a synonym for
     * [IntervalRange.overlaps].
     */
    fun lookupPointVisit(at: T, receiver: (value: IntervalTreeKeyValue<T, V>) -> Unit) {
        val root = this.root ?: return
        visitFromIterator(root, IntervalTreePointLookupIterator(this, at), receiver)
    }

    /**
     * Returns an iterator over all key/value pairs contained in this [IntervalTree] whose key ranges
     * overlap [range].
     *
     * The semantics of "overlap" are described in [IntervalRange.overlaps].
     */
    fun lookupRange(range: IntervalRange<T>): Iterator<IntervalTreeKeyValue<T, V>> {
        return IntervalTreeRangeLookupIterator(this, range.lowerBound, range.upperBound)
    }

    /**
     * Calls [receiver] with every key/value pair contained in this [IntervalTree] whose key ranges
     * overlap [range].
     *
     * The semantics of "overlap" are described in [IntervalRange.overlaps].
     */
    fun lookupRangeVisit(range: IntervalRange<T>, receiver: (value: IntervalTreeKeyValue<T, V>) -> Unit) {
        val root = this.root ?: return
        visitFromIterator(root, IntervalTreeRangeLookupIterator(this, range.lowerBound, range.upperBound), receiver)
    }

    /**
     * Returns the value corresponding to the key exactly equal to [range], if such a value is contained in this
     * [IntervalTree], or `null` otherwise.
     */
    fun lookupExactRange(range: IntervalRange<T>): V? {
        return this.root?.lookupExactRange(range.lowerBound, range.upperBound)
    }

    /**
     * Returns a new [IntervalTree] containing all of this IntervalTree's key/value pairs, excepting the key/value
     * pair where the key is equal to [range] if such a pair exists, but with an additional key/value pair mapping
     * the key `range` to [value].
     *
     * If a key/value pair exists for `range` in this IntervalTree, and the `value` for this pair is identical
     * to the given `value`, the result will be this IntervalTree. This is permissible because the resulting
     * IntervalTree would otherwise be equivalent.
     */
    fun put(range: IntervalRange<T>, value: V): IntervalTree<T, V> {
        val root = this.root ?: return IntervalTree(
            TreeNode.newInstance(
                range,
                value,
                null,
                null
            ),
            1,
        )
        val preexisting = this.lookupExactRange(range)
        if (preexisting === value) {
            return this
        }
        return IntervalTree(
            root.put(range.lowerBound, range.upperBound, value),
            if (preexisting == null) { this.size + 1 } else { this.size },
        )
    }

    /**
     * Returns a new [IntervalTree] containing all of this IntervalTree's key/value pairs, excepting the key/value
     * pair where the key is equal to [range] if such a pair exists.
     *
     * If a key/value pair does not exist for `range` in this IntervalTree, the resulting value will be this
     * IntervalTree. This is permissible because the resulting IntervalTree would otherwise be equivalent.
     */
    fun remove(range: IntervalRange<T>): IntervalTree<T, V> {
        val root = this.root ?: return this
        val removedRoot = root.remove(range.lowerBound, range.upperBound)
        return if (removedRoot === root) {
            this
        } else {
            IntervalTree(removedRoot, this.size - 1)
        }
    }

    /**
     * Calls [updater] with the key/value pair contained in this [IntervalTree] where the key is equal to [range],
     * if such a pair exists, or `null` otherwise, and returns a new IntervalTree with the updated results.
     *
     * `updater` may return `null`; in this case, the key/value pair for the given `range` is removed from the resulting
     * IntervalTree.
     *
     * Semantics for the return value are described in [IntervalTree.put] for the case of the non-null return from
     * `updater`, and in [IntervalTree.remove] for the case of the null return from `updater`.
     */
    fun update(range: IntervalRange<T>, updater: (value: V?) -> V?): IntervalTree<T, V> {
        val prev = this.lookupExactRange(range)
        val repl = updater(prev)
        if (prev === repl) {
            return this
        }
        val removed = this.remove(range)
        return if (repl == null) {
            removed
        } else {
            removed.put(range, repl)
        }
    }

    override fun iterator(): Iterator<IntervalTreeKeyValueWithNodeBalance<T, V>> {
        return IntervalTreeIterator(this)
    }

    /**
     * Calls [receiver] with every key/value pair in this [IntervalTree].
     */
    fun visitAll(receiver: (value: IntervalTreeKeyValueWithNodeBalance<T, V>) -> Unit) {
        val root = this.root ?: return
        visitFromIteratorForFull(root, IntervalTreeIterator(this), receiver)
    }

    // This is only used for testing purposes.
    internal fun inOrderIterator(): Iterator<Triple<IntervalRange<T>, V, Pair<Int, String>>> {
        return InOrderIterator(this)
    }

    /**
     * Returns the [IntervalRange] with the minimum [IntervalRange.lowerBound] in this [IntervalTree], or
     * `null` if this IntervalTree does not contain any entries.
     */
    fun minRange(): IntervalRange<T>? {
        var cur = this.root
        while (cur != null) {
            if (cur.leftNode != null) {
                cur = cur.leftNode
            } else {
                return IntervalRange(cur.leftKey, cur.rightKey)
            }
        }
        return null
    }

    /**
     * Returns the [IntervalRange] with the maximum [IntervalRange.lowerBound] in this [IntervalTree], or
     * `null` if this IntervalTree does not contain any entries.
     */
    fun maxRange(): IntervalRange<T>? {
        var cur = this.root
        while (cur != null) {
            if (cur.rightNode != null) {
                cur = cur.rightNode
            } else {
                return IntervalRange(cur.leftKey, cur.rightKey)
            }
        }
        return null
    }
}