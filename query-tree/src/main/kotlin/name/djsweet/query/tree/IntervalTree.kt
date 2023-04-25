package name.djsweet.query.tree

import java.util.*
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

data class IntervalRange<T: Comparable<T>> internal constructor(val lowerBound: T, val upperBound: T): Comparable<IntervalRange<T>> {
    companion object {
        internal fun <T: Comparable<T>> fromPair(p: Pair<T, T>): IntervalRange<T> {
            val (lowerBound, upperBound) = enforceRangeInvariants(p)
            return IntervalRange(lowerBound, upperBound)
        }
    }

    fun toPair(): Pair<T, T> {
        return Pair(this.lowerBound, this.upperBound)
    }

    override fun compareTo(other: IntervalRange<T>): Int {
        return compareRanges(this.lowerBound, this.upperBound, other.lowerBound, other.upperBound)
    }
}

data class IntervalTreeKeyValue<T: Comparable<T>, V> internal constructor(val key: IntervalRange<T>, val value: V)

internal class TreeNode<T : Comparable<T>, V>(
    val leftKey: T,
    val rightKey: T,
    val maxRightKey: T,
    val value: V,
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

        fun <T : Comparable<T>, V>newInstance(
            leftKey: T,
            rightKey: T,
            value: V,
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
            return TreeNode(leftKey, rightKey, maxRightKey, value, leftNode, rightNode, height.toByte())
        }

        fun <T: Comparable<T>, V>rotateLeftLeft(node: TreeNode<T, V>): TreeNode<T, V> {
            if (node.leftNode == null) {
                return node
            }
            val leftNode = node.leftNode
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
            if (node.leftNode == null) {
                return node
            }
            val leftNode = node.leftNode
            if (leftNode.rightNode == null) {
                return node
            }
            val leftRightNode = leftNode.rightNode
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
            if (node.rightNode == null) {
                return node
            }
            val rightNode = node.rightNode
            if (rightNode.leftNode == null) {
                return node
            }
            val rightLeftNode = rightNode.leftNode
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
            if (node.rightNode == null) {
                return node
            }
            val rightNode = node.rightNode
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
            return this.value
        }
        if (rangeCompare > 0) {
            // We were greater than range, so look left
            if (this.leftNode == null) {
                return null
            }
            return this.leftNode.lookupExactRange(rangeLow, rangeHigh)
        } else {
            // We were less than range, so look right
            if (this.rightNode == null) {
                return null
            }
            return this.rightNode.lookupExactRange(rangeLow, rangeHigh)
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
        if (rangeCompare == 0) {
            if (this.leftNode != null) {
                val liftRightmostResult = this.leftNode.extractRightmostChild()
                val newLeft = liftRightmostResult.first
                val newTop = liftRightmostResult.second
                return rotateIfNecessary(newInstance(
                    newTop.leftKey,
                    newTop.rightKey,
                    newTop.value,
                    newLeft,
                    this.rightNode
                ))
            } else {
                // We were balanced to begin with, so if leftNode is null, rightNode can have a height
                // of at most 1. Note that rightNode can also be null, which means that the result
                // should be null anyway.
                return this.rightNode
            }
        } else if (rangeCompare > 0) {
            if (this.leftNode == null) {
                return this
            }
            val newLeft = this.leftNode.remove(rangeLow, rangeHigh)
            if (newLeft === this.leftNode) {
                return this
            }
            return rotateIfNecessary(newInstance(
                this.leftKey,
                this.rightKey,
                this.value,
                newLeft,
                this.rightNode
            ))
        } else {
            if (this.rightNode == null) {
                return this
            }
            val newRight = this.rightNode.remove(rangeLow, rangeHigh)
            if (newRight === this.rightNode) {
                return this
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
}

private enum class IntervalTreeIteratorNextStep { SELF, RIGHT }

private data class IntervalTreeIteratorState<T: Comparable<T>, V> (
    val node: TreeNode<T, V>,
    val nextStep: IntervalTreeIteratorNextStep
)

private abstract class IntervalTreeCommonIterator<T : Comparable<T>, V, R>(private val t: IntervalTree<T, V>): Iterator<R> {
    protected val iterationStack: Stack<IntervalTreeIteratorState<T, V>> = Stack()
    protected var nextUp: R? = null
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
        while (curNode != null) {
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
        while (!this.iterationStack.empty()) {
            val cur = this.iterationStack.pop()
            val curNode = cur.node
            if (cur.nextStep == IntervalTreeIteratorNextStep.SELF) {
                if (this.yieldNode(curNode)) {
                    this.iterationStack.push(IntervalTreeIteratorState(curNode, IntervalTreeIteratorNextStep.RIGHT))
                    this.nextUp = this.transformNode(curNode)
                    return
                }
            }
            // cur.nextStep == IntervalTreeIteratorNextStep.RIGHT
            if (curNode.rightNode != null && this.iterateRight(curNode)) {
                this.pushIterationEntry(curNode.rightNode)
            }
        }
        this.nextUp = null
    }

    override fun hasNext(): Boolean {
        this.initialPushIfNecessary()
        return this.nextUp != null
    }

    protected abstract fun yieldNode(n: TreeNode<T, V>): Boolean
    protected abstract fun transformNode(n: TreeNode<T, V>): R
    protected abstract fun iterateRight(n: TreeNode<T, V>): Boolean

    override fun next(): R {
        this.initialPushIfNecessary()
        val ret = this.nextUp ?: throw NoSuchElementException()
        this.computeNextUp()
        return ret
    }
}

private abstract class IntervalTreePairIterator<T : Comparable<T>, V>(t: IntervalTree<T, V>) :
    IntervalTreeCommonIterator<T, V, IntervalTreeKeyValue<T, V>>(t) {

    override fun transformNode(n: TreeNode<T, V>): IntervalTreeKeyValue<T, V> {
        return IntervalTreeKeyValue(IntervalRange(n.leftKey, n.rightKey), n.value)
    }
}

private class IntervalTreePointLookupIterator<T: Comparable<T>, V>(t: IntervalTree<T, V>, private val lookup: T) :
    IntervalTreePairIterator<T, V>(t) {

    override fun iterateRight(n: TreeNode<T, V>): Boolean {
        return this.lookup <= n.maxRightKey
    }

    override fun yieldNode(n: TreeNode<T, V>): Boolean {
        return n.leftKey <= this.lookup && this.lookup <= n.rightKey
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

private class IntervalTreeRangeLookupIterator<T: Comparable<T>, V>(t: IntervalTree<T, V>, val lookupLow: T, val lookupHigh: T) :
    IntervalTreePairIterator<T, V>(t) {
    private fun rangeOverlaps(otherLow: T, otherHigh: T): Boolean {
        return rangeOverlaps(this.lookupLow, this.lookupHigh, otherLow, otherHigh)
    }

    override fun iterateRight(n: TreeNode<T, V>): Boolean {
        return this.rangeOverlaps(n.leftKey, n.maxRightKey)
    }

    override fun yieldNode(n: TreeNode<T, V>): Boolean {
        return this.rangeOverlaps(n.leftKey, n.rightKey)
    }
}

private class IntervalTreeIterator<T : Comparable<T>, V>(t: IntervalTree<T, V>) : IntervalTreeCommonIterator<T, V, Triple<IntervalRange<T>, V, Int>>(t) {
    override fun iterateRight(n: TreeNode<T ,V>): Boolean {
        return true
    }

    override fun yieldNode(n: TreeNode<T, V>): Boolean {
        return true
    }

    override fun transformNode(n: TreeNode<T, V>): Triple<IntervalRange<T>, V, Int> {
        return Triple(IntervalRange(n.leftKey, n.rightKey), n.value, n.weight())
    }
}


private fun <T: Comparable<T>, V> sizeTreePairFromIterable(entries: Iterable<Pair<Pair<T, T>, V>>): Pair<Long, TreeNode<T, V>?> {
    val it = entries.iterator()
    var root: TreeNode<T, V>
    var size: Long = 1
    if (it.hasNext()) {
        val (key, value) = it.next()
        root = TreeNode.newInstance(
            IntervalRange.fromPair(key),
            value,
            null,
            null
        )
    } else {
        return Pair(0, null)
    }
    for ((key, value) in it) {
        val enforcedRange = enforceRangeInvariants(key)
        val prior = root.lookupExactRange(enforcedRange.first, enforcedRange.second)
        root = root.put(enforcedRange.first, enforcedRange.second, value)
        if (prior == null) {
            size += 1
        }
    }
    return Pair(size, root)
}


private enum class IntervalTreeInOrderIteratorNextStep { SELF, LEFT, RIGHT, DONE }


private data class IntervalTreeInOrderIteratorState<T: Comparable<T>, V> (
    val node: TreeNode<T, V>,
    val nextStep: IntervalTreeInOrderIteratorNextStep,
    val fromDirection: String
)

private class InOrderIterator<T: Comparable<T>, V>(tree: IntervalTree<T, V>): Iterator<Triple<IntervalRange<T>, V, Pair<Int, String>>> {
    private val iterationStack: Stack<IntervalTreeInOrderIteratorState<T, V>> = Stack()

    private var nextUp: Triple<IntervalRange<T>, V, Pair<Int, String>>? = null

    init {
        if (tree.root != null) {
            this.iterationStack.push(IntervalTreeInOrderIteratorState(tree.root, IntervalTreeInOrderIteratorNextStep.SELF, "root"))
            this.computeNextUp()
        }
    }

    private fun computeNextUp() {
        // We can't quite do this in .next() because of how .next()
        // is expected to work. .next() should always return and never
        // throw if .hasNext() == true, but there are definitely cases
        // where we're deep in the iteration stack and have no idea
        // whether we're returning or not
        while (!this.iterationStack.empty()) {
            val cur = this.iterationStack.pop()
            val curNode = cur.node
            if (cur.nextStep == IntervalTreeInOrderIteratorNextStep.DONE) {
                continue
            }
            if (cur.nextStep == IntervalTreeInOrderIteratorNextStep.SELF) {
                this.nextUp = Triple(IntervalRange(curNode.leftKey, curNode.rightKey), curNode.value, Pair(this.iterationStack.size, cur.fromDirection))
                this.iterationStack.push(IntervalTreeInOrderIteratorState(curNode, IntervalTreeInOrderIteratorNextStep.LEFT, cur.fromDirection))
                return
            }
            if (cur.nextStep == IntervalTreeInOrderIteratorNextStep.LEFT && curNode.leftNode != null) {
                this.iterationStack.push(IntervalTreeInOrderIteratorState(curNode, IntervalTreeInOrderIteratorNextStep.RIGHT, cur.fromDirection))
                this.iterationStack.push(IntervalTreeInOrderIteratorState(curNode.leftNode, IntervalTreeInOrderIteratorNextStep.SELF, "left"))
                continue
            }
            // cur.nextStep == IntervalTreeInOrderIteratorNextStep.RIGHT
            if (curNode.rightNode != null) {
                this.iterationStack.push(IntervalTreeInOrderIteratorState(curNode, IntervalTreeInOrderIteratorNextStep.DONE, cur.fromDirection))
                this.iterationStack.push(IntervalTreeInOrderIteratorState(curNode.rightNode, IntervalTreeInOrderIteratorNextStep.SELF, "right"))
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

class IntervalTree<T: Comparable<T>, V> private constructor(
    val size: Long,
    internal val root: TreeNode<T, V>?
): Iterable<Triple<IntervalRange<T>, V, Int>> {
    constructor(): this(0, null)

    private constructor(treeSizePair: Pair<Long, TreeNode<T, V>?>): this(treeSizePair.first, treeSizePair.second)

    constructor(entries: Iterable<Pair<Pair<T, T>, V>>) : this(sizeTreePairFromIterable(entries))

    fun lookupPoint(at: T): Iterator<IntervalTreeKeyValue<T, V>> {
        return IntervalTreePointLookupIterator(this, at)
    }

    fun lookupRange(maybeRange: Pair<T, T>): Iterator<IntervalTreeKeyValue<T, V>> {
        val enforced = enforceRangeInvariants(maybeRange)
        return IntervalTreeRangeLookupIterator(this, enforced.first, enforced.second)
    }

    fun lookupExactRange(maybeRange: Pair<T, T>): V? {
        val enforcedRange = enforceRangeInvariants(maybeRange)
        return this.root?.lookupExactRange(enforcedRange.first, enforcedRange.second)
    }

    fun lookupExactRange(maybeRange: IntervalRange<T>): V? {
        return this.root?.lookupExactRange(maybeRange.lowerBound, maybeRange.upperBound)
    }

    fun put(range: Pair<T, T>, value: V): IntervalTree<T, V> {
        val enforcedRange = IntervalRange.fromPair(range)
        if (this.root == null) {
            return IntervalTree(1, TreeNode.newInstance(enforcedRange, value, null, null))
        }
        val preexisting = this.lookupExactRange(enforcedRange)
        if (preexisting === value) {
            return this
        }
        return IntervalTree(
            if (preexisting == null) { this.size + 1 } else { this.size },
            this.root.put(enforcedRange.lowerBound, enforcedRange.upperBound, value)
        )
    }

    fun remove(range: Pair<T, T>): IntervalTree<T, V> {
        if (this.root == null) {
            return this
        }
        val enforcedRange = enforceRangeInvariants(range)
        this.lookupExactRange(enforcedRange) ?: return this
        return IntervalTree(this.size - 1, this.root.remove(enforcedRange.first, enforcedRange.second))
    }

    fun update(maybeRange: Pair<T, T>, updater: (value: V?) -> V?): IntervalTree<T, V> {
        val prev = this.lookupExactRange(maybeRange)
        val repl = updater(prev)
        if (prev === repl) {
            return this
        }
        val removed = this.remove(maybeRange)
        return if (repl == null) {
            removed
        } else {
            removed.put(maybeRange, repl)
        }
    }

    override fun iterator(): Iterator<Triple<IntervalRange<T>, V, Int>> {
        return IntervalTreeIterator(this)
    }

    internal fun inOrderIterator(): Iterator<Triple<IntervalRange<T>, V, Pair<Int, String>>> {
        return InOrderIterator(this)
    }

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