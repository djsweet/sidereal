package name.djsweet.thorium

import java.util.Arrays
import kotlin.collections.ArrayList

abstract class BinaryHeap<T, U : BinaryHeap<T, U>> {
    private val backing = ArrayList<T>()
    val size get() = this.backing.size

    protected abstract fun self(): U
    protected abstract fun maintainsHeapInvariant(parent: T, child: T): Boolean

    fun clear() {
        this.backing.clear()
    }

    fun peek(): T? {
        val backing = this.backing
        return if (backing.size == 0) {
            null
        } else {
            backing[0]
        }
    }

    private fun leftChild(i: Int): Int {
        return 2 * i + 1
    }

    private fun parent(i: Int): Int {
        return (i - 1) / 2
    }

    fun push(item: T): U {
        val backing = this.backing
        var i = backing.size
        backing.add(item)

        while (i > 0) {
            val parentOffset = this.parent(i)
            val parent = backing[parentOffset]
            if (this.maintainsHeapInvariant(parent, item)) {
                break
            }
            backing[i] = parent
            backing[parentOffset] = item
            i = parentOffset
        }

        return this.self()
    }

    private fun downPush(item: T) {
        val backing = this.backing
        val size = backing.size
        if (backing.size == 0) {
            backing.add(item)
        } else {
            backing[0] = item
        }

        var i = 0
        while (i < size) {
            val leftOffset = this.leftChild(i)
            val rightOffset = leftOffset + 1
            var leftChild: T? = null
            var rightChild: T? = null
            if (leftOffset < size) {
                leftChild = backing[leftOffset]
            }
            if (rightOffset < size) {
                rightChild = backing[rightOffset]
            }

            if (leftChild != null && rightChild != null) {
                // The parent might actually dominate both children.
                // There's no guarantee that one child will dominate another, but the new parent
                // needs to dominate both the original parent and the remaining child.
                val needsLeftReplacement = !this.maintainsHeapInvariant(item, leftChild)
                val needsRightReplacement = !this.maintainsHeapInvariant(item, rightChild)
                if (needsLeftReplacement && needsRightReplacement) {
                    val recurseLeft = this.maintainsHeapInvariant(leftChild, rightChild)
                    if (recurseLeft) {
                        backing[i] = leftChild
                        backing[leftOffset] = item
                        i = leftOffset
                    } else {
                        backing[i] = rightChild
                        backing[rightOffset] = item
                        i = rightOffset
                    }
                    continue
                }
            }

            if (leftChild != null && !this.maintainsHeapInvariant(item, leftChild)) {
                backing[i] = leftChild
                backing[leftOffset] = item
                i = leftOffset
                continue
            }

            if (rightChild != null && !this.maintainsHeapInvariant(item, rightChild)) {
                backing[i] = rightChild
                backing[rightOffset] = item
                i = rightOffset
                continue
            }

            break
        }
    }

    fun pop(): T? {
        val backing = this.backing
        val size = backing.size
        if (size == 0) {
            return null
        }
        val result = backing[0]
        val nextSize = size - 1
        if (nextSize > 0) {
            val cur = backing[nextSize]
            backing.removeAt(nextSize)
            this.downPush(cur)
        } else {
            backing.removeAt(nextSize)
        }
        return result
    }

    fun popPush(item: T): T? {
        val backing = this.backing
        val size = backing.size
        val result = if (size == 0) {
            null
        } else {
            backing[0]
        }
        this.downPush(item)
        return result
    }
}

class ByteArrayKeyedBinaryMinHeap<T>: BinaryHeap<Pair<ByteArray, T>, ByteArrayKeyedBinaryMinHeap<T>>() {
    override fun self(): ByteArrayKeyedBinaryMinHeap<T> {
        return this
    }

    override fun maintainsHeapInvariant(parent: Pair<ByteArray, T>, child: Pair<ByteArray, T>): Boolean {
        val (parentBytes) = parent
        val (childBytes) = child
        return Arrays.compareUnsigned(parentBytes, childBytes) <= 0
    }
}