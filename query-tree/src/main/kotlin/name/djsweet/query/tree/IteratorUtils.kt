// SPDX-FileCopyrightText: 2023 Dani Sweet <sidereal@djsweet.name>
//
// SPDX-License-Identifier: MIT

package name.djsweet.query.tree

import kotlin.NoSuchElementException

/**
 * A composable iterator for concatenating other iterators
 *
 * An important feature of [ConcatenatedIterator] is that it maintains its own "call stack" on the heap.
 * This is done to make it suitable for use in stack-constrained operations. One such important use-case is to
 * verify that fully stack-recursive iterators yield the same items in the same order as the proper [Iterator]
 * instances, where the equivalence is established deep in the call stack with very little available remaining
 * stack space.
 */
internal abstract class ConcatenatedIterator<T>: Iterator<T> {
    private var nextValue: T? = null
    private var currentOffset: Int = 0
    private var currentIterator: Iterator<T>? = null
    private var ended: Boolean = false

    private var childStack: ArrayListStack<ConcatenatedIterator<T>>? = null
    private var ownsChildStack = false

    protected abstract fun iteratorForOffset(offset: Int): Iterator<T>?

    internal fun copyChildStack(): List<ConcatenatedIterator<T>> {
        val currentStack = this.childStack ?: return listOf()
        return currentStack.toList()
    }

    private fun peekChildStackIfOwner(): ConcatenatedIterator<T>? {
        // Note that `this` is on the child stack, but we never return `this` as part of this method.
        return if (this.ownsChildStack) {
            val onStack = this.childStack?.peek()
            return if (onStack === this) { null } else { onStack }
        } else {
            null
        }
    }

    private fun iteratorForCurrent(): Iterator<T>? {
        val workingStack = this.childStack ?: ArrayListStack()
        if (this.childStack == null) {
            // Adding `this` to the working stack simplifies getting the next iterator for the next offset
            // in the below loop.
            workingStack.push(this)
            this.childStack = workingStack
            this.ownsChildStack = true
        }

        var current = this.currentIterator ?: this.peekChildStackIfOwner()

        if (current == null) {
            current = this.iteratorForOffset(this.currentOffset)
            this.currentOffset++
        }
        if (!this.ownsChildStack || current !is ConcatenatedIterator<T>) {
            this.currentIterator = current
            return current
        }

        // Here's where we maintain the internal call stack replacement.
        while (current is ConcatenatedIterator<T>) {
            if (workingStack.peek() !== current) {
                current.childStack = workingStack
                workingStack.push(current)
            }
            current = when (val nextIterator = current.iteratorForOffset(current.currentOffset++)) {
                is ConcatenatedIterator<T> -> {
                    nextIterator
                }

                null -> {
                    workingStack.pop()
                    workingStack.peek()
                }

                else -> {
                    current.currentIterator = nextIterator
                    this.currentIterator = nextIterator
                    // Note that this returns entirely from iteratorForCurrent
                    return nextIterator
                }
            }
        }

        return null
    }

    private fun computeNextValueIfNecessary() {
        if (this.nextValue != null) {
            return
        }
        if (this.ended) {
            return
        }

        // To deal with deeply composed ConcatenatedIterators while avoiding StackOverflowError, we implement
        // something of a call stack iteratively here.
        var current = this.iteratorForCurrent()
        while (current != null) {
            if (current.hasNext()) {
                this.nextValue = current.next()
                return
            } else {
                this.currentIterator = null
                current = this.iteratorForCurrent()
            }
        }

        this.childStack?.clear()
        this.ended = true
    }

    override fun hasNext(): Boolean {
        this.computeNextValueIfNecessary()
        return this.nextValue != null
    }

    override fun next(): T {
        this.computeNextValueIfNecessary()
        val ret = this.nextValue ?: throw NoSuchElementException()
        this.nextValue = null
        return ret
    }
}

internal class SingleElementIterator<T>(private var item: T?): Iterator<T> {
    override fun hasNext(): Boolean {
        return this.item != null
    }

    override fun next(): T {
        val result = this.item ?: throw NoSuchElementException()
        this.item = null
        return result
    }
}

internal class EmptyIterator<T>: Iterator<T> {
    override fun hasNext(): Boolean {
        return false
    }

    override fun next(): T {
        throw NoSuchElementException()
    }
}

internal class FlattenIterator<T>(
    private val iterators: Iterator<Iterator<T>>
): Iterator<T> {
    private var cur: Iterator<T>? = null

    private fun possiblySetupNextIterator() {
        var cur = this.cur
        if (cur != null && cur.hasNext()) {
            return
        }
        while (this.iterators.hasNext()) {
            cur = this.iterators.next()
            this.cur = cur
            if (cur.hasNext()) {
                return
            }
        }
        this.cur = null
    }

    override fun hasNext(): Boolean {
        this.possiblySetupNextIterator()
        val cur = this.cur
        return cur != null && cur.hasNext()
    }

    override fun next(): T {
        this.possiblySetupNextIterator()
        val cur = this.cur
        if (cur == null || !cur.hasNext()) {
            throw NoSuchElementException()
        }
        return cur.next()
    }
}

internal class MapSequenceIterator<U, V>(
    private val basis: Iterator<U>,
    private val functor: (item: U) -> V
): Iterator<V> {
    override fun hasNext(): Boolean {
        return this.basis.hasNext()
    }

    override fun next(): V {
        return this.functor(this.basis.next())
    }
}

internal fun <U, V> mapSequence(iterable: Iterable<U>, functor: (item: U) -> V): Iterator<V> {
    return MapSequenceIterator(iterable.iterator(), functor)
}

internal fun <U, V> mapSequence(iterator: Iterator<U>, functor: (item: U) -> V): Iterator<V> {
    return MapSequenceIterator(iterator, functor)
}