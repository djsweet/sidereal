package name.djsweet.query.tree

internal abstract class ConcatenatedIterator<T>: Iterator<T> {
    private var nextValue: T? = null
    private var currentOffset: Int = 0
    private var currentIterator: Iterator<T>? = null
    private var ended: Boolean = false

    protected abstract fun iteratorForOffset(offset: Int): Iterator<T>?

    private fun computeNextValueIfNecessary() {
        if (this.nextValue != null) {
            return
        }
        if (this.ended) {
            return
        }
        var current = this.currentIterator
        if (current == null) {
            current = this.iteratorForOffset(this.currentOffset)
            this.currentIterator = current
        }
        while (current != null) {
            if (current.hasNext()) {
                this.nextValue = current.next()
                return
            }

            this.currentOffset += 1
            current = this.iteratorForOffset(this.currentOffset)
            this.currentIterator = current
        }
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

internal class SingleElementIterator<T>(private val item: T): Iterator<T> {
    private var consumed: Boolean = false

    override fun hasNext(): Boolean {
        return !this.consumed
    }

    override fun next(): T {
        if (this.consumed) {
            throw NoSuchElementException()
        }
        this.consumed = true
        return this.item
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