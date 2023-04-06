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