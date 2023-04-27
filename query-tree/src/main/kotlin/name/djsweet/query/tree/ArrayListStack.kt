package name.djsweet.query.tree

// Something about kotlin.collections.ArrayList makes it measurably slower than java.util.ArrayList,
// hence the explicit import here.
import java.util.*

internal const val DEFAULT_ARRAY_LIST_STACK_CAPACITY = 2

// java.util.Stack is fully synchronized, which adds unnecessary (and measurable) overhead for single-threaded
// operations. Thankfully, java.util.ArrayList is explicitly not synchronized anywhere.
internal class ArrayListStack<V>: Iterable<V> {
    private val underlying = ArrayList<V>(DEFAULT_ARRAY_LIST_STACK_CAPACITY)

    override fun iterator(): Iterator<V> {
        return underlying.iterator()
    }

    val size get() = this.underlying.size

    fun push(value: V) {
        this.underlying.add(value)
    }

    fun pop(): V? {
        return if (this.size == 0) {
            null
        } else {
            this.underlying.removeAt(this.size - 1)
        }
    }

    fun peek(): V? {
        return if (this.size == 0) {
            null
        } else {
            this.underlying[this.size - 1]
        }
    }
}