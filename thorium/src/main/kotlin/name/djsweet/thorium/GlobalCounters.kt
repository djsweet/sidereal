package name.djsweet.thorium

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.LongAdder

class DecrementHeavyGague {
    private val increments = AtomicLong()
    private val decrements = LongAdder()

    val current: Long get() {
        // We expect the invariant of increments before decrements,
        // so if we read the decrements before the increments, we are
        // biased in terms of the increments. This is the behavior we want.
        val decrement = this.decrements.sum()
        return this.increments.get() - decrement
    }

    fun incrementByAndGet(value: Long): Long {
        val decrement = this.decrements.sum()
        val increment = this.increments.addAndGet(value)
        return increment - decrement
    }

    fun decrement(value: Long) {
        // increments.current can become expensive, so for decrements we avoid it.
        this.decrements.add(value)
    }
}

class GlobalCounterContext(queryServerCount: Int) {
    private val queryCounters = Array(queryServerCount) { AtomicLong() }
    private val globalQueryCount = AtomicLong()
    private val globalDataCount = DecrementHeavyGague()

    fun getQueryCountForThread(thread: Int): Long {
        return if (thread >= this.queryCounters.size) { 0L } else { this.queryCounters[thread].get() }
    }

    fun alterQueryCountForThread(thread: Int, alterBy: Long) {
        if (thread >= this.queryCounters.size || alterBy == 0L) {
            return
        }
        this.queryCounters[thread].addAndGet(alterBy)
    }

    fun resetQueryCountForThread(thread: Int) {
        if (thread >= this.queryCounters.size) {
            return
        }
        this.queryCounters[thread].set(0)
    }

    fun getOutstandingDataCount(): Long {
        return this.globalDataCount.current
    }

    fun incrementOutstandingDataCountByAndGet(incrementBy: Long): Long {
        return this.globalDataCount.incrementByAndGet(incrementBy)
    }

    fun decrementOutstandingDataCount(decrementBy: Long) {
        this.globalDataCount.decrement(decrementBy)
    }

    fun incrementGlobalQueryCountByAndGet(incrementBy: Long): Long {
        return this.globalQueryCount.addAndGet(incrementBy)
    }

    fun decrementGlobalQueryCount(decrementBy: Long) {
        this.globalQueryCount.addAndGet(-decrementBy)
    }
}