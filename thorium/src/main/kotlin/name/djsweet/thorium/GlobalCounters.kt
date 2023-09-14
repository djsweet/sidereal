package name.djsweet.thorium

import kotlinx.collections.immutable.PersistentMap
import kotlinx.collections.immutable.persistentMapOf
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

data class KeyPathReferenceCount(
    val references: Int,
    val subKeys: PersistentMap<String, KeyPathReferenceCount>
) {
    constructor(): this(0, persistentMapOf())

    private fun update(keyPath: List<String>, offset: Int, updater: (KeyPathReferenceCount) -> KeyPathReferenceCount?): KeyPathReferenceCount? {
        return if (offset >= keyPath.size) {
            updater(this)
        } else {
            val subKey = keyPath[offset]
            val subKeyEntry = this.subKeys[subKey] ?: KeyPathReferenceCount()
            val nextSubKeyEntry = subKeyEntry.update(keyPath, offset + 1, updater)
            if (nextSubKeyEntry == null) {
                val nextSubKeys = this.subKeys.remove(subKey)
                if (nextSubKeys.size == 0 && this.references <= 0) {
                    null
                } else {
                    this.copy(subKeys = nextSubKeys)
                }
            } else {
                this.copy(subKeys = this.subKeys.put(subKey, nextSubKeyEntry))
            }
        }
    }

    fun update(keyPath: List<String>, updater: (KeyPathReferenceCount) -> KeyPathReferenceCount?): KeyPathReferenceCount? {
        return this.update(keyPath, 0, updater)
    }

    fun isEmpty(): Boolean {
        return this.references <= 0 && this.subKeys.isEmpty()
    }

    fun nullIfEmpty(): KeyPathReferenceCount? {
        return if (this.isEmpty()) { null } else { this }
    }
}

class GlobalCounterContext(queryServerCount: Int) {
    private val queryCounters = Array(queryServerCount) { AtomicLong() }
    private val globalQueryCount = AtomicLong()
    private val globalDataCount = DecrementHeavyGague()

    @Volatile private var keyPathReferenceCountsByChannel: PersistentMap<String, KeyPathReferenceCount>
        = persistentMapOf()

    fun getKeyPathReferenceCountsForChannel(channel: String): KeyPathReferenceCount? {
        return this.keyPathReferenceCountsByChannel[channel]
    }

    @Synchronized fun updateKeyPathReferenceCountsForChannel(
        channel: String,
        updates: Iterable<Pair<List<String>, Int>>
    ) {
        val current = this.keyPathReferenceCountsByChannel[channel] ?: KeyPathReferenceCount()
        var channelUpdate = current
        for ((keyPath, increment) in updates) {
            channelUpdate = channelUpdate.update(keyPath) {
                it.copy(references = it.references + increment).nullIfEmpty()
            } ?: KeyPathReferenceCount()
        }
        if (channelUpdate.isEmpty()) {
            this.keyPathReferenceCountsByChannel = this.keyPathReferenceCountsByChannel.remove(channel)
        } else {
            this.keyPathReferenceCountsByChannel = this.keyPathReferenceCountsByChannel.put(channel, channelUpdate)
        }
    }

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