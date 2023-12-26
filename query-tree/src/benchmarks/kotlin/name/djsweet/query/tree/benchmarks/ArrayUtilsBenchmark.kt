// SPDX-FileCopyrightText: 2023 Dani Sweet <thorium@djsweet.name>
//
// SPDX-License-Identifier: MIT

package name.djsweet.query.tree.benchmarks

import name.djsweet.query.tree.QPTrieUtils
import org.openjdk.jmh.annotations.*
import java.util.concurrent.TimeUnit

val allNybbles = byteArrayOf(
    0, 1, 2, 3,
    4, 5, 6, 7,
    8, 9, 10, 11,
    12, 13, 14, 15
)

@State(Scope.Benchmark)
class ArrayUtilsSpec {
    @Param("1", "2", "4", "8", "16")
    var arraySize: Int = 0
    var targetArray = byteArrayOf()
    var targetNybble = 0.toByte()

    @Setup(Level.Iteration)
    fun setup() {
        this.targetArray = ByteArray(this.arraySize)
        val entrySchedule = allNybbles.clone()
        entrySchedule.shuffle()
        for (i in 0 until this.arraySize) {
            this.targetArray[i] = entrySchedule[i]
        }

        val newTargetOffset = (Math.random() * ((this.arraySize + 1).coerceAtMost(16))).toInt()
        this.targetNybble = entrySchedule[newTargetOffset]
    }
}

fun findByteInArrayLoop(haystack: ByteArray, needle: Byte): Int {
    for (i in haystack.indices) {
        if (haystack[i] == needle) return i
    }
    return -1
}

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations=5)
@Measurement(iterations=30)
class ArrayUtilsBenchmark {
    @Benchmark
    fun loopBenchmark(spec: ArrayUtilsSpec): Int {
        return findByteInArrayLoop(spec.targetArray, spec.targetNybble)
    }

    @Benchmark
    fun switchBenchmark(spec: ArrayUtilsSpec): Int {
        return QPTrieUtils.offsetForNybble(spec.targetArray, spec.targetNybble)
    }
}