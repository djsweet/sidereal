// SPDX-FileCopyrightText: 2023 Dani Sweet <sidereal@djsweet.name>
//
// SPDX-License-Identifier: MIT

package name.djsweet.query.tree.benchmarks

import kotlinx.collections.immutable.*
import name.djsweet.query.tree.IdentitySet
import java.util.concurrent.TimeUnit
import net.jqwik.api.Arbitraries
import org.openjdk.jmh.annotations.*

@State(Scope.Benchmark)
class IdentitySetRunSpec {
    @Param("100", "200", "400", "800")
    var setSize: Int = 0

    var set = IdentitySet<Int>()
    var lookup: Int = 0

    protected fun generateEntries(): MutableList<Int> {
        return Arbitraries.integers().list().ofSize(this.setSize).sample()
    }

    @Setup(Level.Iteration)
    fun setup() {
        val entries = this.generateEntries()
        this.set = IdentitySet(entries)
        entries.shuffle()
        this.lookup = entries.first()
    }
}

@State(Scope.Benchmark)
class IdentitySetAddSpec: IdentitySetRunSpec() {
    @Setup(Level.Iteration)
    override fun setup() {
        val entries = this.generateEntries().shuffled()
        this.set = IdentitySet(entries.subList(1, entries.size))
        this.lookup = entries.first()
    }
}

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations=5)
@Measurement(iterations=30)
class IdentitySetBenchmark {
    @Benchmark
    fun point00contains(spec: IdentitySetRunSpec): Boolean {
        return spec.set.contains(spec.lookup)
    }

    @Benchmark
    fun point01add(spec: IdentitySetAddSpec): IdentitySet<Int> {
        return spec.set.add(spec.lookup)
    }

    @Benchmark
    fun point02remove(spec: IdentitySetRunSpec): IdentitySet<Int> {
        return spec.set.remove(spec.lookup)
    }

    @Benchmark
    fun iterator(spec: IdentitySetRunSpec): Int {
        var lastOne = 0
        for (entry in spec.set) {
            lastOne = entry
        }
        return lastOne
    }

    @Benchmark
    fun visitAll(spec: IdentitySetRunSpec): Int {
        var lastOne = 0
        spec.set.visitAll {
            lastOne = it
        }
        return lastOne
    }
}

@State(Scope.Benchmark)
class PersistentSetRunSpec {
    @Param("100", "200", "400", "800")
    var setSize: Int = 0

    var set = persistentSetOf<Int>()
    var lookup = 0

    protected fun generateEntries(): MutableList<Int> {
        return Arbitraries.integers().list().ofSize(this.setSize).sample()
    }

    @Setup(Level.Iteration)
    fun setup() {
        val entries = this.generateEntries()
        this.set = entries.toPersistentSet()
        entries.shuffle()
        this.lookup = entries.first()
    }
}

@State(Scope.Benchmark)
class PersistentSetAddSpec: PersistentSetRunSpec() {
    @Setup(Level.Iteration)
    override fun setup() {
        val entries = this.generateEntries().shuffled()
        this.set = entries.subList(1, entries.size).toPersistentSet()
        this.lookup = entries.first()
    }
}

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations=5)
@Measurement(iterations=30)
class PersistentSetBenchmark {
    @Benchmark
    fun point00contains(spec: PersistentSetRunSpec): Boolean {
        return spec.set.contains(spec.lookup)
    }

    @Benchmark
    fun point01add(spec: PersistentSetAddSpec): PersistentSet<Int> {
        return spec.set.add(spec.lookup)
    }

    @Benchmark
    fun point02remove(spec: PersistentSetRunSpec): PersistentSet<Int> {
        return spec.set.remove(spec.lookup)
    }

    @Benchmark
    fun iterator(spec: PersistentSetRunSpec): Int {
        var lastOne = 0
        for (entry in spec.set) {
            lastOne = entry
        }
        return lastOne
    }
}