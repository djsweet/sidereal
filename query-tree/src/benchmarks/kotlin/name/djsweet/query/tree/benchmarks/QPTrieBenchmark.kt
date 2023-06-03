package name.djsweet.query.tree.benchmarks

import kotlinx.collections.immutable.PersistentMap
import kotlinx.collections.immutable.persistentMapOf
import name.djsweet.query.tree.QPTrie
import name.djsweet.query.tree.QPTrieKeyValue
import org.openjdk.jmh.annotations.*
import net.jqwik.api.*
import java.util.*
import java.util.concurrent.TimeUnit

@State(Scope.Benchmark)
class QPTrieRunSpec {
    @Param("100", "200", "400", "800", "1600")
    var treeSize: Int = 0

    var trie = QPTrie<Boolean>()
    var lookup: ByteArray = byteArrayOf()
    val byteArrayArbitrary: Arbitrary<ByteArray> = Arbitraries
        .bytes()
        .array(ByteArray::class.java)
        .ofMinSize(5)
        .ofMaxSize(20)

    val keyReceiver = ArrayList<ByteArray>()

    protected fun setupWithData(data: List<ByteArray>) {
        var nextTrie = QPTrie<Boolean>()
        for (entry in data) {
            nextTrie = nextTrie.put(entry, true)
        }
        this.trie = nextTrie
    }

    protected fun sampleByteArray(): List<ByteArray> {
        return this.byteArrayArbitrary.list().ofSize(this.treeSize).sample()
    }

    @Setup(Level.Iteration)
    fun setup() {
        val entries = this.sampleByteArray()
        this.setupWithData(entries)
        this.lookup = entries.shuffled().first()
        this.keyReceiver.ensureCapacity(this.trie.size.toInt())
    }
}

@State(Scope.Benchmark)
class QPTrieAddSpec: QPTrieRunSpec() {
    @Setup(Level.Iteration)
    override fun setup() {
        val entries = this.sampleByteArray()
        this.setupWithData(entries)
        var toAdd = this.byteArrayArbitrary.sample()
        while (this.trie.get(toAdd) != null) {
            toAdd = this.byteArrayArbitrary.sample()
        }
        this.lookup = toAdd
    }
}

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations=5)
@Measurement(iterations=30)
class QPTrieBenchmark {
    // Mind the 00, 01, 02 ... business.
    // JMH tries to run these benchmarks in alphabetical order, so you'll have to follow alphabetical order
    // if you want the benchmarks to run in said order. Introducing an explicit serial number does the trick
    // of enforcing this number.

    @Benchmark
    fun point00Gets(spec: QPTrieRunSpec): Boolean {
        return spec.trie.get(spec.lookup) ?: throw Error("Could not find lookup in QPTrie")
    }

    @Benchmark
    fun point01Adds(spec: QPTrieAddSpec): QPTrie<Boolean> {
        return spec.trie.update(spec.lookup) {
            if (it != null) {
                throw Error("Tried to add an existing key to QPTrie")
            }
            true
        }
    }

    @Benchmark
    fun point02Updates(spec: QPTrieRunSpec): QPTrie<Boolean> {
        return spec.trie.update(spec.lookup) {
            if (it != true) {
                throw Error("Could not find lookup in QPTrie")
            }
            false
        }
    }

    @Benchmark
    fun point03Deletes(spec: QPTrieRunSpec): QPTrie<Boolean> {
        return spec.trie.update(spec.lookup) {
            if (it != true) {
                throw Error("Could not find lookup in QPTrie")
            }
            null
        }
    }

    @Benchmark
    fun iterator00Full(spec: QPTrieRunSpec): QPTrieKeyValue<Boolean>? {
        var lastEntry: QPTrieKeyValue<Boolean>? = null
        var seenEntries = 0
        for (ent in spec.trie.iteratorUnsafeSharedKey()) {
            lastEntry = ent
            seenEntries++
        }
        if (seenEntries.toLong() != spec.trie.size) {
            throw Error("Only saw some entries in full iteration")
        }
        return lastEntry
    }

    @Benchmark
    fun iterator000KeysInto(spec: QPTrieRunSpec): ArrayList<ByteArray> {
        spec.keyReceiver.clear()
        spec.trie.keysIntoUnsafeSharedKey(spec.keyReceiver)
        return spec.keyReceiver
    }

    @Benchmark
    fun iterator01LessThanOrEqual(spec: QPTrieRunSpec): QPTrieKeyValue<Boolean>? {
        var lastEntry: QPTrieKeyValue<Boolean>? = null
        for (ent in spec.trie.iteratorLessThanOrEqualUnsafeSharedKey(spec.lookup)) {
            lastEntry = ent
        }
        return lastEntry
    }

    @Benchmark
    fun iterator02GreaterThanOrEqual(spec: QPTrieRunSpec): QPTrieKeyValue<Boolean>? {
        var lastEntry: QPTrieKeyValue<Boolean>? = null
        for (ent in spec.trie.iteratorGreaterThanOrEqualUnsafeSharedKey(spec.lookup)) {
            lastEntry = ent
        }
        return lastEntry
    }

    @Benchmark
    fun visit00Full(spec: QPTrieRunSpec): QPTrieKeyValue<Boolean>? {
        var lastEntry: QPTrieKeyValue<Boolean>? = null
        var seenEntries = 0
        spec.trie.visitUnsafeSharedKey {
            lastEntry = it
            seenEntries++
        }
        if (seenEntries.toLong() != spec.trie.size) {
            throw Error("Only saw some entries in full iteration")
        }
        return lastEntry
    }

    @Benchmark
    fun visit01LessThanOrEqual(spec: QPTrieRunSpec): QPTrieKeyValue<Boolean>? {
        var lastEntry: QPTrieKeyValue<Boolean>? = null
        spec.trie.visitLessThanOrEqualUnsafeSharedKey(spec.lookup) {
            lastEntry = it
        }
        return lastEntry
    }

    @Benchmark
    fun visit02GreaterThanOrEqual(spec: QPTrieRunSpec): QPTrieKeyValue<Boolean>? {
        var lastEntry: QPTrieKeyValue<Boolean>? = null
        spec.trie.visitGreaterThanOrEqualUnsafeSharedKey(spec.lookup) {
            lastEntry = it
        }
        return lastEntry
    }
}

class ComparableByteArray(val array: ByteArray): Comparable<ComparableByteArray> {
    override fun compareTo(other: ComparableByteArray): Int {
        return Arrays.compareUnsigned(this.array, other.array)
    }

    override fun equals(other: Any?): Boolean {
        return if (other === this) {
            true
        } else if (other is ComparableByteArray) {
            this.array.contentEquals(other.array)
        } else {
            super.equals(other)
        }
    }

    override fun hashCode(): Int {
        return this.array.contentHashCode()
    }
}

@State(Scope.Benchmark)
class PersistentMapRunSpec {
    @Param("100", "200", "400", "800", "1600")
    var treeSize: Int = 0

    var map = persistentMapOf<ComparableByteArray, Boolean>()
    var lookup = ComparableByteArray(byteArrayOf())
    // It's not clear whether ByteArray.hashCode() is the right thing,
    // IntelliJ's auto-generated hashCode calls contentHashCode instead,
    // so it's likely not accurate to the contents. We'll have to lift
    // our ByteArray into a ComparableByteArray so that we have guaranteed
    // behavior here.
    val byteArrayArbitrary: Arbitrary<ComparableByteArray> = Arbitraries
        .bytes()
        .array(ByteArray::class.java)
        .ofMinSize(5)
        .ofMaxSize(20)
        .map { ComparableByteArray(it) }

    protected fun setupWithData(data: List<ComparableByteArray>) {
        var nextMap = persistentMapOf<ComparableByteArray, Boolean>()
        for (entry in data) {
            nextMap = nextMap.put(entry, true)
        }
        this.map = nextMap
    }

    protected fun sampleStrings(): List<ComparableByteArray> {
        return this.byteArrayArbitrary.list().ofSize(this.treeSize).sample()
    }

    @Setup(Level.Iteration)
    fun setup() {
        val entries = this.sampleStrings()
        this.setupWithData(entries)
        this.lookup = entries.shuffled().first()
    }
}

@State(Scope.Benchmark)
class PersistentMapAddSpec: PersistentMapRunSpec() {
    @Setup(Level.Iteration)
    override fun setup() {
        val entries = this.sampleStrings()
        this.setupWithData(entries)
        var toAdd = this.byteArrayArbitrary.sample()
        while (this.map[toAdd] != null) {
            toAdd = this.byteArrayArbitrary.sample()
        }
        this.lookup = toAdd
    }
}

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations=5)
@Measurement(iterations=30)
class PersistentMapBenchmark {
    // Mind the 00, 01, 02 ... business.
    // JMH tries to run these benchmarks in alphabetical order, so you'll have to follow alphabetical order
    // if you want the benchmarks to run in said order. Introducing an explicit serial number does the trick
    // of enforcing this number.

    @Benchmark
    fun point00Gets(spec: PersistentMapRunSpec): Boolean {
        return spec.map[spec.lookup] ?: throw Error("Could not find lookup in QPTrie")
    }

    @Benchmark
    fun point01Adds(spec: PersistentMapAddSpec): PersistentMap<ComparableByteArray, Boolean> {
        return spec.map.put(spec.lookup, true)
    }

    @Benchmark
    fun point02Updates(spec: PersistentMapRunSpec): PersistentMap<ComparableByteArray, Boolean> {
        return spec.map.put(spec.lookup, false)
    }

    @Benchmark
    fun point03Deletes(spec: PersistentMapRunSpec): PersistentMap<ComparableByteArray, Boolean> {
        return spec.map.remove(spec.lookup)
    }

    @Benchmark
    fun iterator00Full(spec: PersistentMapRunSpec): Map.Entry<ComparableByteArray, Boolean>? {
        var lastEntry: Map.Entry<ComparableByteArray, Boolean>? = null
        var seenEntries = 0
        for (ent in spec.map) {
            lastEntry = ent
            seenEntries++
        }
        if (seenEntries != spec.map.size) {
            throw Error("Only saw some entries in full iteration")
        }
        return lastEntry
    }
}