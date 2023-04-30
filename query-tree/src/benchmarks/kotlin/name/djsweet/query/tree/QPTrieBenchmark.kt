package name.djsweet.query.tree

import kotlinx.collections.immutable.PersistentMap
import kotlinx.collections.immutable.persistentMapOf
import org.openjdk.jmh.annotations.*
import net.jqwik.api.*
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
        for (ent in spec.trie) {
            lastEntry = ent
            seenEntries++
        }
        if (seenEntries.toLong() != spec.trie.size) {
            throw Error("Only saw some entries in full iteration")
        }
        return lastEntry
    }

    @Benchmark
    fun iterator01LessThanOrEqual(spec: QPTrieRunSpec): QPTrieKeyValue<Boolean>? {
        var lastEntry: QPTrieKeyValue<Boolean>? = null
        for (ent in spec.trie.iteratorLessThanOrEqual(spec.lookup)) {
            lastEntry = ent
        }
        return lastEntry
    }

    @Benchmark
    fun iterator02GreaterThanOrEqual(spec: QPTrieRunSpec): QPTrieKeyValue<Boolean>? {
        var lastEntry: QPTrieKeyValue<Boolean>? = null
        for (ent in spec.trie.iteratorGreaterThanOrEqual(spec.lookup)) {
            lastEntry = ent
        }
        return lastEntry
    }
}

@State(Scope.Benchmark)
class PersistentMapRunSpec {
    @Param("100", "200", "400", "800", "1600")
    var treeSize: Int = 0

    var map = persistentMapOf<String, Boolean>()
    var lookup: String = ""
    // It's not clear whether ByteArray.hashCode() is the right thing,
    // IntelliJ's auto-generated hashCode calls contentHashCode instead
    // so it's likely not accurate to the contents. Instead, we'll compare
    // against strings, expecting them to be UTF-16, so halving the size.
    val stringArbitrary: Arbitrary<String> = Arbitraries
        .strings()
        .ofMinLength(3)
        .ofMaxLength(10)

    protected fun setupWithData(data: List<String>) {
        var nextMap = persistentMapOf<String, Boolean>()
        for (entry in data) {
            nextMap = nextMap.put(entry, true)
        }
        this.map = nextMap
    }

    protected fun sampleStrings(): List<String> {
        return this.stringArbitrary.list().ofSize(this.treeSize).sample()
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
        var toAdd = this.stringArbitrary.sample()
        while (this.map[toAdd] != null) {
            toAdd = this.stringArbitrary.sample()
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
    fun point01Adds(spec: PersistentMapAddSpec): PersistentMap<String, Boolean> {
        return spec.map.put(spec.lookup, true)
    }

    @Benchmark
    fun point02Updates(spec: PersistentMapRunSpec): PersistentMap<String, Boolean> {
        return spec.map.put(spec.lookup, false)
    }

    @Benchmark
    fun point03Deletes(spec: PersistentMapRunSpec): PersistentMap<String, Boolean> {
        return spec.map.remove(spec.lookup)
    }

    @Benchmark
    fun iterator00Full(spec: PersistentMapRunSpec): Map.Entry<String, Boolean>? {
        var lastEntry: Map.Entry<String, Boolean>? = null
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