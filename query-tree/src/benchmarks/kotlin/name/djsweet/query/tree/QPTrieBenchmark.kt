package name.djsweet.query.tree

import org.openjdk.jmh.annotations.*
import net.jqwik.api.*
import java.util.concurrent.TimeUnit

@State(Scope.Benchmark)
class QPTrieRunSpec {
    @Param("100", "200", "400", "1600", "6400", "16000", "64000")
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
        return this.byteArrayArbitrary.list().ofMinSize(this.treeSize).ofMaxSize(this.treeSize).sample()
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
