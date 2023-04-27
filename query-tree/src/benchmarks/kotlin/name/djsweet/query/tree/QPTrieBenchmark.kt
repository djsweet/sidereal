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

    @Setup(Level.Iteration)
    fun setup() {
        var nextTrie = QPTrie<Boolean>()
        val entries = this.byteArrayArbitrary.list().ofMinSize(this.treeSize).ofMaxSize(this.treeSize).sample()
        for (entry in entries) {
            nextTrie = nextTrie.put(entry, true)
        }
        this.trie = nextTrie
        this.lookup = entries.shuffled().first()
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
    fun point01Updates(spec: QPTrieRunSpec): QPTrie<Boolean> {
        return spec.trie.update(spec.lookup) {
            if (it != true) {
                throw Error("Could not find lookup in QPTrie")
            }
            false
        }
    }

    @Benchmark
    fun point02Deletes(spec: QPTrieRunSpec): QPTrie<Boolean> {
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
