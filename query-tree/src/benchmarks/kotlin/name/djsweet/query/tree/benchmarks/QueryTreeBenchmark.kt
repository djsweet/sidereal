package name.djsweet.query.tree.benchmarks

import name.djsweet.query.tree.QPTrie
import name.djsweet.query.tree.QuerySetTree
import name.djsweet.query.tree.QuerySpec
import name.djsweet.query.tree.QueryTreeTest
import org.openjdk.jmh.annotations.*
import java.util.concurrent.TimeUnit

@State(Scope.Benchmark)
class QueryTreeRunSpec {
    @Param("100", "200", "400", "800", "1600")
    var queryCount: Int = 0

    val testWithArbitraries = QueryTreeTest()
    var queries = listOf<QuerySpec>()
    var lookup = QPTrie<ByteArray>()
    var queryTree = QuerySetTree<Int>()
    var resultArray = ArrayList<Int>()

    fun linearScanLookup(): Set<Int> {
        val seen = mutableSetOf<Int>()
        val lookup = this.lookup
        for ((index, query) in this.queries.withIndex()) {
            if (query.matchesData(lookup)) {
                seen.add(index)
            }
        }
        return seen
    }

    @Setup
    fun setup() {
        val (queries, data) = this.testWithArbitraries.queryTreeTestDataWithQueryListSize(
            this.queryCount,
            this.queryCount,
            false
        ).sample()
        this.queries = queries
        this.lookup = data.shuffled().first()
        var nextQueryTree = QuerySetTree<Int>()
        for ((index, query) in this.queries.withIndex()) {
            val (_, alteredTree) = nextQueryTree.addElementByQuery(query, index)
            nextQueryTree = alteredTree
            println("cardinality=${query.cardinality} query=${query}")
        }
        this.queryTree = nextQueryTree
        val matchSet = this.linearScanLookup()
        this.resultArray.ensureCapacity(matchSet.size)
        println("Expecting to match ${matchSet.size} ${if (matchSet.size != 1) { "queries" } else {"query" }}")
    }
}

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations=5)
@Measurement(iterations=30)
class QueryTreeBenchmark {
    @Benchmark
    fun point00LookupLinearScan(spec: QueryTreeRunSpec): Set<Int> {
        return spec.linearScanLookup()
    }

    @Benchmark
    fun point01LookupUsingTree(spec: QueryTreeRunSpec): Set<Int> {
        val seen = mutableSetOf<Int>()
        for ((_, index) in spec.queryTree.getByData(spec.lookup)) {
            seen.add(index)
        }
        return seen
    }

    @Benchmark
    fun point02VisitUsingTree(spec: QueryTreeRunSpec): Set<Int> {
        val seen = mutableSetOf<Int>()
        spec.queryTree.visitByData(spec.lookup) { _, value ->
            seen.add(value)
        }
        return seen
    }

    @Benchmark
    fun point03VisitUsingTreeNoSet(spec: QueryTreeRunSpec): Int {
        var lastSeen: Int = -1
        spec.queryTree.visitByData(spec.lookup) { _, value ->
            lastSeen = value
        }
        return lastSeen
    }

    @Benchmark
    fun point04VisitUsingTreeIntoArray(spec: QueryTreeRunSpec): Int {
        val into = spec.resultArray
        into.clear()
        spec.queryTree.visitByData(spec.lookup) { _, value ->
            into.add(value)
        }
        return into[into.size - 1]
    }
}