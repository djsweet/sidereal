package name.djsweet.query.tree

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

    @Setup
    fun setup() {
        val (queries, data) = this.testWithArbitraries.queryTreeTestDataWithQueryListSize(
            this.queryCount,
            this.queryCount
        ).sample()
        this.queries = queries
        this.lookup = data.shuffled().first()
        var nextQueryTree = QuerySetTree<Int>()
        for ((index, query) in this.queries.withIndex()) {
            val (_, alteredTree) = nextQueryTree.addElementByQuery(query, index)
            nextQueryTree = alteredTree
        }
        this.queryTree = nextQueryTree
    }
}

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations=5)
@Measurement(iterations=30)
class QueryTreeBenchmark {
    @Benchmark
    fun point00LookupLinearScan(spec: QueryTreeRunSpec): Set<Int> {
        val seen = mutableSetOf<Int>()
        val lookup = spec.lookup
        for ((index, query) in spec.queries.withIndex()) {
            if (query.matchesData(lookup)) {
                seen.add(index)
            }
        }
        return seen
    }

    @Benchmark
    fun point01LookupUsingTree(spec: QueryTreeRunSpec): Set<Int> {
        val seen = mutableSetOf<Int>()
        for ((_, index) in spec.queryTree.getByData(spec.lookup)) {
            seen.add(index)
        }
        return seen
    }
}