package name.djsweet.query.tree

import kotlinx.collections.immutable.PersistentList
import kotlinx.collections.immutable.persistentListOf
import kotlinx.collections.immutable.toPersistentList
import org.junit.jupiter.api.Test

import org.junit.jupiter.api.Assertions.*
import net.jqwik.api.*
import java.util.*
import kotlin.collections.ArrayList

class QueryTreeTest {
    companion object {
        private fun launderIntermediateQueryTermsToPublic(
            spec: Triple<QuerySpec, ArrayList<IntermediateQueryTerm>, IntermediateQueryTerm?>
        ): Triple<QuerySpec, ArrayList<PublicIntermediateQueryTerm>, PublicIntermediateQueryTerm?> {
            val (querySpec, equalityTermsInternal, inequalityTermInternal) = spec
            val equalityTerms = ArrayList(equalityTermsInternal.map { PublicIntermediateQueryTerm(it) })
            val inequalityTerm = if (inequalityTermInternal != null) {
                PublicIntermediateQueryTerm(inequalityTermInternal)
            } else {
                null
            }
            return Triple(querySpec, equalityTerms, inequalityTerm)
        }

        private fun unLaunderIntermediateQueryTermsFromPublic(
            spec: Triple<QuerySpec, ArrayList<PublicIntermediateQueryTerm>, PublicIntermediateQueryTerm?>
        ): Triple<QuerySpec, ArrayList<IntermediateQueryTerm>, IntermediateQueryTerm?> {
            val (querySpec, equalityTermsPublic, inequalityTermPublic) = spec
            val equalityTerms = ArrayList(equalityTermsPublic.map { it.actual })
            val inequalityTerm = inequalityTermPublic?.actual
            return Triple(querySpec, equalityTerms, inequalityTerm)
        }

        private fun enforceRangeInvariantsByteArray(range: Pair<ByteArray, ByteArray>): Pair<ByteArray, ByteArray> {
            val rangeCompare = Arrays.compareUnsigned(range.first, range.second)
            return if (rangeCompare > 0) {
                Pair(range.second, range.first)
            } else {
                range
            }
        }

        private fun <T>listOfArbitrariesToArbitraryOfList(arbs: List<Arbitrary<T>>): Arbitrary<List<T>> {
            return Arbitraries.just(arbs.map { it.sample() })
        }

        private fun <T>flattenList(lists: List<List<T>>): List<T> {
            val arrayList = arrayListOf<T>()
            for (list in lists) {
                arrayList.addAll(list)
            }
            return arrayList.toList()
        }

        private fun <T>concatList(left: List<T>, right: List<T>): List<T> {
            val arrayList = arrayListOf<T>()
            arrayList.addAll(left)
            arrayList.addAll(right)
            return arrayList.toList()
        }

        private fun uniqueQuerySpecs(querySpecs: List<QuerySpec>): List<QuerySpec> {
            var result = persistentListOf<QuerySpec>()
            for (querySpec in querySpecs) {
                if (!result.none { it == querySpec }) {
                    continue
                }
                result = result.add(0, querySpec)
            }
            return result
        }

        private fun<T> findIndex(arr: ArrayList<T>, needle: T): Int {
            for (i in arr.indices) {
                if (arr[i]?.equals(needle) == true) {
                    return i
                }
            }
            return -1
        }

        private const val keySize = 24
        private const val valueSize = 10
        private const val keySpaceSize = 16
    }

    class PublicIntermediateQueryTerm internal constructor(internal val actual: IntermediateQueryTerm)

    @Test
    fun intermediateQueryTermConvenienceConstructors() {
        val key = byteArrayOf(7)
        val valueSingular = byteArrayOf(8)
        val valueUpper = byteArrayOf(9)

        val equalityTerm = IntermediateQueryTerm.equalityTerm(key, valueSingular)
        assertEquals(IntermediateQueryTermKind.EQUALS, equalityTerm.kind)
        assertArrayEquals(key, equalityTerm.key)
        assertArrayEquals(valueSingular, equalityTerm.lowerBound)
        assertNull(equalityTerm.upperBound)

        val lessTerm = IntermediateQueryTerm.greaterOrLessTerm(key, null, valueUpper)
        assertEquals(IntermediateQueryTermKind.GREATER_OR_LESS, lessTerm.kind)
        assertArrayEquals(key, lessTerm.key)
        assertNull(lessTerm.lowerBound)
        assertArrayEquals(valueUpper, lessTerm.upperBound)

        val rangeTerm = IntermediateQueryTerm.greaterOrLessTerm(key, valueSingular, valueUpper)
        assertEquals(IntermediateQueryTermKind.GREATER_OR_LESS, rangeTerm.kind)
        assertArrayEquals(key, rangeTerm.key)
        assertArrayEquals(valueSingular, rangeTerm.lowerBound)
        assertArrayEquals(valueUpper, rangeTerm.upperBound)

        val greaterTerm = IntermediateQueryTerm.greaterOrLessTerm(key, valueSingular, null)
        assertEquals(IntermediateQueryTermKind.GREATER_OR_LESS, greaterTerm.kind)
        assertArrayEquals(key, greaterTerm.key)
        assertArrayEquals(valueSingular, greaterTerm.lowerBound)
        assertNull(greaterTerm.upperBound)

        val startsWithTerm = IntermediateQueryTerm.startsWithTerm(key, valueSingular)
        assertEquals(IntermediateQueryTermKind.STARTS_WITH, startsWithTerm.kind)
        assertArrayEquals(key, startsWithTerm.key)
        assertArrayEquals(valueSingular, startsWithTerm.lowerBound)
        assertNull(startsWithTerm.upperBound)
    }

    private fun byteArray(maxSize: Int): Arbitrary<ByteArray> {
        return Arbitraries.bytes().array(ByteArray::class.java).ofMaxSize(maxSize)
    }

    private fun generateLessThanTerm(key: ByteArray, existingSpec: QuerySpec): Arbitrary<Pair<QuerySpec, IntermediateQueryTerm>> {
        return this.byteArray(valueSize).map { value ->
            Pair(
                existingSpec.withLessThanOrEqualToTerm(key, value),
                IntermediateQueryTerm.greaterOrLessTerm(key, null, value)
            )
        }
    }

    private fun generateRangeTerm(key: ByteArray, existingSpec: QuerySpec): Arbitrary<Pair<QuerySpec, IntermediateQueryTerm>> {
        return this.byteArray(valueSize).flatMap { maybeLowerBound ->
            this.byteArray(valueSize).map { maybeUpperBound ->
                val (lowerBound, upperBound) = enforceRangeInvariantsByteArray(Pair(maybeLowerBound, maybeUpperBound))
                Pair(
                    existingSpec.withBetweenOrEqualToTerm(key, lowerBound, upperBound),
                    IntermediateQueryTerm.greaterOrLessTerm(key, lowerBound, upperBound)
                )
            }
        }
    }

    private fun generateGreaterThanTerm(key: ByteArray, existingSpec: QuerySpec): Arbitrary<Pair<QuerySpec, IntermediateQueryTerm>> {
        return this.byteArray(valueSize).map { value ->
            Pair(
                existingSpec.withGreaterThanOrEqualToTerm(key, value),
                IntermediateQueryTerm.greaterOrLessTerm(key, value, null)
            )
        }
    }

    private fun generateStartsWithTerm(key: ByteArray, existingSpec: QuerySpec): Arbitrary<Pair<QuerySpec, IntermediateQueryTerm>> {
        return this.byteArray(valueSize).map {value ->
            Pair(
                existingSpec.withStartsWithTerm(key, value),
                IntermediateQueryTerm.startsWithTerm(key, value)
            )
        }
    }

    private fun generateInequalityQuery(key: ByteArray, existingSpec: QuerySpec): Arbitrary<Pair<QuerySpec, IntermediateQueryTerm>> {
        return Arbitraries.oneOf(listOf(
            this.generateLessThanTerm(key, existingSpec),
            this.generateRangeTerm(key, existingSpec),
            this.generateGreaterThanTerm(key, existingSpec),
            this.generateStartsWithTerm(key, existingSpec)
        ))
    }

    private fun generateEqualityQuery(key: ByteArray, existingSpec: QuerySpec): Arbitrary<Pair<QuerySpec, IntermediateQueryTerm>> {
        return this.byteArray(valueSize).map { value ->
            Pair(
                existingSpec.withEqualityTerm(key, value),
                IntermediateQueryTerm.equalityTerm(key, value)
            )
        }
    }

    private fun querySpecNoInequality(keySpace: List<ByteArray>): Arbitrary<Triple<QuerySpec, ArrayList<IntermediateQueryTerm>, IntermediateQueryTerm?>> {
        // We don't need to build out a huge query spec from the entirety of the key space.
        // 1. That's pretty unrealistic
        // 2. That's hella slow in jqwik.
        val subsetSize = Arbitraries.integers().between(0, keySpace.size).sample()
        val subset = keySpace.shuffled().subList(0, subsetSize)
        if (subset.isEmpty()) {
            return Arbitraries.just(Triple(QuerySpec.empty, arrayListOf(), null))
        }

        val firstQuery = this.generateEqualityQuery(subset.last(), QuerySpec.empty).map { (querySpec, term) ->
            Triple(querySpec, persistentListOf(term), null as IntermediateQueryTerm?)
        }.sample()
        val (querySpec, reverseTerms, inequality) = subset.subList(1, subset.size).foldRight(firstQuery) { key, (querySpec, terms, inequality) ->
            val (newQuerySpec, newTerm) = this.generateEqualityQuery(key, querySpec).sample()
            Triple(newQuerySpec, terms.add(0, newTerm), inequality)
        }
        return Arbitraries.just(Triple(querySpec, ArrayList(reverseTerms.reversed()), inequality))
    }

    private fun querySpecInequality(keySpace: List<ByteArray>): Arbitrary<Triple<QuerySpec, ArrayList<IntermediateQueryTerm>, IntermediateQueryTerm?>> {
        val subSpace = keySpace.subList(0, keySpace.size - 1)
        val key = keySpace.last()
        return this.querySpecNoInequality(subSpace).flatMap { (querySpec, terms, _) ->
            this.generateInequalityQuery(key, querySpec).map { (newQuerySpec, inequalityTerm)  ->
                Triple(newQuerySpec, terms, inequalityTerm)
            }
        }
    }

    private fun querySpecJustInequality(keySpace: List<ByteArray>): Arbitrary<Triple<QuerySpec, ArrayList<IntermediateQueryTerm>, IntermediateQueryTerm?>> {
        val key = keySpace.shuffled().first()
        return this.generateInequalityQuery(key, QuerySpec.empty).map {(querySpec, inequalityTerm) ->
            Triple(querySpec, arrayListOf(), inequalityTerm)
        }
    }

    private fun querySpecWithKeySpace(
        keySpace: List<ByteArray>
    ): Arbitrary<Triple<QuerySpec, ArrayList<PublicIntermediateQueryTerm>, PublicIntermediateQueryTerm?>> {
        return Arbitraries.oneOf(
            listOf(
                this.querySpecNoInequality(keySpace),
                this.querySpecInequality(keySpace),
                this.querySpecJustInequality(keySpace),
            )
        ).map { launderIntermediateQueryTermsToPublic(it) }
    }

    @Provide
    fun querySpecMeta(): Arbitrary<Triple<QuerySpec, ArrayList<PublicIntermediateQueryTerm>, PublicIntermediateQueryTerm?>> {
        return this.byteArray(keySize).list().ofMinSize(1).ofMaxSize(keySpaceSize).flatMap { keySpace ->
            this.querySpecWithKeySpace(keySpace)
        }
    }

    @Property
    fun buildingQuerySpecs(
        @ForAll @From("querySpecMeta") publicSpec: Triple<QuerySpec, ArrayList<PublicIntermediateQueryTerm>, PublicIntermediateQueryTerm?>
    ) {
        val (querySpec, equalityTerms, maybeInequalityTerm) = unLaunderIntermediateQueryTermsFromPublic(publicSpec)
        var equalityTrie = QPTrie(equalityTerms.map { term -> Pair(term.key, term.lowerBound!!) })
        if (maybeInequalityTerm != null) {
            equalityTrie = equalityTrie.remove(maybeInequalityTerm.key)
            assertFalse(querySpec.hasEqualityTerm(maybeInequalityTerm.key))
            val qsit = querySpec.inequalityTerm
            assertNotNull(qsit)
            assertArrayEquals(maybeInequalityTerm.key, qsit!!.key)
            assertEquals(maybeInequalityTerm.kind, qsit.kind)
            assertArrayEquals(maybeInequalityTerm.lowerBound, qsit.lowerBound)
            assertArrayEquals(maybeInequalityTerm.upperBound, qsit.upperBound)
        } else {
            assertNull(querySpec.inequalityTerm)
        }
        val expectedEqualityTerms = ArrayList<Pair<ByteArrayButComparable, ByteArrayButComparable>>()
        for ((key, value) in equalityTrie) {
            expectedEqualityTerms.add(Pair(ByteArrayButComparable(key), ByteArrayButComparable(value)))
        }
        val givenEqualityTerms = ArrayList<Pair<ByteArrayButComparable, ByteArrayButComparable>>()
        for ((key, value) in querySpec.equalityTerms) {
            givenEqualityTerms.add(Pair(ByteArrayButComparable(key), ByteArrayButComparable(value)))
        }
        assertListOfByteArrayValuePairsEquals(expectedEqualityTerms, givenEqualityTerms)
        if (querySpec.inequalityTerm == null) {
            assertEquals(querySpec.equalityTerms.size, querySpec.cardinality.toLong())
        } else {
            assertEquals(querySpec.equalityTerms.size + 1, querySpec.cardinality.toLong())
        }

        // This isn't much of a test, but it does get coverage out of the way.
        assertDoesNotThrow { querySpec.toString() }

        var removingQuerySpec = querySpec
        while (expectedEqualityTerms.size > 0) {
            val removeThis = expectedEqualityTerms.first()
            val (removeKey, _) = removeThis
            expectedEqualityTerms.removeAt(0)
            removingQuerySpec = removingQuerySpec.withoutEqualityTerm(removeKey.array)
            assertFalse(removingQuerySpec.hasEqualityTerm(removeKey.array))
            assertNotEquals(removingQuerySpec, querySpec)
            givenEqualityTerms.clear()
            for ((keepKey, value) in removingQuerySpec.equalityTerms) {
                givenEqualityTerms.add(Pair(ByteArrayButComparable(keepKey), ByteArrayButComparable(value)))
                assertTrue(removingQuerySpec.hasEqualityTerm(keepKey))
            }
            assertListOfByteArrayValuePairsEquals(expectedEqualityTerms, givenEqualityTerms)

            val removedAgain = removingQuerySpec.withoutEqualityTerm(removeKey.array)
            assertTrue(removedAgain === removingQuerySpec)
        }

        // Again, not much of a test, but it ensures full line coverage.
        assertFalse(querySpec.equals(1))
    }

    private fun intermediateQueryTermForEquality(): Arbitrary<IntermediateQueryTerm> {
        return this.byteArray(keySize).flatMap { key ->
            this.byteArray(valueSize).map { value ->
                IntermediateQueryTerm.equalityTerm(key, value)
            }
        }
    }

    private fun intermediateQueryTermForLessThan(): Arbitrary<IntermediateQueryTerm> {
        return this.byteArray(keySize).flatMap { key ->
            this.byteArray(valueSize).map { value ->
                IntermediateQueryTerm.greaterOrLessTerm(key, null, value)
            }
        }
    }

    private fun intermediateQueryTermForRange(): Arbitrary<IntermediateQueryTerm> {
        return this.byteArray(keySize).flatMap { key ->
            this.byteArray(valueSize).flatMap { maybeLowerBound ->
                this.byteArray(valueSize).map { maybeUpperBound ->
                    val (lowerBound, upperBound) = enforceRangeInvariantsByteArray(Pair(maybeLowerBound, maybeUpperBound))
                    IntermediateQueryTerm.greaterOrLessTerm(key, lowerBound, upperBound)
                }
            }
        }
    }

    private fun intermediateQueryTermForGreaterThan(): Arbitrary<IntermediateQueryTerm> {
        return this.byteArray(keySize).flatMap { key ->
            this.byteArray(valueSize).map { value ->
                IntermediateQueryTerm.greaterOrLessTerm(key, value, null)
            }
        }
    }

    private fun intermediateQueryTermForStartsWith(): Arbitrary<IntermediateQueryTerm> {
        return this.byteArray(keySize).flatMap { key ->
            this.byteArray(valueSize).map { value ->
                IntermediateQueryTerm.startsWithTerm(key, value)
            }
        }
    }

    private fun intermediateQueryTermForInequality(): Arbitrary<IntermediateQueryTerm> {
        return Arbitraries.oneOf(listOf(
            this.intermediateQueryTermForLessThan(),
            this.intermediateQueryTermForRange(),
            this.intermediateQueryTermForGreaterThan(),
            this.intermediateQueryTermForStartsWith()
        ))
    }

    @Provide
    fun intermediateQueryTerms(): Arbitrary<PersistentList<PublicIntermediateQueryTerm>> {
        return Arbitraries.oneOf(listOf(
            this.intermediateQueryTermForEquality().list(),
            this.intermediateQueryTermForEquality().list().flatMap { terms ->
                this.intermediateQueryTermForInequality().map { inequalityTerm ->
                    terms.add(terms.size, inequalityTerm)
                    terms
                }
            }
        )).map { terms -> terms.map { PublicIntermediateQueryTerm(it) } .toPersistentList() }
    }

    @Property
    fun queryPathConstructorSameAsPrepend(
        @ForAll @From("intermediateQueryTerms") publicQueryTerms: PersistentList<PublicIntermediateQueryTerm>
    ) {
        val queryTerms = publicQueryTerms.map { it.actual }.toList()
        val oneShotPath = QueryPath(queryTerms.reversed())
        var prependPath = QueryPath(persistentListOf())
        for (term in queryTerms) {
            prependPath = prependPath.prepend(term)
        }

        assertEquals(oneShotPath.breadcrumbs, prependPath.breadcrumbs)
    }

    @Property
    fun queryPathFirstAndSubLists(
        @ForAll @From("intermediateQueryTerms") publicQueryTerms: PersistentList<PublicIntermediateQueryTerm>
    ) {
        var queryTerms = publicQueryTerms.map { it.actual }.toPersistentList()
        var queryPath = QueryPath(queryTerms)
        var pathSize = listSize(queryPath.breadcrumbs)
        while (queryTerms.size > 0 && pathSize > 0) {
            assertEquals(queryTerms.size, pathSize)
            assertEquals(queryTerms.first(), queryPath.first())
            queryTerms = queryTerms.subList(1, queryTerms.size).toPersistentList()
            queryPath = queryPath.rest()
            pathSize = listSize(queryPath.breadcrumbs)
        }
    }

    @Property
    fun queryPathKeys(
        @ForAll @From("intermediateQueryTerms") publicQueryTerms: PersistentList<PublicIntermediateQueryTerm>
    ) {
        var unseenKeys = QPTrie<Int>()
        // Note that the QueryPath constructor doesn't perform any dedpulication with keys,
        // so we have to test by way of keeping reference counts.
        for (item in publicQueryTerms) {
            val key = item.actual.key
            unseenKeys = unseenKeys.update(key) { (it ?: 0) + 1 }
        }

        val queryPath = QueryPath(publicQueryTerms.map { it.actual })
        for (key in queryPath.keys()) {
            assertNotNull(unseenKeys.get(key))
            unseenKeys = unseenKeys.update(key) { if (it == null || it <= 1) { null } else { it - 1 } }
        }

        assertEquals(0, unseenKeys.size)
    }

    private fun arbitraryDataForKeySpace(keySpace: List<ByteArray>): Arbitrary<QPTrie<ByteArray>> {
        val empty = QPTrie<ByteArray>()
        if (keySpace.isEmpty()) {
            return Arbitraries.just(empty)
        }

        val firstValue = this.byteArray(valueSize).sample()
        val basis = empty.put(keySpace.first(), firstValue)
        return Arbitraries.just(keySpace.subList(1, keySpace.size).fold(basis) { acc, key ->
            val nextValue = this.byteArray(valueSize).sample()
            acc.put(key, nextValue)
        })
    }

    private fun termValueLessThanOrEqualTo(basis: ByteArray): Arbitrary<ByteArray> {
        if (basis.isEmpty()) {
            return Arbitraries.just(basis.clone())
        }
        // This is, admittedly, incredibly cheap, but the QPTrie tests should
        // be robust enough for us to get away with doing this.
        return Arbitraries.oneOf(listOf(
            Arbitraries.just(basis.clone()),
            Arbitraries.just(basis.slice(0 until basis.size - 1).toByteArray())
        ))
    }

    private fun termValueGreaterThanOrEqualTo(basis: ByteArray): Arbitrary<ByteArray> {
        if (basis.isEmpty()) {
            return Arbitraries.just(basis.clone())
        }
        val greater = ByteArray(basis.size + 1) { 0 }
        basis.copyInto(greater)
        // This is, admittedly, incredibly cheap, but the QPTrie tests should
        // be robust enough for us to get away with doing this.
        return Arbitraries.oneOf(listOf(
            Arbitraries.just(basis.clone()),
            Arbitraries.just(greater)
        ))
    }

    private fun termValueBetweenOrEqualTo(lowerBound: ByteArray, upperBound: ByteArray): Arbitrary<ByteArray> {
        if (lowerBound.contentEquals(upperBound)) {
            return Arbitraries.just(lowerBound.clone())
        }
        // An invariant we already have is that lowerBound <= upperBound.
        // Above, we handle the case where they're equal.
        // The lowest possible greater bound is the prefix of lowerBound with the lowest byte,
        // which could actually be equal to upperBound, but that's still fully permissible.
        val barelyGreater = ByteArray(lowerBound.size + 1) { -128 }
        lowerBound.copyInto(barelyGreater)
        return Arbitraries.oneOf(listOf(
            Arbitraries.just(lowerBound.clone()),
            Arbitraries.just(upperBound.clone()),
            Arbitraries.just(barelyGreater)
        ))
    }

    private fun basedDataForIntermediateQueryTerm(queryTerm: IntermediateQueryTerm): Arbitrary<Pair<ByteArray, ByteArray>> {
        return when (queryTerm.kind) {
            IntermediateQueryTermKind.EQUALS -> Arbitraries.just(Pair(queryTerm.key, queryTerm.lowerBound!!))
            IntermediateQueryTermKind.STARTS_WITH -> this.byteArray(
                valueSize - queryTerm.lowerBound!!.size
            ).map { suffix ->
                val fullByteArray = ByteArray(queryTerm.lowerBound.size + suffix.size)
                queryTerm.lowerBound.copyInto(fullByteArray)
                suffix.copyInto(fullByteArray, queryTerm.lowerBound.size)
                Pair(queryTerm.key, fullByteArray)
            }
            IntermediateQueryTermKind.GREATER_OR_LESS -> (if (queryTerm.lowerBound != null && queryTerm.upperBound != null) {
                this.termValueBetweenOrEqualTo(queryTerm.lowerBound, queryTerm.upperBound)
            } else if (queryTerm.lowerBound != null) {
                this.termValueGreaterThanOrEqualTo(queryTerm.lowerBound)
            } else {
                this.termValueLessThanOrEqualTo(queryTerm.upperBound!!)
            }).map { Pair(queryTerm.key, it) }
        }
    }

    private fun fillTrieWithArbitraryData(
        basisTrie: QPTrie<ByteArray>,
        keySpace: List<ByteArray>,
        seenKeys: QPTrie<Boolean>
    ): Arbitrary<QPTrie<ByteArray>> {
        val keySubSpace = keySpace.filter { seenKeys.get(it) == null }
        return this.arbitraryDataForKeySpace(keySubSpace).map { subTrie ->
            var result = basisTrie
            for ((key, value) in subTrie) {
                result = result.put(key, value)
            }
            result
        }
    }

    private fun basedDataForQueryPath(
        queryPath: QueryPath,
        keySpace: List<ByteArray>
    ): Arbitrary<List<QPTrie<ByteArray>>> {
        val breadcrumbs = queryPath.breadcrumbs ?: return Arbitraries.just(listOf())

        val fillTrieLift = { term: IntermediateQueryTerm, workingTrie: QPTrie<ByteArray>, seenKeys: QPTrie<Boolean>, pastList: PersistentList<QPTrie<ByteArray>>  ->
            val (key, value) = this.basedDataForIntermediateQueryTerm(term).sample()
            val nextWorkingTrie = workingTrie.put(key, value)
            val nextSeenKeys = seenKeys.put(key, true)
            val filled = this.fillTrieWithArbitraryData(nextWorkingTrie, keySpace, nextSeenKeys).sample()
            Triple(pastList.add(0, filled), nextWorkingTrie, nextSeenKeys)
        }
        val (firstData, firstTerm) = this.basedDataForIntermediateQueryTerm(listFirst(breadcrumbs)!!).sample()
        val firstTrie = QPTrie(listOf(firstData to firstTerm))
        val firstFilled = this.fillTrieWithArbitraryData(
            firstTrie,
            keySpace,
            QPTrie(listOf(firstData to true)),
        ).sample()
        val basis = Triple(persistentListOf(firstFilled), firstTrie, QPTrie<Boolean>())
        val (entries) = listIterator(listRest(breadcrumbs)).asSequence().fold(basis) { acc, term ->
            val (pastList, workingTrie, seenKeys) = acc
            Arbitraries.frequency<Triple<PersistentList<QPTrie<ByteArray>>, QPTrie<ByteArray>, QPTrie<Boolean>>>(
                Tuple.of(4, acc),
                Tuple.of(1, fillTrieLift(term, workingTrie, seenKeys, pastList))
            ).sample()
        }
        return Arbitraries.just(entries.toList())
    }

    private fun basedDataForQuerySpec(
        querySpec: QuerySpec,
        keySpace: List<ByteArray>
    ): Arbitrary<List<QPTrie<ByteArray>>> {
        val equalityTerms = querySpec.equalityTerms.map {
            (key, value) -> IntermediateQueryTerm.equalityTerm(key, value)
        }
        val inequalityPortion = if (querySpec.inequalityTerm == null) {
            listOf()
        } else {
            listOf(querySpec.inequalityTerm)
        }
        return this.basedDataForQueryPath(QueryPath(concatList(equalityTerms, inequalityPortion)), keySpace)
    }

    data class QueryTreeTestData(val queries: List<QuerySpec>, val data: List<QPTrie<ByteArray>>) {
        override fun toString(): String {
            val data = this.data.map {
                trie -> trie.iterator().asSequence().map {
                    (key, value) -> Pair(ByteArrayButComparable(key), ByteArrayButComparable(value))
                }.toList()
            }
            return "QueryTreeTestData(queries=${this.queries}, data=${data})"
        }
    }

    fun queryTreeTestDataWithQueryListSize(minSize: Int, maxSize: Int, unique: Boolean): Arbitrary<QueryTreeTestData> {
        return this.byteArray(keySize).list().ofMinSize(1).ofMaxSize(keySpaceSize).flatMap { keySpace ->
            val listArbitrary = this.querySpecWithKeySpace(keySpace).map {
                    (queries) -> queries
            }.list()

            val queryList = if (minSize == maxSize) {
                listArbitrary.ofSize(minSize)
            } else {
                listArbitrary.ofMinSize(minSize).ofMaxSize(maxSize)
            }
            queryList.flatMap { queries ->
                val basedData = listOfArbitrariesToArbitraryOfList(queries.map {
                    this.basedDataForQuerySpec(it, keySpace)
                }).map { flattenList(it) }
                val arbitraryData = arbitraryDataForKeySpace(keySpace).list().ofMaxSize(128)

                val basedDataSample = basedData.sample()
                val arbitraryDataSample = arbitraryData.sample()
                val dataList = concatList(basedDataSample, arbitraryDataSample)
                Arbitraries.just(
                    QueryTreeTestData(
                        if (unique) { uniqueQuerySpecs(queries) } else { queries },
                        dataList
                    )
                )
            }
        }
    }

    @Provide
    fun queryTreeTestData(): Arbitrary<QueryTreeTestData> {
        return this.queryTreeTestDataWithQueryListSize(1, 24, true)
    }

    private data class SizeComputableInteger(val value: Int, val weight: Int): SizeComputable {
        override fun computeSize(): Long {
            return this.weight.toLong()
        }
    }

    private fun verifyQueriesNonSet(
        data: List<QPTrie<ByteArray>>,
        queries: List<QuerySpec>,
        handles: ArrayList<SizeComputableInteger>,
        paths: ArrayList<QueryPath>,
        queryTree: QueryTree<SizeComputableInteger>
    ) {
        for (dataEntry in data) {
            val expectedResults = queries.mapIndexed {
                    idx, q -> Pair(idx, q)
            }.filter {
                    (_, q) -> q.matchesData(dataEntry)
            }.map {
                    (idx) -> handles[idx]
            }.toTypedArray()
            var givenResults = persistentListOf<SizeComputableInteger>()
            for ((path, result) in queryTree.getByData(dataEntry)) {
                assertDoesNotThrow { path.toString() }
                assertEquals(path.hashCode(), listHashCode(path.breadcrumbs))
                if (path.breadcrumbs == null) {
                    assertEquals(1, path.hashCode())
                } else {
                    assertTrue(path.hashCode() != 1)
                }
                assertFalse(path.equals(1)) // Only for super.equal() checks.
                val pathIdx = findIndex(paths, path)
                if (result != handles[pathIdx]) {
                    println("Expected to find ${handles[pathIdx]}")
                    println("Found $result instead")

                    println("Data was ${dataEntry.map { ByteArrayButComparable(it.key) to ByteArrayButComparable(it.value) }}")
                    println("Path was $path")
                    println("Available options were:")
                    for (i in handles.indices) {
                        println("${paths[i]} -> ${handles[i]}")
                    }
                    println("And the tree has")
                    for ((knownPath, knownResult) in queryTree) {
                        println("$knownPath -> $knownResult")
                    }
                }
                assertEquals(result, handles[pathIdx])
                givenResults = givenResults.add(0, result)
            }
            val givenResultsArray = givenResults.sortedBy { it.value }.toTypedArray()
            if (expectedResults.size != givenResultsArray.size) {
                println("data was ${dataEntry.toList().map { (key, value) -> Pair(ByteArrayButComparable(key), ByteArrayButComparable(value)) }}")
                for (entry in expectedResults) {
                    val query = queries[findIndex(handles, entry)]
                    println("Expected to service query $query")
                }
                for (entry in givenResultsArray) {
                    val query = queries[findIndex(handles, entry)]
                    println("Actually serviced query $query")
                }
                for((path, value) in queryTree) {
                    println("$path=$value")
                }
            }
            assertArrayEquals(expectedResults, givenResultsArray)

            val visitedResultsArray = ArrayList<SizeComputableInteger>()
            queryTree.visitByData(dataEntry) { (path, result) ->
                val pathIdx = findIndex(paths, path)
                assertEquals(result, handles[pathIdx])
                visitedResultsArray.add(result)
            }
            visitedResultsArray.sortBy { it.value }
            if (!expectedResults.contentEquals(visitedResultsArray.toTypedArray())) {
                println("data was ${dataEntry.toList().map { (key, value) -> Pair(ByteArrayButComparable(key), ByteArrayButComparable(value)) }}")
                for (entry in expectedResults) {
                    val query = queries[findIndex(handles, entry)]
                    println("Expected to service query $query")
                }
                for (entry in visitedResultsArray) {
                    val query = queries[findIndex(handles, entry)]
                    println("Actually serviced query $query")
                }
                for((path, value) in queryTree) {
                    println("$path=$value")
                }
            }
            assertArrayEquals(expectedResults, visitedResultsArray.toTypedArray())
        }

        val unseenPaths = paths.toMutableList()
        for ((path, value) in queryTree) {
            val indexOfPath = paths.indexOf(path)
            val handle = handles[indexOfPath]
            assertEquals(handle, value)
            unseenPaths.remove(path)
        }
        assertEquals(0, unseenPaths.size)
    }

    @Property(tries=200)
    fun queryTreeNonSet(
        @ForAll @From("queryTreeTestData") testData: QueryTreeTestData,
    ) {
        val weight = Arbitraries.integers().between(1, 12).sample()
        val (queries, data) = testData
        val paths = ArrayList<QueryPath>()
        val handles = ArrayList<SizeComputableInteger>()
        var querySerial = 0
        var queryTree = QueryTree<SizeComputableInteger>()
        assertEquals(0L, queryTree.size)
        for (query in queries) {
            val handle = SizeComputableInteger(querySerial, weight)
            val (path, nextTree) = queryTree.updateByQuery(query) { handle }
            paths.add(path)
            handles.add(handle)
            queryTree = nextTree
            querySerial += 1

            // This is getting handled below, but we want to make sure
            // that immediate inserts also work.
            val resultForPath = queryTree.getByPath(path)
            if (handle != resultForPath) {
                println("Wait, the path should have been $path=$handle")
                println("Instead it was $path=$resultForPath")
            }
            assertEquals(handle, resultForPath)
            assertEquals((querySerial * weight).toLong(), queryTree.size)
        }
        val fullTreeSize = (querySerial * weight).toLong()
        assertEquals(fullTreeSize, queryTree.size)

        assertEquals(queries.size, paths.size)
        for (i in queries.indices) {
            val resultForPath = queryTree.getByPath(paths[i])
            if (handles[i] != resultForPath) {
                println("Concerning the path ${paths[i]}")
                println("Expected ${handles[i]}")
                println("Got $resultForPath")
                for (idx in paths.indices) {
                    println("${paths[idx]} = ${handles[idx]}")
                }
            }
            assertEquals(handles[i], resultForPath)
        }

        this.verifyQueriesNonSet(data, queries, handles, paths, queryTree)

        for (i in handles.indices) {
            val path = paths[i]
            handles[i] = handles[i].copy(value=handles[i].value * 2)
            queryTree = queryTree.updateByPath(path) { handles[i] }

            val resultForPath = queryTree.getByPath(path)
            assertEquals(handles[i], resultForPath)
            assertEquals(fullTreeSize, queryTree.size)
        }

        this.verifyQueriesNonSet(data, queries, handles, paths, queryTree)

        var queryTreeFilledByPath = QueryTree<SizeComputableInteger>()
        for (i in paths.indices) {
            val path = paths[i]
            val handle = handles[i]
            queryTreeFilledByPath = queryTreeFilledByPath.updateByPath(path) { handle }

            val resultForPath = queryTreeFilledByPath.getByPath(path)
            assertEquals(handle, resultForPath)
        }

        assertEquals(fullTreeSize, queryTreeFilledByPath.size)

        this.verifyQueriesNonSet(data, queries, handles, paths, queryTreeFilledByPath)

        // This is a bit silly, but we should make sure that updating in such a way that
        // nothing changes results in the exact same value.
        for (i in paths.indices) {
            val path = paths[i]
            val handle = handles[i]

            val newTree = queryTree.updateByPath(path) { handle }
            assertTrue(newTree === queryTree)
        }

        val firstAttemptRemovalSize = Arbitraries.integers().between(1, handles.size).sample()
        for (i in 0 until firstAttemptRemovalSize) {
            val path = paths[i]
            val handle = handles[i]
            var priorHandle: SizeComputableInteger? = null
            // println("ok, remove $path ${queries[i]} for first run")
            queryTree = queryTree.updateByPath(path) {
                priorHandle = it
                null
            }

            val resultForPath = queryTree.getByPath(path)
            assertNull(resultForPath)
            assertEquals(handle, priorHandle)
            assertEquals(fullTreeSize - (i+1) * weight, queryTree.size)
        }
        for (i in firstAttemptRemovalSize until handles.size) {
            val path = paths[i]
            val handle = handles[i]
            val resultForPath = queryTree.getByPath(path)
            assertEquals(handle, resultForPath)
        }
        val afterFirstRemovalSize = fullTreeSize - firstAttemptRemovalSize * weight
        assertEquals(afterFirstRemovalSize, queryTree.size)

        this.verifyQueriesNonSet(
            data,
            queries.subList(firstAttemptRemovalSize, queries.size),
            ArrayList(handles.subList(firstAttemptRemovalSize, handles.size)),
            ArrayList(paths.subList(firstAttemptRemovalSize, paths.size)),
            queryTree
        )

        for (i in firstAttemptRemovalSize until handles.size) {
            val path = paths[i]
            val handle = handles[i]
            var priorHandle: SizeComputableInteger? = null
            queryTree = queryTree.updateByPath(path) {
                priorHandle = it
                null
            }

            val resultForPath = queryTree.getByPath(path)
            assertNull(resultForPath)
            assertEquals(handle, priorHandle)
            assertEquals(afterFirstRemovalSize - (i -firstAttemptRemovalSize + 1) * weight, queryTree.size)
        }
        for (i in 0 until handles.size) {
            val path = paths[i]
            val resultForPath = queryTree.getByPath(path)
            assertNull(resultForPath)
        }
        assertEquals(0L, queryTree.size)
    }

    @Test
    fun queryTreeJustLessThan() {
        val querySpec = QuerySpec.withLessThanOrEqualToTerm(byteArrayOf(12), byteArrayOf(13))
        val handle = SizeComputableInteger(1, 1)

        val (path, tree) = QueryTree<SizeComputableInteger>().updateByQuery(querySpec) { handle }
        assertEquals(1L, tree.size)
        // /*
        for ((treePath, value) in tree) {
            println("$treePath=$value")
            val byPath = tree.getByPath(treePath)
            println("$treePath=$byPath")
        }
        // */
        val gottenAgain = tree.getByPath(path)
        assertEquals(handle, gottenAgain)

        val byData = tree.getByData(QPTrie(listOf(byteArrayOf(12) to byteArrayOf(12)))).asSequence().toList()
        assertEquals(1, byData.size)
        assertEquals(handle, byData.first().value)
    }

    @Test
    fun queryTreeJustGreaterThan() {
        val querySpec = QuerySpec.withGreaterThanOrEqualToTerm(byteArrayOf(12), byteArrayOf(13))
        val handle = SizeComputableInteger(1, 1)

        val (path, tree) = QueryTree<SizeComputableInteger>().updateByQuery(querySpec) { handle }
        assertEquals(1L, tree.size)
        /*
        for ((treePath, value) in tree) {
            println("${treePath}=${value}")
        }
        // */
        val gottenAgain = tree.getByPath(path)
        assertEquals(handle, gottenAgain)

        val byData = tree.getByData(QPTrie(listOf(byteArrayOf(12) to byteArrayOf(14)))).asSequence().toList()
        assertEquals(1, byData.size)
        assertEquals(handle, byData.first().value)
    }

    data class QuerySetTreeTestData(val queries: List<Pair<QuerySpec, Int>>, val data: List<QPTrie<ByteArray>>)

    @Provide
    fun queryTreeTestDataForSetOperations(): Arbitrary<QuerySetTreeTestData> {
        return this.queryTreeTestData().flatMap { (queries, data) ->
            Arbitraries.just(
                QuerySetTreeTestData(
                    queries.map { it to Arbitraries.integers().between(1, 6).sample() },
                    data
                )
            )
        }
    }

    private fun verifyQueriesSet(
        data: List<QPTrie<ByteArray>>,
        queries: List<QuerySpec>,
        handles: ArrayList<Int>,
        paths: ArrayList<QueryPath>,
        queryTree: QuerySetTree<Int>
    ) {
        for (dataEntry in data) {
            val expectedResults = queries.mapIndexed { idx, q ->
                Pair(idx, q)
            }.filter { (_, q) ->
                q.matchesData(dataEntry)
            }.map { (idx) ->
                handles[idx]
            }.toSet()
            val givenResults = mutableSetOf<Int>()
            for ((path, result) in queryTree.getByData(dataEntry)) {
                var pathIdx = findIndex(paths, path)
                var foundIt = false
                while (paths[pathIdx] == path) {
                    if (result == handles[pathIdx]) {
                        foundIt = true
                        break
                    } else {
                        pathIdx ++
                    }
                }
                if (!foundIt) {
                    fail<String>("Could not find result for path $path -> $result")
                }
                givenResults.add(result)
            }
            if (expectedResults.size != givenResults.size) {
                println(
                    "data was ${
                        dataEntry.toList()
                            .map { (key, value) -> Pair(ByteArrayButComparable(key), ByteArrayButComparable(value)) }
                    }"
                )
                println("Size disparity: ${expectedResults.size} != ${givenResults.size}")
                for (entry in expectedResults) {
                    val query = queries[findIndex(handles, entry)]
                    println("Expected to service query $query")
                }
                for (entry in givenResults) {
                    val query = queries[findIndex(handles, entry)]
                    println("Actually serviced query $query")
                }
                for ((path, value) in queryTree) {
                    println("$path=$value")
                }
            }
            assertEquals(expectedResults, givenResults)

            val visitedResults = mutableSetOf<Int>()
            queryTree.visitByData(dataEntry) { path, result ->
                var pathIdx = findIndex(paths, path)
                var foundIt = false
                while (paths[pathIdx] == path) {
                    if (result == handles[pathIdx]) {
                        foundIt = true
                        break
                    } else {
                        pathIdx ++
                    }
                }
                if (!foundIt) {
                    fail<String>("Could not find result for path $path -> $result")
                }
                visitedResults.add(result)
            }
            if (expectedResults != visitedResults) {
                println(
                    "data was ${
                        dataEntry.toList()
                            .map { (key, value) -> Pair(ByteArrayButComparable(key), ByteArrayButComparable(value)) }
                    }"
                )
                for (entry in expectedResults) {
                    val query = queries[findIndex(handles, entry)]
                    println("Expected to service query $query")
                }
                for (entry in visitedResults) {
                    val query = queries[findIndex(handles, entry)]
                    println("Actually serviced query $query")
                }
                for ((path, value) in queryTree) {
                    println("$path=$value")
                }
            }
            assertEquals(expectedResults, visitedResults)
        }

        // Needs to be a multi-set
        val unseenPaths = paths.toMutableList()
        for ((path, value) in queryTree) {
            var pathIdx = paths.indexOf(path)
            var foundIt = false
            while (paths[pathIdx] == path) {
                if (value == handles[pathIdx]) {
                    unseenPaths.remove(path)
                    foundIt = true
                    break
                } else {
                    pathIdx++
                }
            }
            if (!foundIt) {
                for (i in 0 until paths.size) {
                    val knownPath = paths[i]
                    val handle = handles[i]
                    println("Known path config $knownPath -> $handle")
                }
                fail<String>("Could not find path $path to remove for $value")
            }
        }
        assertEquals(0, unseenPaths.size)
    }

    @Property(tries=200)
    fun queryTreeSet(
        @ForAll @From("queryTreeTestDataForSetOperations") testData: QuerySetTreeTestData
    ) {
        val (queriesAndCounts, data) = testData
        val paths = ArrayList<QueryPath>()
        val handles = ArrayList<Int>()
        var querySerial = 0
        var queryTree = QuerySetTree<Int>()
        var lastCount = 0
        val queries = mutableListOf<QuerySpec>()
        for ((query, count) in queriesAndCounts) {
            val handlesForQuery = mutableSetOf<Int>()
            for (i in 0 until count) {
                val handle = querySerial
                // Note that if we aren't the first handle, we can't actually use addElementByQuery here!
                // We have non-distinct queries, and there's no guarantee that the same query will end up
                // with the same path if added more than once.
                val (path, nextTree) = if (i == 0) {
                    val (nextPath, reallyNextTree) = queryTree.addElementByQuery(query, handle)
                    Pair(nextPath, reallyNextTree)
                } else {
                    val priorPath = paths[paths.size - 1]
                    val reallyNextTree = queryTree.addElementByPath(priorPath, handle)
                    Pair(priorPath, reallyNextTree)
                }
                paths.add(path)
                handles.add(handle)
                handlesForQuery.add(handle)
                queryTree = nextTree
                querySerial += 1

                // This is getting handled below, but we want to make sure
                // that immediate inserts also work.
                val resultsForPath = queryTree.getByPath(path).asSequence().toSet()
                if (handlesForQuery != resultsForPath) {
                    println("Wait, the path should have been $path=$handlesForQuery")
                    println("Instead it was $path=$resultsForPath")
                    var sawSomething = false
                    for ((actualPath, actualValue) in queryTree) {
                        sawSomething = true
                        println("So what we had was $actualPath=$actualValue")
                    }
                    if (!sawSomething) {
                        println("Why is the tree empty?")
                    }
                }
                assertEquals(handlesForQuery, resultsForPath)
                assertEquals(querySerial.toLong(), queryTree.size)

                // We're adding the query multiple times so that verifyQueriesSet can use
                // same-index offsetting to compare paths and handles.
                queries.add(query)
            }
            lastCount += count
        }
        assertEquals(querySerial.toLong(), queryTree.size)
        this.verifyQueriesSet(data, queries, handles, paths, queryTree)

        // The above tests creating the tree in general, now let's test updating it.
        for (i in 0 until handles.size) {
            val path = paths[i]
            val handle = handles[i]
            val expectedHandles = paths.mapIndexed {
                idx, p -> Pair(idx, p)
            }.filter {
                (idx, p) -> idx != i && p == path
            }.map { (idx) -> handles[idx] }.toMutableSet()
            queryTree = queryTree.removeElementByPath(path, handle)
            val removedResults = queryTree.getByPath(path).asSequence().toSet()
            assertEquals(expectedHandles, removedResults)
            assertEquals(querySerial.toLong() - 1, queryTree.size)

            val removedAgain = queryTree.removeElementByPath(path, handle)
            assertTrue(removedAgain === queryTree)

            queryTree = queryTree.addElementByPath(path, handle)
            expectedHandles.add(handle)
            val reAddedResults = queryTree.getByPath(path).asSequence().toSet()
            assertEquals(expectedHandles, reAddedResults)
            assertEquals(querySerial.toLong(), queryTree.size)

            val addedAgain = queryTree.addElementByPath(path, handle)
            assertTrue(addedAgain === queryTree)
        }
        this.verifyQueriesSet(data, queries, handles, paths, queryTree)

        var removedUntilIdx = 0
        while (removedUntilIdx < handles.size) {
            val removeStartIdx = removedUntilIdx
            val removePath = paths[removedUntilIdx]
            while (removedUntilIdx < paths.size && paths[removedUntilIdx] == removePath) {
                removedUntilIdx++
            }
            for (i in removeStartIdx until removedUntilIdx) {
                val handle = handles[i]
                queryTree = queryTree.removeElementByPath(removePath, handle)
            }
            assertEquals(querySerial.toLong() - removedUntilIdx, queryTree.size)
            this.verifyQueriesSet(
                data,
                queries.subList(removedUntilIdx, queries.size),
                ArrayList(handles.subList(removedUntilIdx, handles.size)),
                ArrayList(paths.subList(removedUntilIdx, paths.size)),
                queryTree
            )
        }
        assertEquals(0, queryTree.size)
    }

    @Test fun queryTreeSetMissingQueryRegistrationsSampleBefore() {
        // These specs caused a failure in queryTreeSet as "Sample arg0".
        // This was actually fine.
        val firstSpec = Pair(
            QuerySpec
                .withEqualityTerm(
                    byteArrayOf(4, -34, 56, -13, -127, -12, 4, -50, 19, -55, -69, 32),
                    byteArrayOf(75)
                )
                .withEqualityTerm(
                    byteArrayOf(40, 10, -10, 88, 35, -8, -21, -31, -37),
                    byteArrayOf(-30, -126, -67, -1, 12, 72, -41, 23, -85)
                )
                .withEqualityTerm(
                    byteArrayOf(-100),
                    byteArrayOf(-82, -10, 11, 5)
                )
                .withEqualityTerm(
                    byteArrayOf(-8, -25, 48, 99, -39, -13),
                    byteArrayOf(-1, 41)
                )
                .withEqualityTerm(
                    byteArrayOf(-6, -20, -9, 20),
                    byteArrayOf(11, 19, -78, -108, 62, -19)
                )
                .withEqualityTerm(
                    byteArrayOf(-3, 75, 1, 22, 74, -127, 31),
                    byteArrayOf(120, 1, -14, 14, 10, -95, 52, 108, 13, 35)
                )
                .withGreaterThanOrEqualToTerm(
                    byteArrayOf(112),
                    byteArrayOf(-85, 11, -122)
                ),
            1
        ) // 1
        val secondSpec = Pair(
            QuerySpec
                .withBetweenOrEqualToTerm(
                    byteArrayOf( -3, 75, 1, 22, 74, -127, 31),
                    byteArrayOf(1),
                    byteArrayOf(-2)
                ),
            6
        ) // 7
        val thirdSpec = Pair(
            QuerySpec
                .withEqualityTerm(
                    byteArrayOf(4, -34, 56, -13, -127, -12, 4, -50, 19, -55, -69, 32),
                    byteArrayOf(75)
                )
                .withEqualityTerm(
                    byteArrayOf(40, 10, -10, 88, 35, -8, -21, -31, -37),
                    byteArrayOf(-30, -126, -67, -1, 12, 72, -41, 23, -85)
                )
                .withEqualityTerm(
                    byteArrayOf(-100),
                    byteArrayOf(-82, -10, 11, 5)
                )
                .withEqualityTerm(
                    byteArrayOf(-8, -25, 48, 99, -39, -13),
                    byteArrayOf(-1, 41)
                )
                .withEqualityTerm(
                    byteArrayOf(-6, -20, -9, 20),
                    byteArrayOf(11, 19, -78, -108, 62, -19)
                )
                .withEqualityTerm(
                    byteArrayOf(-3, 75, 1, 22, 74, -127, 31),
                    byteArrayOf(120, 1, -14, 14, 10, -95, 52, 108, 13, 35)
                )
                .withGreaterThanOrEqualToTerm(
                    byteArrayOf(112),
                    byteArrayOf(-1)
                ),
            2
        ) // 9
        val fourthSpec = Pair(
            QuerySpec
                .withGreaterThanOrEqualToTerm(
                    byteArrayOf(-3, 75, 1, 22, 74, -127, 31),
                    byteArrayOf(-47, 31, -3)
                ),
            5
        ) // 14
        val fifthSpec = Pair(
            QuerySpec
                .withBetweenOrEqualToTerm(
                    byteArrayOf(-3, 75, 1, 22, 74, -127, 31),
                    byteArrayOf(-55, 10, 44, 69, 0, 82, -39, 0, 121),
                    byteArrayOf(-12, 11, -25, 20, 99, 0)
                ),
            5
        ) // 19
        val sixthSpec = Pair(
            QuerySpec
                .withBetweenOrEqualToTerm(
                    byteArrayOf(-3, 75, 1, 22, 74, -127, 31),
                    byteArrayOf(8, 78, -47, 23, -72, 5, 5, -18, -33),
                    byteArrayOf(-90, 8, 26, 54, -55)
                ),
            4
        ) // 23
        val seventhSpec = Pair(
            QuerySpec
                .withEqualityTerm(
                    byteArrayOf(4, -34, 56, -13, -127, -12, 4, -50, 19, -55, -69, 32),
                    byteArrayOf(75)
                )
                .withEqualityTerm(
                    byteArrayOf(40, 10, -10, 88, 35, -8, -21, -31, -37),
                    byteArrayOf(-30, -126, -67, -1, 12, 72, -41, 23, -85)
                )
                .withEqualityTerm(
                    byteArrayOf(-100),
                    byteArrayOf(-82, -10, 11, 5)
                )
                .withEqualityTerm(
                    byteArrayOf(-8, -25, 48, 99, -39, -13),
                    byteArrayOf(-1, 41)
                )
                .withEqualityTerm(
                    byteArrayOf(-6, -20, -9, 20),
                    byteArrayOf(11, 19, -78, -108, 62, -19)
                )
                .withEqualityTerm(
                    byteArrayOf(-3, 75, 1, 22, 74, -127, 31),
                    byteArrayOf(120, 1, -14, 14, 10, -95, 52, 108, 13, 35)
                )
                .withGreaterThanOrEqualToTerm(
                    byteArrayOf(112),
                    byteArrayOf(5, 85, -1, 119, 106, -75)
                ),
            2
        ) // 25
        val eighthSpec = Pair(
            QuerySpec
                .withBetweenOrEqualToTerm(
                    byteArrayOf(-3, 75, 1, 22, 74, -127, 31),
                    byteArrayOf(0),
                    byteArrayOf(-1)
                ),
            3
        ) // 28
        val ninthSpec = Pair(
            QuerySpec
                .withEqualityTerm(
                    byteArrayOf(),
                    byteArrayOf(17, 114)
                )
                .withEqualityTerm(
                    byteArrayOf(3, -4, 26, -72, -24, -49),
                    byteArrayOf(-9, 45, -13, -1, 0, -41, 13)
                )
                .withEqualityTerm(
                    byteArrayOf(112),
                    byteArrayOf(113)
                )
                .withEqualityTerm(
                    byteArrayOf(-8, -25, 48, 99, -39, -13),
                    byteArrayOf(-76, 2, 105, -55, 24, 0)
                )
                .withEqualityTerm(
                    byteArrayOf(-6, -20, -9, 20),
                    byteArrayOf()
                ),
            5
        ) // 33
        val tenthSpec = Pair(
            QuerySpec
                .withEqualityTerm(
                    byteArrayOf(4, -34, 56, -13, -127, -12, 4, -50, 19, -55, -69, 32),
                    byteArrayOf(75)
                )
                .withEqualityTerm(
                    byteArrayOf(40, 10, -10, 88, 35, -8, -21, -31, -37),
                    byteArrayOf(-30, -126, -67, -1, 12, 72, -41, 23, -85)
                )
                .withEqualityTerm(
                    byteArrayOf(-100),
                    byteArrayOf(-82, -10, 11, 5)
                )
                .withEqualityTerm(
                    byteArrayOf(-8, -25, 48, 99, -39, -13),
                    byteArrayOf(-1, 41)
                )
                .withEqualityTerm(
                    byteArrayOf(-6, -20, -9, 20),
                    byteArrayOf(11, 19, -78, -108, 62, -19)
                )
                .withEqualityTerm(
                    byteArrayOf(-3, 75, 1, 22, 74, -127, 31),
                    byteArrayOf(120, 1, -14, 14, 10, -95, 52, 108, 13, 35)
                )
                .withLessThanOrEqualToTerm(
                    byteArrayOf(112),
                    byteArrayOf(126)
                ),
            4
        ) // 37
        val eleventhSpec = Pair(
            QuerySpec
                .withBetweenOrEqualToTerm(
                    byteArrayOf(-3, 75, 1, 22, 74, -127, 31),
                    byteArrayOf(1),
                    byteArrayOf(2)
                ),
            4
        ) // 41
        val twelfthSpec = Pair(
            QuerySpec
                .withEqualityTerm(
                    byteArrayOf(4, -34, 56, -13, -127, -12, 4, -50, 19, -55, -69, 32),
                    byteArrayOf(75)
                )
                .withEqualityTerm(
                    byteArrayOf(40, 10, -10, 88, 35, -8, -21, -31, -37),
                    byteArrayOf(-30, -126, -67, -1, 12, 72, -41, 23, -85)
                )
                .withEqualityTerm(
                    byteArrayOf(-100),
                    byteArrayOf(-82, -10, 11, 5)
                )
                .withEqualityTerm(
                    byteArrayOf(-8, -25, 48, 99, -39, -13),
                    byteArrayOf(-1, 41)
                )
                .withEqualityTerm(
                    byteArrayOf(-6, -20, -9, 20),
                    byteArrayOf(11, 19, -78, -108, 62, -19)
                )
                .withEqualityTerm(
                    byteArrayOf(-3, 75, 1, 22, 74, -127, 31),
                    byteArrayOf(120, 1, -14, 14, 10, -95, 52, 108, 13, 35)
                )
                .withGreaterThanOrEqualToTerm(
                    byteArrayOf(112),
                    byteArrayOf()
                ),
            1,
        ) // 42
        val thirteenthSpec = Pair(
            QuerySpec
                .withGreaterThanOrEqualToTerm(
                    byteArrayOf(-3, 75, 1, 22, 74, -127, 31),
                    byteArrayOf(-66, -11, 2, 15, -84, -11, 93, -9, 34, 114)
                ),
            2
        ) // 44
        val fourteenthSpec = Pair(
            QuerySpec
                .withBetweenOrEqualToTerm(
                    byteArrayOf(-3, 75, 1, 22, 74, -127, 31),
                    byteArrayOf(-128),
                    byteArrayOf(-2)
                ),
            5
        ) // 49
        val specs = listOf(
            firstSpec,
            secondSpec,
            thirdSpec,
            fourthSpec,
            fifthSpec,
            sixthSpec,
            seventhSpec,
            eighthSpec,
            ninthSpec,
            tenthSpec,
            eleventhSpec,
            twelfthSpec,
            thirteenthSpec,
            fourteenthSpec
        )
        this.queryTreeSet(QuerySetTreeTestData(specs, listOf()))
    }

    @Test fun queryTreeSetMissingQueryRegistrationsSampleAfter() {
        // These specs caused a failure in queryTreeSet as "After Execution arg0".
        // This was actually buggy.
        val firstKey = byteArrayOf()
        val firstValue = byteArrayOf(-63, -28, 74, -8, -18, 9, 126, -107)
        val secondKey = byteArrayOf(3, -4, 26, -72, -24, -49)
        val secondValue = byteArrayOf()
        val thirdKey = byteArrayOf(4, -34, 56, -13, -127, -12, 4, -50, 19, -55, -69, 32)
        val thirdValue = byteArrayOf()
        val fourthKey = byteArrayOf(112)
        val fifthKey = byteArrayOf(-3, 75, 1, 22, 74, -127, 31)

        val firstSpec = Pair(
            QuerySpec
                .withEqualityTerm(firstKey, firstValue)
                .withEqualityTerm(secondKey, secondValue)
                .withEqualityTerm(thirdKey, thirdValue)
                .withGreaterThanOrEqualToTerm(fourthKey, byteArrayOf(-85, 11, -122)),
            6
        )
        val secondSpec = Pair(
            QuerySpec
                .withBetweenOrEqualToTerm(fifthKey, byteArrayOf(1), byteArrayOf(-2)),
            5
        ) // 11
        val thirdSpec = Pair(
            QuerySpec
                .withEqualityTerm(firstKey, firstValue)
                .withEqualityTerm(secondKey, secondValue)
                .withEqualityTerm(thirdKey, thirdValue)
                .withGreaterThanOrEqualToTerm(fourthKey, byteArrayOf(-1)),
            2
        ) // 13
        val fourthSpec = Pair(
            QuerySpec
                .withGreaterThanOrEqualToTerm(fifthKey, byteArrayOf(-47, 31, -3)),
            2
        ) // 15
        val fifthSpec = Pair(
            QuerySpec
                .withBetweenOrEqualToTerm(
                    fifthKey,
                    byteArrayOf(-55, 10, 44, 69, 0, 82, -39, 0, 121),
                    byteArrayOf(-12, 11, -25, 20, 99, 0)
                ),
            3
        ) // 18
        val sixthSpec = Pair(
            QuerySpec
                .withBetweenOrEqualToTerm(
                    fifthKey,
                    byteArrayOf(8, 78, -47, 23, -72, 5, 5, -18, -33),
                    byteArrayOf(-90, 8, 26, 54, -55)
                ),
            1
        ) // 19
        val seventhSpec = Pair(
            QuerySpec
                .withEqualityTerm(firstKey, firstValue)
                .withEqualityTerm(secondKey, secondValue)
                .withEqualityTerm(thirdKey, thirdValue)
                .withGreaterThanOrEqualToTerm(fourthKey, byteArrayOf(5, 85, -1, 119, 106, -75)),
            2
        ) // 21
        val eighthSpec = Pair(
            QuerySpec
                .withBetweenOrEqualToTerm(
                    fifthKey,
                    byteArrayOf(0),
                    byteArrayOf(-1)
                ),
            6
        ) // 27
        val ninthSpec = Pair(
            QuerySpec
                .withEqualityTerm(secondKey, secondValue)
                .withEqualityTerm(thirdKey, byteArrayOf(127, -117, -37, 0, -127, 47, -33))
                .withEqualityTerm(
                    byteArrayOf(7, -4),
                    byteArrayOf(-5, -15, -127, 0, -1, -25, -116, -2, 117)
                )
                .withEqualityTerm(
                    byteArrayOf(38, 10, -2, -124, -72, -10),
                    byteArrayOf(-75, -35, 0)
                )
                .withEqualityTerm(
                    byteArrayOf(40, 10, -10, 88, 35, -8, -21, -31, -37),
                    byteArrayOf(-67, -26, 49, -9, -108, -9, 41, -36)
                )
                .withEqualityTerm(
                    fourthKey,
                    byteArrayOf(-3, 11, 58, -7, 53, 70, -30, -92, 3)
                )
                .withEqualityTerm(
                    byteArrayOf(-100),
                    byteArrayOf(-68, 52, -2, -68, -8, 37, -29, 52, -54, -4)
                )
                .withEqualityTerm(
                    byteArrayOf(-8, -25, 48, 99, -39, -13),
                    byteArrayOf(-103, 118, 40, -54, -54, -68, -9)
                )
                .withEqualityTerm(
                    byteArrayOf(-6, -20, -9, 20),
                    byteArrayOf(-96, 83, -27)
                )
                .withEqualityTerm(
                    fifthKey,
                    byteArrayOf(4, -40, 127, 24, -6, -36, -128)
                )
            ,
            6
        ) // 33
        val tenthSpec = Pair(
            QuerySpec
                .withEqualityTerm(firstKey, firstValue)
                .withEqualityTerm(secondKey, secondValue)
                .withEqualityTerm(thirdKey, thirdValue)
                .withLessThanOrEqualToTerm(fourthKey, byteArrayOf(126)),
            5
        ) // 38
        val eleventhSpec = Pair(
            QuerySpec
                .withBetweenOrEqualToTerm(
                    fifthKey,
                    byteArrayOf(1),
                    byteArrayOf(2)
                ),
            6
        ) // 44
        val twelfthSpec = Pair(
            QuerySpec
                .withEqualityTerm(firstKey, firstValue)
                .withEqualityTerm(secondKey, secondValue)
                .withEqualityTerm(thirdKey, thirdValue)
                .withGreaterThanOrEqualToTerm(fourthKey, byteArrayOf()),
            2
        ) // 46
        val specs = listOf(
            firstSpec,
            secondSpec,
            thirdSpec,
            fourthSpec,
            fifthSpec,
            sixthSpec,
            seventhSpec,
            eighthSpec,
            ninthSpec,
            tenthSpec,
            eleventhSpec,
            twelfthSpec,
        )
        this.queryTreeSet(QuerySetTreeTestData(specs, listOf()))
    }

    @Test fun queryTreeNonSetVisitationMissingData() {
        val testData = QueryTreeTestData(
            queries=listOf(
                QuerySpec.withLessThanOrEqualToTerm(byteArrayOf(), byteArrayOf(0))
            ),
            data=listOf(
                QPTrie(listOf(byteArrayOf() to byteArrayOf(0))),
                QPTrie(listOf(byteArrayOf() to byteArrayOf())),
            )
        )
        this.queryTreeNonSet(testData)
    }

    @Test fun queryTreeNonSetVisitationStillMissingData() {
        val testData = QueryTreeTestData(
            queries = listOf(
                QuerySpec.withGreaterThanOrEqualToTerm(byteArrayOf(), byteArrayOf(0, 0, 3))
            ),
            data = listOf(
                QPTrie(listOf(byteArrayOf() to byteArrayOf(-22)))
            )
        )
        this.queryTreeNonSet(testData)
    }

    @Test fun queryTreeNonSetVisitationsTooMuchData() {
        val testData = QueryTreeTestData(
            queries = listOf(
                QuerySpec.withLessThanOrEqualToTerm(byteArrayOf(), byteArrayOf(0, 0, 0, -41, 28, 7))
            ),
            data = listOf(
                QPTrie(listOf(byteArrayOf() to byteArrayOf(106, -10, -7, -52, 3, 2)))
            )
        )
        this.queryTreeNonSet(testData)
    }

    @Test fun queryTreeWrongInternalPath() {
        val dataPoint = QPTrie(listOf(
            byteArrayOf(4, 16, 25, 29, -13, 6) to byteArrayOf(),
            byteArrayOf(17, 68, -9, -14, 97, -77, 81, -6) to byteArrayOf(-10, 9, -38, -27),
            byteArrayOf(94, -1, 48, 78, -6, -10, -35, -127, -20, -39, -10, -60, 89, -2) to byteArrayOf(107),
            byteArrayOf(-9, -37, 24, 84, -128, 35, -42, -50, 75) to byteArrayOf(-14, 68, 19, -22, 11, -22, 119, 13),
            byteArrayOf(-3, 7, 106, -2, 41, -94) to byteArrayOf(126)
        ))

        val mostCommonEqualityTerms = QuerySpec
            .withEqualityTerm(
                byteArrayOf(4, 16, 25, 29, -13, 6),
                byteArrayOf()
            )
            .withEqualityTerm(
                byteArrayOf(-9, -37, 24, 84, -128, 35, -42, -50, 75),
                byteArrayOf(5, -115, 75, -10, 8)
            )
            .withEqualityTerm(
                byteArrayOf(-3, 7, 106, -2, 41, -94),
                byteArrayOf(-38, -5, -27, 8, 82, 7, -93, 107, 4, 77)
            )
        val mostCommonBetweenKey = byteArrayOf(17, 68, -9, -14, 97, -77, 81, -6)
        val queries = listOf(
            QuerySpec.withBetweenOrEqualToTerm(
                byteArrayOf(-9, -37, 24, 84, -128, 35, -42, -50, 75),
                byteArrayOf(0),
                byteArrayOf(126)
            ),
            mostCommonEqualityTerms.withBetweenOrEqualToTerm(
                mostCommonBetweenKey,
                byteArrayOf(),
                byteArrayOf(127)
            ),
            QuerySpec.withBetweenOrEqualToTerm(
                byteArrayOf(-9, -37, 24, 84, -128, 35, -42, -50, 75),
                byteArrayOf(-128),
                byteArrayOf(-9, 17, 5, 60)
            ),
            mostCommonEqualityTerms.withBetweenOrEqualToTerm(
                mostCommonBetweenKey,
                byteArrayOf(4, -17, -17),
                byteArrayOf(36, 19, -26, -8, -11, 12, 6, -1)
            ),
            QuerySpec.withEqualityTerm(
                byteArrayOf(4, 16, 25, 29, -13, 6),
                byteArrayOf()
            ),
            QuerySpec.withStartsWithTerm(
                byteArrayOf(-9, -37, 24, 84, -128, 35, -42, -50, 75),
                byteArrayOf(-110, -37, -40, 20, 126, -127)
            ),
            mostCommonEqualityTerms.withBetweenOrEqualToTerm(
                mostCommonBetweenKey,
                byteArrayOf(-47, -123, -21, 16, -7, 2),
                byteArrayOf(-23, -31, 28, -12, 111, 21, -61, -128, 29, 127)
            ),
            mostCommonEqualityTerms.withBetweenOrEqualToTerm(
                mostCommonBetweenKey,
                byteArrayOf(2),
                byteArrayOf(-128)
            ),
            mostCommonEqualityTerms.withBetweenOrEqualToTerm(
                mostCommonBetweenKey,
                byteArrayOf(127),
                byteArrayOf(-1)
            ),
            QuerySpec.withBetweenOrEqualToTerm(
                byteArrayOf(-9, -37, 24, 84, -128, 35, -42, -50, 75),
                byteArrayOf(27, 17, -18, 1, -41, 73, 41, -104),
                byteArrayOf(-11)
            ),
            mostCommonEqualityTerms.withBetweenOrEqualToTerm(
                mostCommonBetweenKey,
                byteArrayOf(-1),
                byteArrayOf(-1)
            ),
            mostCommonEqualityTerms.withStartsWithTerm(
                mostCommonBetweenKey,
                byteArrayOf(-12, -60)
            )
        )
        val testData = QueryTreeTestData(queries, listOf(dataPoint))
        this.queryTreeNonSet(testData)
    }

    @Test fun setWithCardinality() {
        val set = SetWithCardinality<Int>().add(1).add(2).add(3)
        assertEquals(3, set.computeSize())
        assertDoesNotThrow { set.toString() }
    }

    @Test fun addQueryToSetTreeTwice() {
        val query = QuerySpec.withEqualityTerm(byteArrayOf(), byteArrayOf(1))
        val baseTree = QuerySetTree<Int>()
        val (_, newTree) = baseTree.addElementByQuery(query, 2)
        val (_, newerTree) = newTree.addElementByQuery(query, 2)
        assertTrue(newTree === newerTree)
    }

    @Test fun addThenRemoveByQueryEquals() {
        val query = QuerySpec.withEqualityTerm(byteArrayOf(), byteArrayOf(1))
        val baseTree = QueryTree<SizeComputableInteger>()
        val (_, newTree) = baseTree.updateByQuery(query) { SizeComputableInteger(1, 1) }
        val (_, newerTree) = newTree.updateByQuery(query) { null }
        assertFalse(newTree === newerTree)
        assertEquals(1L, newTree.size)
        assertEquals(0L, newerTree.size)
    }

    @Test fun addThenRemoveByQueryRange() {
        val query = QuerySpec.withBetweenOrEqualToTerm(byteArrayOf(), byteArrayOf(1), byteArrayOf(2))
        val baseTree = QueryTree<SizeComputableInteger>()
        val (_, newTree) = baseTree.updateByQuery(query) { SizeComputableInteger(1, 1) }
        val (_, newerTree) = newTree.updateByQuery(query) { null }
        assertFalse(newTree === newerTree)
        assertEquals(1L, newTree.size)
        assertEquals(0L, newerTree.size)
    }

    @Test fun addThenRemoveByQueryStartsWith() {
        val query = QuerySpec.withStartsWithTerm(byteArrayOf(), byteArrayOf(1))
        val baseTree = QueryTree<SizeComputableInteger>()
        val (_, newTree) = baseTree.updateByQuery(query) { SizeComputableInteger(1, 1) }
        val (_, newerTree) = newTree.updateByQuery(query) { null }
        assertFalse(newTree === newerTree)
        assertEquals(1L, newTree.size)
        assertEquals(0L, newerTree.size)
    }

    @Test fun querySpecEqualityRemovesInequalityForSameKey() {
        val query = QuerySpec.withLessThanOrEqualToTerm(byteArrayOf(), byteArrayOf())
        val nextQuery = query.withEqualityTerm(byteArrayOf(), byteArrayOf())
        assertFalse(nextQuery === query)
        assertEquals(1, query.cardinality)
        assertEquals(1, nextQuery.cardinality)
        assertEquals(0L, query.equalityTerms.size)
        assertNotNull(query.inequalityTerm)
        assertEquals(1L, nextQuery.equalityTerms.size)
        assertNull(nextQuery.inequalityTerm)
    }
}