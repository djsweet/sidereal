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

        private fun querySpecMatchesData(querySpec: QuerySpec, data: QPTrie<ByteArray>): Boolean {
            for ((key, value) in querySpec.equalityTerms) {
                val dataAtKey = data.get(key) ?: return false
                if (!value.contentEquals(dataAtKey)) {
                    return false
                }
            }
            val inequalityTerm = querySpec.inequalityTerm ?: return true
            val dataAtInequality = data.get(inequalityTerm.key) ?: return false
            return when (inequalityTerm.kind) {
                IntermediateQueryTermKind.EQUALS -> (inequalityTerm.lowerBound!!).contentEquals(dataAtInequality)
                IntermediateQueryTermKind.STARTS_WITH -> {
                    val boundSize = inequalityTerm.lowerBound!!.size
                    if (boundSize > dataAtInequality.size) {
                        false
                    } else {
                        Arrays.equals(
                            inequalityTerm.lowerBound,
                            0,
                            boundSize,
                            dataAtInequality,
                            0,
                            boundSize
                        )
                    }
                }
                IntermediateQueryTermKind.GREATER_OR_LESS -> if (inequalityTerm.lowerBound == null && inequalityTerm.upperBound == null) {
                    false
                } else if (inequalityTerm.lowerBound != null && inequalityTerm.upperBound != null) {
                    Arrays.compareUnsigned(
                        dataAtInequality, inequalityTerm.lowerBound
                    ) >= 0 && Arrays.compareUnsigned(
                        dataAtInequality, inequalityTerm.upperBound
                    ) <= 0
                } else if (inequalityTerm.upperBound != null) {
                    Arrays.compareUnsigned(dataAtInequality, inequalityTerm.upperBound) <= 0
                } else {
                    Arrays.compareUnsigned(dataAtInequality, inequalityTerm.lowerBound!!) >= 0
                }
            }
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
            return Arbitraries.just(Triple(QuerySpec(), arrayListOf(), null))
        }

        val firstQuery = this.generateEqualityQuery(subset.last(), QuerySpec()).map { (querySpec, term) ->
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
        return this.generateInequalityQuery(key, QuerySpec()).map {(querySpec, inequalityTerm) ->
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
            val qsit = querySpec.inequalityTerm
            assertNotNull(qsit)
            assertEquals(maybeInequalityTerm.key, qsit!!.key)
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

        var removingQuerySpec = querySpec
        while (expectedEqualityTerms.size > 0) {
            val removeThis = expectedEqualityTerms.first()
            val (removeKey, _) = removeThis
            expectedEqualityTerms.removeAt(0)
            removingQuerySpec = removingQuerySpec.withoutEqualityTerm(removeKey.array)
            givenEqualityTerms.clear()
            for ((keepKey, value) in removingQuerySpec.equalityTerms) {
                givenEqualityTerms.add(Pair(ByteArrayButComparable(keepKey), ByteArrayButComparable(value)))
            }
            assertListOfByteArrayValuePairsEquals(expectedEqualityTerms, givenEqualityTerms)

            val removedAgain = removingQuerySpec.withoutEqualityTerm(removeKey.array)
            assertTrue(removedAgain === removingQuerySpec)
        }
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

        assertArrayEquals(oneShotPath.breadcrumbs.toTypedArray(), prependPath.breadcrumbs.toTypedArray())
    }

    @Property
    fun queryPathFirstAndSubLists(
        @ForAll @From("intermediateQueryTerms") publicQueryTerms: PersistentList<PublicIntermediateQueryTerm>
    ) {
        var queryTerms = publicQueryTerms.map { it.actual }.toPersistentList()
        var queryPath = QueryPath(queryTerms)
        while (queryTerms.size > 0 && queryPath.breadcrumbs.size > 0) {
            assertEquals(queryTerms.size, queryPath.breadcrumbs.size)
            assertEquals(queryTerms.first(), queryPath.first())
            queryTerms = queryTerms.subList(1, queryTerms.size).toPersistentList()
            queryPath = queryPath.rest()
        }
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
        val breadcrumbs = queryPath.breadcrumbs
        if (breadcrumbs.isEmpty()) {
            return Arbitraries.just(listOf())
        }

        val fillTrieLift = { term: IntermediateQueryTerm, workingTrie: QPTrie<ByteArray>, seenKeys: QPTrie<Boolean>, pastList: PersistentList<QPTrie<ByteArray>>  ->
            val (key, value) = this.basedDataForIntermediateQueryTerm(term).sample()
            val nextWorkingTrie = workingTrie.put(key, value)
            val nextSeenKeys = seenKeys.put(key, true)
            val filled = this.fillTrieWithArbitraryData(nextWorkingTrie, keySpace, nextSeenKeys).sample()
            Triple(pastList.add(0, filled), nextWorkingTrie, nextSeenKeys)
        }
        val (firstData, firstTerm) = this.basedDataForIntermediateQueryTerm(breadcrumbs.first()).sample()
        val firstTrie = QPTrie(listOf(firstData to firstTerm))
        val firstFilled = this.fillTrieWithArbitraryData(
            firstTrie,
            keySpace,
            QPTrie(listOf(firstData to true)),
        ).sample()
        val basis = Triple(persistentListOf(firstFilled), firstTrie, QPTrie<Boolean>())
        val (entries) = breadcrumbs.subList(1, breadcrumbs.size).fold(basis) { acc, term ->
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
        val equalityTerms = querySpec.equalityTerms.map { (key, value) -> IntermediateQueryTerm.equalityTerm(key, value) }
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

    @Provide
    fun queryTreeTestData(): Arbitrary<QueryTreeTestData> {
        return this.byteArray(keySize).list().ofMinSize(1).ofMaxSize(keySpaceSize).flatMap { keySpace ->
            val queryList = this.querySpecWithKeySpace(keySpace).map {
                (queries, _, _) -> queries
            }.list().ofMinSize(1).ofMaxSize(24)
            queryList.flatMap { queries ->
                val basedData = listOfArbitrariesToArbitraryOfList(queries.map {
                    this.basedDataForQuerySpec(it, keySpace)
                }).map { flattenList(it) }
                val arbitraryData = arbitraryDataForKeySpace(keySpace).list().ofMaxSize(128)

                val basedDataSample = basedData.sample()
                val arbitraryDataSample = arbitraryData.sample()
                val dataList = concatList(basedDataSample, arbitraryDataSample)
                Arbitraries.just(QueryTreeTestData(uniqueQuerySpecs(queries), dataList))
            }
        }
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
                    (_, q) -> querySpecMatchesData(q, dataEntry)
            }.map {
                    (idx) -> handles[idx]
            }.toTypedArray()
            var givenResults = persistentListOf<SizeComputableInteger>()
            for ((path, result) in queryTree.getByData(dataEntry)) {
                val pathIdx = findIndex(paths, path)
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
                println("Wait, the path was $path=$handle")
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
        val querySpec = QuerySpec().withLessThanOrEqualToTerm(byteArrayOf(12), byteArrayOf(13))
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

        val byData = tree.getByData(QPTrie(listOf(byteArrayOf(12) to byteArrayOf(12)))).asSequence().toList()
        assertEquals(1, byData.size)
        assertEquals(handle, byData.first().value)
    }

    @Test
    fun queryTreeJustGreaterThan() {
        val querySpec = QuerySpec().withGreaterThanOrEqualToTerm(byteArrayOf(12), byteArrayOf(13))
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
                querySpecMatchesData(q, dataEntry)
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
                val (path, nextTree) = queryTree.addElementByQuery(query, handle)
                paths.add(path)
                handles.add(handle)
                handlesForQuery.add(handle)
                queryTree = nextTree
                querySerial += 1

                // This is getting handled below, but we want to make sure
                // that immediate inserts also work.
                val resultsForPath = queryTree.getByPath(path).asSequence().toSet()
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
}