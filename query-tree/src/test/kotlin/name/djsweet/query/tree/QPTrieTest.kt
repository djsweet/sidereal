package name.djsweet.query.tree

import org.junit.jupiter.api.Test

import org.junit.jupiter.api.Assertions.*
import net.jqwik.api.*
import java.util.*

private fun <V> trieIsEmpty(trie: QPTrie<V>) {
    assertEquals(0, trie.size)
    assertNull(trie.get(byteArrayOf(0x1, 0x2, 0x3)))

    val basicIt = trie.iteratorUnsafeSharedKey()
    assertFalse(basicIt.hasNext())

    val ascIt = trie.iteratorAscendingUnsafeSharedKey()
    assertFalse(ascIt.hasNext())

    val descIt = trie.iteratorDescendingUnsafeSharedKey()
    assertFalse(descIt.hasNext())

    val lessEqualIt = trie.iteratorLessThanOrEqualUnsafeSharedKey(byteArrayOf(0x1, 0x2, 0x4))
    assertFalse(lessEqualIt.hasNext())

    val greaterEqualIt = trie.iteratorGreaterThanOrEqualUnsafeSharedKey(byteArrayOf(0x1, 0x2))
    assertFalse(greaterEqualIt.hasNext())

    val startsWithIt = trie.iteratorStartsWithUnsafeSharedKey(byteArrayOf(0x1))
    assertFalse(startsWithIt.hasNext())

    val nodesStartIt = trie.iteratorPrefixOfOrEqualToUnsafeSharedKey(byteArrayOf(0x1, 0x2, 0x3, 0x4))
    assertFalse(nodesStartIt.hasNext())
}

private fun <T> fixIteratorForInvariants(it: Iterator<QPTrieKeyValue<T>>): List<Pair<ByteArrayButComparable, T>> {
    return it.asSequence().map { (key, value) ->
        Pair(ByteArrayButComparable(key), value)
    }.toList()
}

private fun <V> verifyIteratorInvariants(t: QPTrie<V>, spec: IntervalTree<ByteArrayButComparable, V>) {
    val expectedAscending = spec.iterator().asSequence().map { Pair(it.first.lowerBound, it.second) }.toList()
    val expectedDescending = expectedAscending.reversed()

    val minKeyValue = t.minKeyValueUnsafeSharedKey()
    if (t.size == 0L) {
        assertNull(minKeyValue)
    } else {
        assertNotNull(minKeyValue)
        val (firstKey, firstValue) = expectedAscending.first()
        assertArrayEquals(firstKey.array, minKeyValue!!.key)
        assertEquals(firstValue, minKeyValue.value)
    }

    val givenAscendingImplicit = fixIteratorForInvariants(t.iterator())
    assertListOfByteArrayValuePairsEquals(expectedAscending, givenAscendingImplicit)
    val givenAscendingExplicit = fixIteratorForInvariants(t.iteratorAscendingUnsafeSharedKey())
    assertListOfByteArrayValuePairsEquals(expectedAscending, givenAscendingExplicit)

    val ascendingVisited = ArrayList<Pair<ByteArrayButComparable, V>>()
    t.visitAscendingUnsafeSharedKey { ascendingVisited.add(ByteArrayButComparable(it.key) to it.value) }
    assertListOfByteArrayValuePairsEquals(expectedAscending, ascendingVisited)

    ascendingVisited.clear()
    t.visitUnsafeSharedKey { ascendingVisited.add(ByteArrayButComparable(it.key) to it.value) }
    assertListOfByteArrayValuePairsEquals(expectedAscending, ascendingVisited)

    val keyResult = ArrayList<ByteArray>()
    t.keysIntoUnsafeSharedKey(keyResult)
    assertEquals(t.size.toInt(), keyResult.size)
    assertEquals(
        givenAscendingImplicit.map { it.first }.toList(),
        keyResult.map { ByteArrayButComparable(it) }.toList()
    )

    val givenDescending = fixIteratorForInvariants(t.iteratorDescendingUnsafeSharedKey())
    assertListOfByteArrayValuePairsEquals(expectedDescending, givenDescending)

    val descendingVisited = ArrayList<Pair<ByteArrayButComparable, V>>()
    t.visitDescendingUnsafeSharedKey { descendingVisited.add(ByteArrayButComparable(it.key) to it.value) }
    assertListOfByteArrayValuePairsEquals(expectedDescending, descendingVisited)

    if (spec.size == 0L) {
        return
    }

    val minItem = spec.minRange()!!
    val maxItem = spec.maxRange()!!

    // Less than or equal checks
    for (element in expectedAscending) {
        val expectedUpTo = spec.lookupRange(Pair(minItem.lowerBound, element.first)).asSequence().map { (key, value) ->
            Pair(key.lowerBound, value)
        }.toList().reversed()
        val minRanges = fixIteratorForInvariants(t.iteratorLessThanOrEqualUnsafeSharedKey(element.first.array))
        assertListOfByteArrayValuePairsEquals(expectedUpTo, minRanges)

        val lessThanVisited = ArrayList<Pair<ByteArrayButComparable, V>>()
        t.visitLessThanOrEqualUnsafeSharedKey(element.first.array) {
            lessThanVisited.add(ByteArrayButComparable(it.key) to it.value)
        }
        assertListOfByteArrayValuePairsEquals(expectedUpTo, lessThanVisited)
    }

    // Greater than or equal checks
    for (element in expectedAscending) {
        val expectedDownTo = spec.lookupRange(Pair(element.first, maxItem.lowerBound)).asSequence().map { (key, value) ->
            Pair(key.lowerBound, value)
        }.toList()
        val maxRanges = fixIteratorForInvariants(t.iteratorGreaterThanOrEqualUnsafeSharedKey(element.first.array))
        assertListOfByteArrayValuePairsEquals(expectedDownTo, maxRanges)

        val greaterThanVisited = ArrayList<Pair<ByteArrayButComparable, V>>()
        t.visitGreaterThanOrEqualUnsafeSharedKey(element.first.array) {
            greaterThanVisited.add(ByteArrayButComparable(it.key) to it.value)
        }
        assertListOfByteArrayValuePairsEquals(expectedDownTo, greaterThanVisited)
    }
}

class QPTrieTest {
    class PublicByteArrayButComparable internal constructor(internal val actual: ByteArrayButComparable) {
        override fun toString(): String {
            return this.actual.toString()
        }
    }

    @Test fun emptyTrie() {
        val trie = QPTrie<String>()
        trieIsEmpty(trie)

        val removeNothing = trie.remove(byteArrayOf(0x1, 0x2, 0x3))
        trieIsEmpty(removeNothing)

        val updateNull = trie.update(byteArrayOf(0x1, 0x2, 0x3)) { null }
        trieIsEmpty(updateNull)
    }

    @Test fun twoElementTrieSimpleGetting() {
        var trie = QPTrie<String>()
        assertEquals(0, trie.size)
        val shortEntry = byteArrayOf(0)
        val longEntry = byteArrayOf(-73, 1, -39, 4, -1, -8, 11, -40, -61, -27, 36, -16, -80, -5, -1, 7, -41)

        trie = trie.put(shortEntry, "short")
        assertEquals("short", trie.get(shortEntry))
        assertNull(trie.get(longEntry))
        assertEquals(1, trie.size)

        trie = trie.put(longEntry, "long")
        assertEquals("short", trie.get(shortEntry))
        assertEquals("long", trie.get(longEntry))
        assertEquals(2, trie.size)
    }

    @Test fun twoElementSharedPrefixTrieSimpleGetting() {
        val shortEntry = byteArrayOf(-128)
        val longEntry = byteArrayOf(-128, 12, 13)
        var trie = QPTrie(listOf(
            shortEntry to "short"
        ))
        assertEquals(1, trie.size)
        assertEquals("short", trie.get(shortEntry))
        assertNull(trie.get(longEntry))

        trie = trie.put(longEntry, "long")
        assertEquals(2, trie.size)
        assertEquals("short", trie.get(shortEntry))
        assertEquals("long", trie.get(longEntry))
    }

    @Test fun threeElementRepeatedUpdateTrieSimpleGetting() {
        val shortEntry = byteArrayOf()
        val middleEntry = byteArrayOf(94)
        val longEntry = byteArrayOf(
            94, -97, 35, 99, -3,
            95, 0, 126, -34, 83,
            38, 9, -7, 39, 100, -69,
            -119, -16, -22, -106, -1,
            -104, 21, -71, 32, -33
        )
        var trie = QPTrie(listOf(middleEntry to "keep"))
        assertEquals(1, trie.size)
        assertEquals("keep", trie.get(middleEntry))
        assertNull(trie.get(shortEntry))
        assertNull(trie.get(longEntry))

        trie = trie.put(shortEntry, "first")
        assertEquals(2, trie.size)
        assertEquals("keep", trie.get(middleEntry))
        assertEquals("first", trie.get(shortEntry))
        assertNull(trie.get(longEntry))

        trie = trie.put(shortEntry, "second")
        assertEquals(2, trie.size)
        assertEquals("keep", trie.get(middleEntry))
        assertEquals("second", trie.get(shortEntry))
        assertNull(trie.get(longEntry))

        trie = trie.put(shortEntry, "third")
        assertEquals(2, trie.size)
        assertEquals("keep", trie.get(middleEntry))
        assertEquals("third", trie.get(shortEntry))
        assertNull(trie.get(longEntry))

        trie = trie.put(shortEntry, "fourth")
        assertEquals(2, trie.size)
        assertEquals("keep", trie.get(middleEntry))
        assertEquals("fourth", trie.get(shortEntry))
        assertNull(trie.get(longEntry))

        trie = trie.put(shortEntry, "fifth")
        assertEquals(2, trie.size)
        assertEquals("keep", trie.get(middleEntry))
        assertEquals("fifth", trie.get(shortEntry))
        assertNull(trie.get(longEntry))

        trie = trie.put(shortEntry, "sixth")
        assertEquals(2, trie.size)
        assertEquals("keep", trie.get(middleEntry))
        assertEquals("sixth", trie.get(shortEntry))
        assertNull(trie.get(longEntry))

        trie = trie.put(shortEntry, "seventh")
        assertEquals(2, trie.size)
        assertEquals("keep", trie.get(middleEntry))
        assertEquals("seventh", trie.get(shortEntry))
        assertNull(trie.get(longEntry))

        trie = trie.put(longEntry, "long")
        assertEquals(3, trie.size)
        assertEquals("seventh", trie.get(shortEntry))
        assertEquals("long", trie.get(longEntry))
        assertEquals("keep", trie.get(middleEntry))
    }

    @Test fun addingThenRemovingTrieEntries() {
        val trieEntries = listOf(
            byteArrayOf(12, -1, 30, 25, 2, 18, -52, 1, 11, 56, -71, 28, -29, -18, -7, -30, 19, 29) to "first",
            byteArrayOf(-54, -31, 127) to "second",
            byteArrayOf(-1, 49, 75, -2, -35, 10, 102, 6, -1, -39, 0, 40, -39, -128, -27, 14, -3, 15, -2, 8, 18, -7, 4, -101, 57, 64, 35) to "third",
            byteArrayOf(9, -17, 14, 0, 11, 12) to "fourth",
            byteArrayOf( -2, -3, -46, -92, 10, 22, -8, 19, 6, 3, 34, 12, 76, -58, -46, 37, 1, 19, -105, -106, 30, 126, 15, -56, 13) to "fifth",
            byteArrayOf(23, 5, 0, 16, 58) to "sixth",
            byteArrayOf(45, -2, 108, 42, 32, 30, -28, -95, 46, 82, 127, -2, 20, -8, -106, 1, -22, -40) to "seventh",
            byteArrayOf(-4, 30, 113, 19, -10, 3, 64, -39, 99, 0, 66, 8, 0) to "eighth",
            byteArrayOf(13, 3, -4, -35, -7, 12, -113, 126, 26, 23, -40, 31, 12, -43, 25, 86, -15, -4, 10, -127, 104, -6, -66, -68, -76, 35, -26) to "ninth",
            byteArrayOf(11, 20, -1, -12, 117, -12, 33, -39, -26, 24, 4, 32, 2, -90, 80, -53, 29, 124, -52, 59, -5, 0, 11, -53, 4, 6, 14, 13, 82, -54, 5, 18, 1, 6, -1, 36, 31, -26, 108, 12, -67, -6, 105, 22, -13, 31, 36, -79, -127, -128, 83, -3, -2, -57, 26) to "tenth",
            byteArrayOf(48, 12, 23, 50, 112, -62, -29, 92, -23, -93) to "eleventh",
            byteArrayOf(11, 16, 94, -2, -7, 12, -10, 106, 112, 1, 8, 19, 11, 17, -36, -9, -128, -2, -9, 106, -36, 115, -27, 94, -90, 83, 0, -4, -26, -128, -83, -6, -16, 68, -107) to "twelfth",
            byteArrayOf(3) to "thirteenth",
            byteArrayOf(-32, 12, 52, -1, -37, -7) to "fourteenth",
            byteArrayOf(-14, -24, -4, 13, -34, 32, 10, -19, 100, -76, 2, 44, -98, 31, 111, -102, 52, 1, 106, -1, 97) to "fifteenth",
            byteArrayOf(-8, 59, -82, 11, -25, -7, -55, -6, -34, -126, -17, -34, -2, -4, -41, 95, -37, 57, 12, -34, 41, 126, 12, 14, -15, -108, 65, -109) to "sixteenth",
            byteArrayOf(-125, 39, -17, -28, 3, -128, 119, -77, -10, 10, 71, -13, -12, -12, -42) to "seventeenth",
            byteArrayOf(25, -13, 2, -6, 7, 12, 12, -23, -3) to "eighteenth",
            byteArrayOf(113, -70, -119, -127, -10, -35, -128, -96, 63, 16, 6, 15, 22) to "nineteenth",
            byteArrayOf(34, -25, -62, 126, -15, 11, -8, -25) to "twentieth",
            byteArrayOf(28, 4, 20, -44, -128, 121, 6, -30, -102, -22, -39, -40, -6, 8, 61, -37, 11, 18, 19, -98) to "twenty first",
            byteArrayOf(-54, -8, 8, -38, 125, 8, -108, 11, 57, 3, -59, 44, -128, -38, 38, -13) to "twenty second",
            byteArrayOf(-1, 58, 9, -3, 5, -1, -87, 49, 13, -19, -103, -127, 107, 68, -25, -16, -19, -25, -8, -17, -7, -30, -15, -27, -46, -48, 36, -10, -41, -20, -43, -28) to "twenty third",
            byteArrayOf(2) to "twenty fourth",
            byteArrayOf(-2, 8, -117, -54, 18, -13, 30, -61, 72, -3, -128, -2, 4, -37, 3, -25, 25, -8, -4, -26, -17, 18, -1, -1, 34, -38, 126, -3, 9, -4, -99, 2, -128, -34, 107, -56, 77, -6, -28, 102, -93, 21, 0, 8, 99, 121, 38, -23, 4, 49, -56, 9, 24, -38, -38, -41) to "twenty-fifth",
            byteArrayOf(-18, -11, -34, 28, 2, -115, -27, -71, 95, -32, -119, 85) to "twenty sixth",
            byteArrayOf(-11, 53, -4, 126) to "twenty seventh",
            byteArrayOf(8, -8, -11, -27, -33, 7, -39) to "twenty eighth"
        )
        var trie = QPTrie(trieEntries)
        for (entry in trieEntries) {
            assertEquals(entry.second, trie.get(entry.first))
        }
        for (i in 0 until 14) {
            trie = trie.remove(trieEntries[trieEntries.size - i - 1].first)
        }
        assertEquals(trieEntries[0].second, trie.get(trieEntries[0].first))
        assertEquals(trieEntries[1].second, trie.get(trieEntries[1].first))
        assertEquals(trieEntries[2].second, trie.get(trieEntries[2].first))
        assertEquals(trieEntries[3].second, trie.get(trieEntries[3].first))
        assertEquals(trieEntries[4].second, trie.get(trieEntries[4].first))
        assertEquals(trieEntries[5].second, trie.get(trieEntries[5].first))
        assertEquals(trieEntries[6].second, trie.get(trieEntries[6].first))
        assertEquals(trieEntries[7].second, trie.get(trieEntries[7].first))
        assertEquals(trieEntries[8].second, trie.get(trieEntries[8].first))
        assertEquals(trieEntries[9].second, trie.get(trieEntries[9].first))
        assertEquals(trieEntries[10].second, trie.get(trieEntries[10].first))
        assertEquals(trieEntries[11].second, trie.get(trieEntries[11].first))
        assertEquals(trieEntries[12].second, trie.get(trieEntries[12].first))
        assertEquals(trieEntries[13].second, trie.get(trieEntries[13].first))
        for (i in 0 until 14) {
            val entry = trieEntries[trieEntries.size - i - 1]
            assertNull(trie.get(entry.first))
        }
    }

    private fun<V> qpTrieKeyValueForBytes(bytes: ByteArray, value: V): QPTrieKeyValue<V> {
        return QPTrieKeyValue(bytes, value)
    }

    @Test fun iteratorForLessThanOrEqualTwoElementTrie() {
        val lowerBytes = byteArrayOf()
        val higherBytes = byteArrayOf(0)
        val trie = QPTrie(listOf(
            lowerBytes to "lower",
            higherBytes to "higher"
        ))

        val firstResults = fixIteratorForInvariants(trie.iteratorLessThanOrEqualUnsafeSharedKey(lowerBytes))
        assertListOfByteArrayValuePairsEquals(
            fixIteratorForInvariants(arrayOf(
                this.qpTrieKeyValueForBytes(lowerBytes, "lower")
            ).iterator()),
            firstResults
        )
        val secondResults = fixIteratorForInvariants(trie.iteratorLessThanOrEqualUnsafeSharedKey(higherBytes))
        assertListOfByteArrayValuePairsEquals(
            fixIteratorForInvariants(arrayOf(
                this.qpTrieKeyValueForBytes(higherBytes, "higher"),
                this.qpTrieKeyValueForBytes(lowerBytes, "lower")
            ).iterator()),
            secondResults
        )
    }

    @Test fun iteratorForGreaterThanOrEqualTwoElementTrie() {
        val lowerBytes = byteArrayOf()
        val higherBytes = byteArrayOf(0)
        val trie = QPTrie(listOf(
            lowerBytes to "lower",
            higherBytes to "higher"
        ))

        val firstResults = fixIteratorForInvariants(trie.iteratorGreaterThanOrEqualUnsafeSharedKey(lowerBytes))
        assertListOfByteArrayValuePairsEquals(
            fixIteratorForInvariants(listOf(
                this.qpTrieKeyValueForBytes(lowerBytes, "lower"),
                this.qpTrieKeyValueForBytes(higherBytes, "higher")
            ).iterator()),
            firstResults
        )

        val secondResults = fixIteratorForInvariants(trie.iteratorGreaterThanOrEqualUnsafeSharedKey(higherBytes))
        assertListOfByteArrayValuePairsEquals(
            fixIteratorForInvariants(listOf(
                this.qpTrieKeyValueForBytes(higherBytes, "higher")
            ).iterator()),
            secondResults
        )
    }

    @Test fun removingNonexistentResultsInSameInstance() {
        val shortEntry = byteArrayOf(-128)
        val longEntry = byteArrayOf(-128, 12, 13)
        val trie = QPTrie(listOf(
            shortEntry to "short",
            longEntry to "long"
        ))
        val removedTop = trie.remove(byteArrayOf())
        assertTrue(removedTop === trie)

        val removedMid = trie.remove(byteArrayOf(-128, 12))
        assertTrue(removedMid === trie)

        val removedBottom = trie.remove(byteArrayOf(-128, 12, 13, 14))
        assertTrue(removedBottom === trie)
    }

    @Test fun updatingToSameResultsInSameInstance() {
        val shortEntry = byteArrayOf(-128)
        val longEntry = byteArrayOf(-128, 12, 13)
        val longString = "long"
        val trie = QPTrie(listOf(
            shortEntry to "short",
            longEntry to longString,
        ))
        val updatedTrie = trie.update(longEntry) { longString }
        assert(updatedTrie === trie)
    }

    @Test fun sanityCheckByteArrayPairs() {
        // We can't actually use a Map here; it uses hashing semantics for
        // equality that aren't well documented.
        val arr1 = ByteArrayButComparable(byteArrayOf())
        val arr2 = ByteArrayButComparable(byteArrayOf())
        val map = IntervalTree(listOf(
            Pair(arr1, arr1) to "bad",
            Pair(arr2, arr2) to "good",
        ))
        assertEquals(
            "good",
            map.lookupExactRange(
                Pair(
                    ByteArrayButComparable(byteArrayOf()),
                    ByteArrayButComparable(byteArrayOf())
                )
            )
        )
    }

    @Provide
    fun trieTestSpecs(): Arbitrary<List<Pair<PublicByteArrayButComparable, String>>> {
        return Arbitraries.integers().between(0, 256).flatMap { specSize ->
            Arbitraries.bytes().list().ofMaxSize(64).flatMap { byteList ->
                Arbitraries.strings().map { Pair(PublicByteArrayButComparable(ByteArrayButComparable(byteList.toByteArray())), it) }
            }.list().ofMaxSize(specSize)
        }
    }

    @Provide
    fun testTrieSpecsWithRemovalOffset(): Arbitrary<Pair<List<Pair<PublicByteArrayButComparable, String>>, Int>> {
        return this.trieTestSpecs().flatMap { spec ->
            Arbitraries.integers().between(0, spec.size / 2).map { reverseSplitPoint ->
                Pair(spec, reverseSplitPoint)
            }
        }
    }

    @Property
    fun trieLifecycleWithoutPrefixes(
        @ForAll @From("testTrieSpecsWithRemovalOffset") spec: Pair<List<Pair<PublicByteArrayButComparable, String>>, Int>
    ) {
        val initialEntries = spec.first.toTypedArray()

        var distinct = IntervalTree<ByteArrayButComparable, String>()
        for ((publicByteArray, value) in initialEntries) {
            distinct = distinct.put(Pair(publicByteArray.actual, publicByteArray.actual), value)
        }
        var trie = QPTrie<String>()
        var expectedSize = 0
        for ((key, value) in distinct) {
            trie = trie.put(key.lowerBound.array, value)
            expectedSize += 1
        }
        assertEquals(expectedSize.toLong(), trie.size)
        for ((key, value) in distinct) {
            val found = trie.get(key.lowerBound.array)
            assertEquals(value, found)
        }
        verifyIteratorInvariants(trie, distinct)
        var distinctRemoved = IntervalTree<ByteArrayButComparable, String>()
        for (i in 0 until spec.second) {
            val (publicArray, value) = initialEntries[initialEntries.size - i - 1]
            val actual = publicArray.actual
            distinct = distinct.remove(Pair(actual, actual))
            distinctRemoved = distinctRemoved.put(Pair(actual, actual), value)
            trie = trie.remove(actual.array)
        }
        expectedSize -= distinctRemoved.size.toInt()
        assertEquals(expectedSize.toLong(), trie.size)

        for ((key, value) in distinct) {
            val found = trie.get(key.lowerBound.array)
            assertEquals(value, found)
        }
        for (i in 0 until spec.second) {
            val (publicArray) = initialEntries[initialEntries.size - i - 1]
            assertNull(trie.get(publicArray.actual.array))
        }
        verifyIteratorInvariants(trie, distinct)
    }

    @Provide fun prefixLookupsSchedule(): Arbitrary<List<Pair<ByteArray, String>>> {
        val runLengths = Arbitraries.integers().between(1, 10)
            .array(IntArray::class.java).ofMinSize(1).ofMaxSize(5)
        val keys = runLengths.list().ofMinSize(1).ofMaxSize(16).map { lookupSchedule ->
            val byteArrays = mutableListOf<ByteArray>()
            for (i in 0 until lookupSchedule.size) {
                val workingByteList = mutableListOf<Byte>()
                var curByte = i
                for (runLength in lookupSchedule[i]) {
                    for (j in 0 until runLength) {
                        workingByteList.add(curByte.toByte())
                        curByte += 1
                    }
                    byteArrays.add(workingByteList.toByteArray())
                }
            }
            byteArrays.toList()
        }
        return keys.flatMap { reifiedKeys ->
            Arbitraries.strings().list().ofMinSize(reifiedKeys.size).ofMaxSize(reifiedKeys.size).map { values ->
                reifiedKeys.zip(values)
            }
        }
    }

    private fun groupPrefixLookupSchedule(schedule: List<Pair<ByteArray, String>>): Map<Byte, List<Pair<ByteArray, String>>> {
        val withSortOrder = schedule.sortedBy { ByteArrayButComparable(it.first) }
        val workingMap = mutableMapOf<Byte, MutableList<Pair<ByteArray, String>>>()
        for (item in withSortOrder) {
            val firstByte = item.first[0]
            var prior = workingMap[firstByte]
            if (prior == null) {
                prior = mutableListOf()
                workingMap[firstByte] = prior
            }
            prior.add(item)
        }
        val resultingMap = mutableMapOf<Byte, List<Pair<ByteArray, String>>>()
        for (kvp in workingMap) {
            resultingMap[kvp.key] = kvp.value.toList()
        }
        return resultingMap.toMap()
    }

    @Property
    fun prefixComparisons(
        @ForAll @From("prefixLookupsSchedule") schedule: List<Pair<ByteArray, String>>
    ) {
        val trie = QPTrie(schedule.shuffled())
        val grouped = this.groupPrefixLookupSchedule(schedule)
        for (kvp in grouped) {
            val targets = kvp.value
            for (i in targets.indices) {
                val lookupKey = targets[i].first
                // First, everything that starts with this current entry.
                val expectedStartsWith = fixIteratorForInvariants(
                    targets.subList(i, targets.size).map {
                        (key, value) -> this.qpTrieKeyValueForBytes(key, value)
                    } .iterator()
                )
                val receivedStartsWith = fixIteratorForInvariants(trie.iteratorStartsWithUnsafeSharedKey(lookupKey))
                assertListOfByteArrayValuePairsEquals(expectedStartsWith, receivedStartsWith)
                // Second, everything this current entry starts with.
                val expectedPrefixOf = fixIteratorForInvariants(
                    targets.subList(0, i+1).map {
                        (key, value) -> this.qpTrieKeyValueForBytes(key, value)
                    } .iterator()
                )
                val visitedStartsWith = ArrayList<Pair<ByteArrayButComparable, String>>()
                trie.visitStartsWithUnsafeSharedKey(lookupKey) {
                    visitedStartsWith.add(ByteArrayButComparable(it.key) to it.value)
                }
                assertListOfByteArrayValuePairsEquals(expectedStartsWith, visitedStartsWith)

                val receivedPrefixOf = fixIteratorForInvariants(trie.iteratorPrefixOfOrEqualToUnsafeSharedKey(lookupKey))
                assertListOfByteArrayValuePairsEquals(expectedPrefixOf, receivedPrefixOf)
                val visitedPrefixOf = ArrayList<Pair<ByteArrayButComparable, String>>()
                trie.visitPrefixOfOrEqualToUnsafeSharedKey(lookupKey) {
                    visitedPrefixOf.add(ByteArrayButComparable(it.key) to it.value)
                }
                assertListOfByteArrayValuePairsEquals(expectedPrefixOf, visitedPrefixOf)
            }
        }
    }

    @Test
    fun emptyPrefixComparisons() {
        val trie = QPTrie(listOf(byteArrayOf(12) to byteArrayOf(13)))

        val lookupStartingWith = trie.iteratorPrefixOfOrEqualToUnsafeSharedKey(byteArrayOf()).asSequence().toList()
        assertEquals(0, lookupStartingWith.size)

        val lookupStartsWith = trie.iteratorStartsWithUnsafeSharedKey(byteArrayOf()).asSequence().toList()
        assertEquals(1, lookupStartsWith.size)
        assertArrayEquals(byteArrayOf(12), lookupStartsWith.first().key)
        assertArrayEquals(byteArrayOf(13), lookupStartsWith.first().value)
    }

    @Test
    fun emptyPrefixLessThanIterator() {
        val trie = QPTrie(listOf(byteArrayOf(12) to byteArrayOf(13)))
        val lookupLessThan = trie.iteratorLessThanOrEqualUnsafeSharedKey(byteArrayOf()).asSequence().toList()
        assertEquals(0, lookupLessThan.size)
    }

    @Test
    fun emptyPrefixGreaterThanIterator() {
        val trie = QPTrie(listOf(byteArrayOf() to byteArrayOf(13)))
        val lookupGreaterThan = trie.iteratorGreaterThanOrEqualUnsafeSharedKey(byteArrayOf(12)).asSequence().toList()
        assertEquals(0, lookupGreaterThan.size)

        val trieAgain = trie.put(byteArrayOf(12), byteArrayOf(14))
        val lookupGreaterThanAgain = trieAgain.iteratorGreaterThanOrEqualUnsafeSharedKey(byteArrayOf(12)).asSequence().toList()
        assertEquals(1, lookupGreaterThanAgain.size)
        assertArrayEquals(byteArrayOf(12), lookupGreaterThanAgain.first().key)
        assertArrayEquals(byteArrayOf(14), lookupGreaterThanAgain.first().value)

        val lookupWithEmpty = trieAgain.iteratorGreaterThanOrEqualUnsafeSharedKey(byteArrayOf()).asSequence().toList()
        assertEquals(2, lookupWithEmpty.size)

        val trieNonEmpty = QPTrie(listOf(byteArrayOf(12) to byteArrayOf(14)))
        val lookupNonEmptyWithEmpty = trieNonEmpty.iteratorGreaterThanOrEqualUnsafeSharedKey(byteArrayOf()).asSequence().toList()
        assertEquals(1, lookupNonEmptyWithEmpty.size)
    }

    @Test
    fun lessThanIteratorTwoCases() {
        // Keep in mind that iteratorLessThanOrEqual is descending.
        val targetPairs = listOf(
            byteArrayOf(-13, 24, 19, 46, -1, 17) to 2,
            byteArrayOf(-16, 64, -2, -14) to 1,
        )
        val trie = QPTrie(targetPairs)
        val lookupLess = trie.iteratorLessThanOrEqualUnsafeSharedKey(
            byteArrayOf(-1, 5, -80, 11, -50, 45, -16, 117, 13, 2, -5, -79)
        ).asSequence().toList()
        assertListOfByteArrayValuePairsEquals(
            targetPairs.map { (key, value) -> ByteArrayButComparable(key) to value },
            lookupLess.map { (key, value) -> ByteArrayButComparable(key) to value }
        )
    }

    @Test
    fun mutatingSingleKeyDoesntAffectTrie() {
        val targetKey = byteArrayOf(0)
        var trie = QPTrie(listOf(targetKey to 7))

        assertEquals(7, trie.get(byteArrayOf(0)))
        targetKey[0] = 1
        assertEquals(7, trie.get(byteArrayOf(0)))

        val updateKey = byteArrayOf(2)
        trie = trie.put(updateKey, 8)
        assertEquals(7, trie.get(byteArrayOf(0)))
        assertEquals(8, trie.get(byteArrayOf(2)))

        updateKey[0] = 3
        assertEquals(7, trie.get(byteArrayOf(0)))
        assertEquals(8, trie.get(byteArrayOf(2)))
    }

    @Test
    fun visitGreaterThanWithOnlyTwoElements() {
        val greaterArray = byteArrayOf(
            57, 9, -111, 37, 115, 7, -66, -20,
            95, 106, 78, -13, 38, -95, -7, 35,
            -39, -49, 113, 17, -37, -64, 84, 14,
            -37, 52, 8, 19, 18, -80
        )
        val trie = QPTrie(listOf(
            byteArrayOf(0) to "",
            greaterArray to "greater"
        ))

        val greaterReceived = ArrayList<Pair<ByteArrayButComparable, String>>()
        trie.visitGreaterThanOrEqualUnsafeSharedKey(greaterArray) {
            greaterReceived.add(ByteArrayButComparable(it.key) to it.value)
        }
        val expectedReceived = listOf(ByteArrayButComparable(greaterArray) to "greater")
        for (i in 0 until greaterReceived.size.coerceAtLeast(expectedReceived.size)) {
            if (i < greaterReceived.size) {
                val thing = greaterReceived[i]
                println("OK we looked at ${thing.first} -> ${thing.second}")
            } else {
                println("We did not have a pair for $i")
            }
            if (i < expectedReceived.size) {
                val expectedThing = expectedReceived[i]
                println("But we should have had ${expectedThing.first} -> ${expectedThing.second}")
            } else {
                println("But we should not have had anything at $i")
            }
        }
        assertListOfByteArrayValuePairsEquals(expectedReceived, greaterReceived)
    }

    @Test fun visitGreaterThanWithEmptyKey() {
        val trie = QPTrie(listOf(byteArrayOf(0) to byteArrayOf()))
        var visitResult: QPTrieKeyValue<ByteArray>? = null
        var visitCalls = 0
        trie.visitGreaterThanOrEqualUnsafeSharedKey(byteArrayOf()) {
            visitResult = it
            visitCalls++
        }
        assertNotNull(visitResult)
        assertEquals(1, visitCalls)
        assertArrayEquals(byteArrayOf(0), visitResult!!.key)
        assertArrayEquals(byteArrayOf(), visitResult!!.value)
    }

    @Test fun visitGreaterThanSingleExample() {
        val trie = QPTrie(listOf(byteArrayOf(0, 0, 0, -41, 28, 7) to byteArrayOf()))
        var visitCalls = 0
        trie.visitGreaterThanOrEqualUnsafeSharedKey(byteArrayOf(106, -10, -7, -52, 3, 2)) {
            visitCalls++
        }
        assertEquals(0, visitCalls)
    }

    @Test fun visitLessThanSingleExample() {
        val trie = QPTrie(listOf(byteArrayOf(0, 0, 3) to byteArrayOf()))
        var visitCalls = 0
        var visitResult: QPTrieKeyValue<ByteArray>? = null
        trie.visitLessThanOrEqualUnsafeSharedKey(byteArrayOf(-22)) {
            visitCalls++
            visitResult = it
        }
        assertNotNull(visitResult)
        assertEquals(1, visitCalls)
        assertArrayEquals(byteArrayOf(0, 0, 3), visitResult!!.key)
        assertArrayEquals(byteArrayOf(), visitResult!!.value)
    }

    @Test fun visitGreaterThanAgainWithEmptyKey() {
        val trie = QPTrie(listOf(
            byteArrayOf() to 1,
            byteArrayOf(0) to 2
        ))
        val seenEntries = ArrayList<Int>()
        trie.visitGreaterThanOrEqualUnsafeSharedKey(byteArrayOf()) {
            if (it.key.contentEquals(byteArrayOf())) {
                assertEquals(1, it.value)
            }
            if (it.key.contentEquals(byteArrayOf(0))) {
                assertEquals(2, it.value)
            }
            seenEntries.add(it.value)
        }
        assertArrayEquals(arrayOf(1, 2), seenEntries.toTypedArray())
    }

    @Test fun visitLessThanInvolvingEmptyKey() {
        val trie = QPTrie(listOf(
            byteArrayOf() to 1,
            byteArrayOf(0) to 2
        ))
        val seenEntries = ArrayList<Int>()
        trie.visitLessThanOrEqualUnsafeSharedKey(byteArrayOf(4, -49, 127, -13, -111)) {
            if (it.key.contentEquals(byteArrayOf())) {
                assertEquals(1, it.value)
            }
            if (it.key.contentEquals(byteArrayOf(0))) {
                assertEquals(2, it.value)
            }
            seenEntries.add(it.value)
        }
        assertArrayEquals(arrayOf(2, 1), seenEntries.toTypedArray())
    }

    @Test fun keyValueEquality() {
        val leftPair = QPTrieKeyValue(byteArrayOf(0, 1, 2), 1)
        val rightPair = QPTrieKeyValue(byteArrayOf(0, 1, 2), 1)
        val middlePair = QPTrieKeyValue(byteArrayOf(2, 3, 4), 2)
        val middlePairWrongKey = QPTrieKeyValue(byteArrayOf(2, 3, 4), "something")

        assertTrue(leftPair.equals(leftPair))
        assertEquals(leftPair.hashCode(), leftPair.hashCode())
        assertTrue(rightPair.equals(rightPair))
        assertEquals(rightPair.hashCode(), rightPair.hashCode())
        assertTrue(middlePair.equals(middlePair))
        assertEquals(middlePair.hashCode(), middlePair.hashCode())
        assertTrue(middlePairWrongKey.equals(middlePairWrongKey))
        assertEquals(middlePairWrongKey.hashCode(), middlePairWrongKey.hashCode())

        assertTrue(leftPair == rightPair)
        assertTrue(rightPair == leftPair)
        assertEquals(leftPair.hashCode(), rightPair.hashCode())
        assertFalse(leftPair == middlePair)
        assertFalse(middlePair == leftPair)
        assertFalse(middlePair == middlePairWrongKey)
        assertFalse(middlePairWrongKey == middlePair)
    }

    @Test fun removeNotSetOddNybble() {
        val trie = QPTrie(listOf(
            byteArrayOf(0) to 0,
            byteArrayOf(1) to 1
        ))
        val removedTrie = trie.remove(byteArrayOf())
        assertTrue(trie === removedTrie)
        assertEquals(2, removedTrie.size)
        assertEquals(0, trie.get(byteArrayOf(0)))
        assertEquals(1, trie.get(byteArrayOf(1)))
        assertNull(trie.get(byteArrayOf()))
    }

    @Test fun startsWithForLesserEntry() {
        val trie = QPTrie(listOf(
            byteArrayOf(64) to 1
        ))
        val startsWithZero = trie.iteratorStartsWithUnsafeSharedKey(byteArrayOf(0)).asSequence().toList()
        assertEquals(0, startsWithZero.size)

        var sawEntry = false
        trie.visitStartsWithUnsafeSharedKey(byteArrayOf(0)) {
            sawEntry = true
        }
        assertFalse(sawEntry)
    }

    @Test fun startsWithForPrefixSupersetEntry() {
        val trie = QPTrie(listOf(
            byteArrayOf(1, 2, 3, 4) to 5
        ))
        val startsWithJustFive = trie.iteratorStartsWithUnsafeSharedKey(byteArrayOf(1, 2, 3, 4, 5)).asSequence().toList()
        assertEquals(0, startsWithJustFive.size)

        var sawEntry = false
        trie.visitStartsWithUnsafeSharedKey(byteArrayOf(1, 2, 3, 4, 5)) {
            sawEntry = true
        }
        assertFalse(sawEntry)
    }
}