package name.djsweet.query.tree

import org.junit.jupiter.api.Assertions.*
import net.jqwik.api.*
import net.jqwik.api.arbitraries.ListArbitrary

class DataUtilsTest {
    companion object {
        private fun qpTrieToPairList(qpTrie: QPTrie<ByteArray>): ArrayList<Pair<ByteArrayButComparable, ByteArrayButComparable>> {
            val result = arrayListOf<Pair<ByteArrayButComparable, ByteArrayButComparable>>()
            for (kvp in qpTrie) {
                result.add(Pair(ByteArrayButComparable(kvp.first), ByteArrayButComparable(kvp.second)))
            }
            return result
        }
    }

    @Property
    fun mapSequenceForIterable(
        @ForAll ints: List<Int>
    ) {
        val expectedInts = ArrayList<Int>()
        for (orig in ints) {
            expectedInts.add(orig * 2)
        }
        val givenIntsIterable = mapSequence(ints) { it * 2 }
        val givenInts = ArrayList<Int>()
        for (given in givenIntsIterable) {
            givenInts.add(given)
        }
        assertArrayEquals(expectedInts.toTypedArray(), givenInts.toTypedArray())
    }

    @Property
    fun mapSequenceForIterator(
        @ForAll ints: List<Int>
    ) {
        val expectedInts = ArrayList<Int>()
        for (orig in ints) {
            expectedInts.add(orig * 2)
        }
        val givenIntsIterable = mapSequence(ints.iterator()) { it * 2 }
        val givenInts = ArrayList<Int>()
        for (given in givenIntsIterable) {
            givenInts.add(given)
        }
        assertArrayEquals(expectedInts.toTypedArray(), givenInts.toTypedArray())
    }

    @Provide
    fun byteArrayList(): ListArbitrary<ByteArray> {
        return Arbitraries.bytes().array(ByteArray::class.java).list()
    }

    @Provide
    fun sharedKeyValueSpec(): Arbitrary<Triple<QPTrie<ByteArray>, QPTrie<ByteArray>, QPTrie<ByteArray>>> {
        return this.byteArrayList().map { byteArrays ->
            val uniques = QPTrie(byteArrays.map { Pair(it, true) })
            val uniquesList = arrayListOf<ByteArray>()
            for (kvp in uniques) {
                uniquesList.add(kvp.first)
            }
            uniquesList
        }.flatMap { availableKeys ->
            this.byteArrayList().ofMinSize(availableKeys.size).ofMaxSize(availableKeys.size).map {
                val keysAndValues = ArrayList<Pair<ByteArray, ByteArray>>()
                for (index in it.indices) {
                    keysAndValues.add(Pair(availableKeys[index], it[index]))
                }
                keysAndValues
            }
        }.flatMap { availableKeyValuePairs ->
            Arbitraries.integers().between(0, availableKeyValuePairs.size).flatMap { sharedCount ->
                Arbitraries.integers().between(0, availableKeyValuePairs.size - sharedCount).map { dataCount ->
                    var shared = QPTrie<ByteArray>()
                    var data = QPTrie<ByteArray>()
                    var keyDispatch = QPTrie<ByteArray>()
                    for (i in 0 until sharedCount) {
                        shared = shared.put(availableKeyValuePairs[i].first, availableKeyValuePairs[i].second)
                        data = data.put(availableKeyValuePairs[i].first, availableKeyValuePairs[i].second)
                        keyDispatch = keyDispatch.put(availableKeyValuePairs[i].first, availableKeyValuePairs[i].second)
                    }
                    for (i in sharedCount until (sharedCount + dataCount)) {
                        data = data.put(availableKeyValuePairs[i].first, availableKeyValuePairs[i].second)
                    }
                    for (i in (sharedCount + dataCount) until availableKeyValuePairs.size) {
                        keyDispatch = keyDispatch.put(availableKeyValuePairs[i].first, availableKeyValuePairs[i].second)
                    }
                    Triple(shared, data, keyDispatch)
                }
            }
        }
    }

    @Property
    fun gettingWorkingDataForAvailableKeys(
        @ForAll @From("sharedKeyValueSpec") spec: Triple<QPTrie<ByteArray>, QPTrie<ByteArray>, QPTrie<ByteArray>>
    ) {
        val shared = spec.first
        val data = spec.second
        val dispatch = spec.third

        val resulting = workingDataForAvailableKeys(data, dispatch)
        val sharedList = qpTrieToPairList(shared)
        val resultingList = qpTrieToPairList(resulting)
        assertListOfByteArrayValuePairsEquals(sharedList, resultingList)
    }
}