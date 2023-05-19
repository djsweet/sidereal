package name.djsweet.query.tree

import org.junit.jupiter.api.Assertions.*
import net.jqwik.api.*
import net.jqwik.api.arbitraries.ListArbitrary
import org.junit.jupiter.api.Test
import java.util.*
import kotlin.collections.ArrayList

class DataUtilsTest {
    companion object {
        private fun qpTrieToPairList(qpTrie: QPTrie<ByteArray>): ArrayList<Pair<ByteArrayButComparable, ByteArrayButComparable>> {
            val result = arrayListOf<Pair<ByteArrayButComparable, ByteArrayButComparable>>()
            for ((key, value) in qpTrie) {
                result.add(Pair(ByteArrayButComparable(key), ByteArrayButComparable(value)))
            }
            return result
        }
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
            for ((key) in uniques) {
                uniquesList.add(key)
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
        val (shared, data, dispatch) = spec

        val resultingSameKey = workingDataForAvailableKeys(data, dispatch)
        val sharedList = qpTrieToPairList(shared)
        val resultingList = qpTrieToPairList(resultingSameKey)
        assertListOfByteArrayValuePairsEquals(sharedList, resultingList)
    }

    @Property
    fun gettingWorkingDataForAvailableKeysFromNull(
        @ForAll @From("sharedKeyValueSpec") spec: Triple<QPTrie<ByteArray>, QPTrie<ByteArray>, QPTrie<ByteArray>>
    ) {
        val data = spec.second
        val resulting = workingDataForAvailableKeys<Int>(data, null)
        assertEquals(0, resulting.size)
    }

    @Test
    fun comparingBytesButUnsigned() {
        // Sanity check for the internals of compareBytesUnsigned
        assertEquals(0, compareBytesUnsigned(0, 0))
        assertEquals(0, compareBytesUnsigned(-1, -1))
        assertEquals(0, compareBytesUnsigned(-2, -2))
        assertEquals(0, compareBytesUnsigned(1, 1))
        assertTrue(compareBytesUnsigned(0, -1) < 0)
        assertTrue(compareBytesUnsigned(-1, 0) > 0)
        assertTrue(compareBytesUnsigned(1, -1) < 0)
        assertTrue(compareBytesUnsigned(-1, 1) > 0)
        assertTrue(compareBytesUnsigned(-1, -2) > 0)
        assertTrue(compareBytesUnsigned(-2, -1) < 0)
        assertTrue(compareBytesUnsigned(0, 1) < 0)
        assertTrue(compareBytesUnsigned(1, 0) > 0)
        assertTrue(compareBytesUnsigned(1, 2) < 0)
        assertTrue(compareBytesUnsigned(2, 1) > 0)
        assertTrue(compareBytesUnsigned(-1, -72) > 0)
        assertTrue(compareBytesUnsigned(-72, -1) < 0)
        assertTrue(Arrays.compareUnsigned(byteArrayOf(-1), byteArrayOf(-72)) >= 1)
        assertTrue(Arrays.compareUnsigned(byteArrayOf(-2), byteArrayOf()) >= 1)
    }
}