// SPDX-FileCopyrightText: 2023 Dani Sweet <thorium@djsweet.name>
//
// SPDX-License-Identifier: MIT

package name.djsweet.thorium.servers

import io.vertx.kotlin.core.json.get
import io.vertx.kotlin.core.json.jsonArrayOf
import io.vertx.kotlin.core.json.jsonObjectOf
import name.djsweet.thorium.GlobalCounterContext
import name.djsweet.thorium.Radix64JsonEncoder
import name.djsweet.thorium.Radix64LowLevelEncoder
import name.djsweet.thorium.encodeJsonToQueryableData
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class JsonToQueryableConversionReferencePathsTest {
    private val byteBudget = 1024
    private val recurseDepth = 5

    @Test
    fun arrayOffsetKeyMatchingInPaths() {
        val counters = GlobalCounterContext(1)
        val testData = jsonObjectOf(
            "not-array" to "some-string-value",
            "immediate-array" to jsonArrayOf(1, 2, 3, 4, 5),
            "sub-object" to jsonObjectOf(
                "sub-not-array" to 5,
                "sub-array" to jsonArrayOf("one", "two", "three"),
                "array-with-objects" to jsonArrayOf(
                    jsonObjectOf(
                        "first" to 1,
                        "second" to 2
                    ),
                    jsonObjectOf(
                        "first" to 3,
                        "second" to 4
                    ),
                    jsonObjectOf(
                        "first" to 5,
                        "second" to 6
                    )
                )
            ),
            "skipped-array" to jsonArrayOf("four", "five", "six"),
        )
        val channel = "test-channel"
        val queryReferences = mutableListOf(
            listOf("not-array") to 1,
            listOf("sub-object", "sub-not-array") to 1,
            listOf("sub-object", "sub-array", "1") to 1,
            listOf("sub-object", "array-with-objects", "0", "second") to 1,
            listOf("immediate-array", "2") to 1
        )
        counters.updateKeyPathReferenceCountsForChannel(channel, queryReferences)
        val refs1 = counters.getKeyPathReferenceCountsForChannel(channel)!!
        val filterContext1 = OnlyPathsWithReferencesFilterContext(refs1)

        val queryEncoded1 = encodeJsonToQueryableData(testData, filterContext1, this.byteBudget, this.recurseDepth)
        val scalarTrie1 = queryEncoded1.scalars.trie
        assertEquals(5L, scalarTrie1.size)

        val notArrayEncodedKey = Radix64LowLevelEncoder().addString("not-array").encode()
        val subObjectKeyEncoder = Radix64LowLevelEncoder().addString("sub-object")
        val subNotArrayEncodedKey = subObjectKeyEncoder.clone().addString("sub-not-array").encode()
        val subArrayEncodedKey = subObjectKeyEncoder.clone()
            .addString("sub-array")
            .addString("1")
            .encode()
        val arrayWithObjectsEncodedKey = subObjectKeyEncoder.clone()
            .addString("array-with-objects")
            .addString("0")
            .addString("second")
            .encode()
        val immediateArrayEncodedKey = Radix64LowLevelEncoder()
            .addString("immediate-array")
            .addString("2")
            .encode()

        val notArrayEncodedValue = Radix64JsonEncoder().addString(testData["not-array"], this.byteBudget).encode()
        val subNotArrayEncodedValue = Radix64JsonEncoder().addNumber(5.0).encode()
        val subArrayEncodedValue = Radix64JsonEncoder().addString(
            testData.getJsonObject("sub-object")?.getJsonArray("sub-array")?.getString(1)!!,
            this.byteBudget
        ).encode()
        val arrayWithObjectsEncodedValue = Radix64JsonEncoder().addNumber(2.0).encode()
        val immediateArrayEncodedValue = Radix64JsonEncoder().addNumber(3.0).encode()

        assertEquals(notArrayEncodedValue.toList(), scalarTrie1.get(notArrayEncodedKey)?.toList())
        assertEquals(subNotArrayEncodedValue.toList(), scalarTrie1.get(subNotArrayEncodedKey)?.toList())
        assertEquals(subArrayEncodedValue.toList(), scalarTrie1.get(subArrayEncodedKey)?.toList())
        assertEquals(arrayWithObjectsEncodedValue.toList(), scalarTrie1.get(arrayWithObjectsEncodedKey)?.toList())
        assertEquals(immediateArrayEncodedValue.toList(), scalarTrie1.get(immediateArrayEncodedKey)?.toList())

        // Now we do the same, but without the deepest array inspection
        val queryReferencesDecrement = mutableListOf(
            listOf("not-array") to -1,
            listOf("sub-object", "array-with-objects", "0", "second") to -1
        )
        counters.updateKeyPathReferenceCountsForChannel(channel,queryReferencesDecrement)
        val refs2 = counters.getKeyPathReferenceCountsForChannel(channel)!!
        val filterContext2 = OnlyPathsWithReferencesFilterContext(refs2)

        val queryEncoded2 = encodeJsonToQueryableData(testData, filterContext2, this.byteBudget, this.recurseDepth)
        val scalarTrie2 = queryEncoded2.scalars.trie
        assertEquals(3L, scalarTrie2.size)

        assertEquals(subNotArrayEncodedValue.toList(), scalarTrie1.get(subNotArrayEncodedKey)?.toList())
        assertEquals(subArrayEncodedValue.toList(), scalarTrie1.get(subArrayEncodedKey)?.toList())
        assertEquals(immediateArrayEncodedValue.toList(), scalarTrie1.get(immediateArrayEncodedKey)?.toList())
    }
}