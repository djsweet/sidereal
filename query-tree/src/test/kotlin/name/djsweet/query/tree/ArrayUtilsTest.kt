// SPDX-FileCopyrightText: 2023 Dani Sweet <sidereal@djsweet.name>
//
// SPDX-License-Identifier: MIT

package name.djsweet.query.tree

import org.junit.jupiter.api.Test

import org.junit.jupiter.api.Assertions.*
import net.jqwik.api.*

private fun slowEqualUpTo(left: ByteArray, right: ByteArray, rightStart: Int, rightEntries: Int): Int {
    val maxSize = left.size.coerceAtMost(rightEntries).coerceAtMost(right.size - rightStart)
    var equalUpTo = 0
    for (i in 0 until maxSize) {
        if (left[i] == right[i + rightStart]) {
            equalUpTo += 1
        } else {
            break
        }
    }
    return equalUpTo
}

class ArrayUtilsTest {
    @Test fun byteArraysEqualUpToZeroForTwoEmptyArrays() {
        val leftEmpty = ByteArray(0)
        val rightEmpty = ByteArray(0)
        assertEquals(
            0,
            byteArraysEqualUpTo(leftEmpty, 0, 0, rightEmpty, 0, 0)
        )
    }

    @Test fun byteArraysEqualUpToZeroForLeftEmptyArrays() {
        val leftArray = ByteArray(0)
        val rightArray = byteArrayOf(0x1, 0x2, 0x3)
        assertEquals(
            0,
            byteArraysEqualUpTo(leftArray, 0, leftArray.size, rightArray, 0, rightArray.size)
        )
    }

    @Test fun byteArraysEqualUpToZeroForRightEmptyArrays() {
        val leftArray = byteArrayOf(0x1, 0x2, 0x3)
        val rightArray = ByteArray(0)
        assertEquals(
            0,
            byteArraysEqualUpTo(leftArray, 0, leftArray.size, rightArray, 0, rightArray.size)
        )
    }

    @Property
    fun equalByteArraysAlwaysEqualUpToLength(@ForAll target: ByteArray) {
        val copyTarget = target.clone()
        assertEquals(
            target.size,
            byteArraysEqualUpTo(target, 0, target.size, copyTarget, 0, copyTarget.size)
        )
    }

    @Property
    fun byteArraysEqualUpToForFullArrays(
        @ForAll left: ByteArray,
        @ForAll right: ByteArray
    ) {
        val equalUpTo = slowEqualUpTo(left, right, 0, right.size)
        assertEquals(
            equalUpTo,
            byteArraysEqualUpTo(left,0, left.size , right, 0, right.size)
        )
    }

    @Provide
    fun rightArrayFromZeroWithLengthSubset(): Arbitrary<Pair<ByteArray, Int>> {
        return Arbitraries.bytes().list().flatMap { byteList ->
            val byteArray = byteList.toByteArray()
            Arbitraries.integers().between(0, byteArray.size).map { size -> Pair(byteArray, size) }
        }
    }

    @Property
    fun byteArraysEqualUpToForPartialRightFromStart(
        @ForAll left: ByteArray,
        @ForAll @From("rightArrayFromZeroWithLengthSubset") rightSpec: Pair<ByteArray, Int>
    ) {
        val right = rightSpec.first
        val rightSize = rightSpec.second
        val equalUpTo = slowEqualUpTo(left, right, 0, rightSize)
        assertEquals(
            equalUpTo,
            byteArraysEqualUpTo(left, 0, left.size, right, 0, rightSize)
        )
    }

    @Provide
    fun rightArrayFromSomewhereWithLengthSubset(): Arbitrary<Triple<ByteArray, Int, Int>> {
        return Arbitraries.bytes().list().flatMap {byteList ->
            val byteArray = byteList.toByteArray()
            Arbitraries.integers().between(0, byteArray.size).flatMap { lowerBound ->
                Arbitraries.integers().between(lowerBound, byteArray.size).map {upperBound ->
                    Triple(byteArray, lowerBound, upperBound - lowerBound)
                }
            }
        }
    }

    @Property
    fun byteArraysEqualUpToForPartialRightFromSomewhere(
        @ForAll left: ByteArray,
        @ForAll @From("rightArrayFromSomewhereWithLengthSubset") rightSpec: Triple<ByteArray, Int, Int>
    ) {
        val right = rightSpec.first
        val rightStart = rightSpec.second
        val rightSize = rightSpec.third
        val equalUpTo = slowEqualUpTo(left, right, rightStart, rightSize)
        assertEquals(
            equalUpTo,
            byteArraysEqualUpTo(left, 0, left.size, right, rightStart, rightSize)
        )
    }

    @Provide
    fun byteArraysForFindInsertRemove(): Arbitrary<Pair<ByteArray, Int>> {
        return Arbitraries.bytes().between(0, 126).flatMap { largestByte ->
            Arbitraries.integers().between(0, largestByte + 1).map { insertOffset ->
                val byteArray = ByteArray(largestByte + 1)
                for (i in byteArray.indices) {
                    byteArray[i] = i.toByte()
                }
                Pair(byteArray, insertOffset)
            }
        }
    }

    @Property
    fun findingKnownBytesInSortedArrays(
        @ForAll @From("byteArraysForFindInsertRemove") spec: Pair<ByteArray, Int>
    ) {
        val byteArray = spec.first
        val findOffset = spec.second.coerceAtMost(byteArray.size - 1)
        assertEquals(findOffset, findByteInSortedArray(byteArray, findOffset.toByte()))
    }

    @Property
    fun notFindingBytesBeyondAvailableBytes(
        @ForAll @From("byteArraysForFindInsertRemove") spec: Pair<ByteArray, Int>
    ) {
        val byteArray = spec.first
        val maxByte = byteArray.size.toByte()
        assertEquals(-(byteArray.size + 1), findByteInSortedArray(byteArray, maxByte))
    }

    @Property
    fun notFindingBytesLessThanAvailableBytes(
        @ForAll @From("byteArraysForFindInsertRemove") spec: Pair<ByteArray, Int>,
    ) {
        val shiftBytes = spec.first.map { (it + 1).toByte() }.toByteArray()
        assertEquals(-1, findByteInSortedArray(shiftBytes, 0))
    }

    @Provide
    fun evenOnlyByteArray(): Arbitrary<ByteArray> {
        return Arbitraries.bytes().between(0, 61).map {
            val byteArray = ByteArray(it.toInt())
            for (i in 0 until it) {
                byteArray[i] = (i * 2).toByte()
            }
            byteArray
        }
    }

    @Provide
    fun oddByte(): Arbitrary<Byte> {
        return Arbitraries.bytes().between(0, 63).map {
            (it * 2 + 1).toByte()
        }
    }

    @Property
    fun findByteInSortedArrayReportsWhereToPlaceByte(
        @ForAll @From("evenOnlyByteArray") evenBytes: ByteArray,
        @ForAll @From("oddByte") oddByte: Byte
    ) {
        val halfByte = (oddByte + 1) / 2
        val expectedPosition = -(halfByte.coerceAtMost(evenBytes.size) + 1)
        assertEquals(expectedPosition, findByteInSortedArray(evenBytes, oddByte))
    }

    @Provide
    fun sixteenElementSortedByteArray(): Arbitrary<ByteArray> {
        return Arbitraries.integers().between(0, 16).map {
            val byteArray = ByteArray(it)
            for (i in 0 until it) {
                byteArray[i] = i.toByte()
            }
            byteArray
        }
    }

    @Property
    fun trieUtilsOffsetForNybbleFindsElement(
        @ForAll @From("sixteenElementSortedByteArray") byteArray: ByteArray,
        @ForAll targetByte: Byte
    ) {
        val expectedOffset = (if (targetByte < 0 || targetByte >= byteArray.size) {
            -1
        } else {
            targetByte
        }).toInt()
        assertEquals(expectedOffset, QPTrieUtils.offsetForNybble(byteArray, targetByte))
    }

    @Property
    fun insertingArray(
        @ForAll leftArray: Array<String>,
        @ForAll middleBit: String,
        @ForAll rightArray: Array<String>
    ) {
        val fullArray = Array(leftArray.size + 1 + rightArray.size) { "" }
        leftArray.copyInto(fullArray)
        rightArray.copyInto(fullArray, leftArray.size + 1)
        fullArray[leftArray.size] = middleBit

        val missingArray = Array(leftArray.size + rightArray.size) { "" }
        leftArray.copyInto(missingArray)
        rightArray.copyInto(missingArray, leftArray.size)

        val targetArray = Array(leftArray.size + 1 + rightArray.size) { "" }
        insertArray(missingArray, targetArray, leftArray.size, middleBit)
        assertArrayEquals(fullArray, targetArray)
    }

    @Property
    fun insertingByteArray(
        @ForAll leftArray: ByteArray,
        @ForAll middleBit: Byte,
        @ForAll rightArray: ByteArray
    ) {
        val fullArray = ByteArray(leftArray.size + 1 + rightArray.size)
        leftArray.copyInto(fullArray)
        rightArray.copyInto(fullArray, leftArray.size + 1)
        fullArray[leftArray.size] = middleBit

        val missingArray = ByteArray(leftArray.size + rightArray.size)
        leftArray.copyInto(missingArray)
        rightArray.copyInto(missingArray, leftArray.size)

        val targetArray = insertByteArray(missingArray, leftArray.size, middleBit)
        assertArrayEquals(fullArray, targetArray)
    }

    @Property
    fun removingArray(
        @ForAll leftArray: Array<String>,
        @ForAll middleBit: String,
        @ForAll rightArray: Array<String>
    ) {
        val fullArray = Array(leftArray.size + 1 + rightArray.size) { "" }
        leftArray.copyInto(fullArray)
        rightArray.copyInto(fullArray, leftArray.size + 1)
        fullArray[leftArray.size] = middleBit

        val missingArray = Array(leftArray.size + rightArray.size) { "" }
        leftArray.copyInto(missingArray)
        rightArray.copyInto(missingArray, leftArray.size)

        val targetArray = Array(leftArray.size + rightArray.size) { "" }
        removeArray(fullArray, targetArray, leftArray.size)
        assertArrayEquals(missingArray, targetArray)
    }

    @Property
    fun removingByteArray(
        @ForAll leftArray: ByteArray,
        @ForAll middleBit: Byte,
        @ForAll rightArray: ByteArray
    ) {
        val fullArray = ByteArray(leftArray.size + 1 + rightArray.size)
        leftArray.copyInto(fullArray)
        rightArray.copyInto(fullArray, leftArray.size + 1)
        fullArray[leftArray.size] = middleBit

        val missingArray = ByteArray(leftArray.size + rightArray.size)
        leftArray.copyInto(missingArray)
        rightArray.copyInto(missingArray, leftArray.size)

        val targetArray = removeByteArray(fullArray, leftArray.size)
        assertArrayEquals(missingArray, targetArray)
    }

    @Property
    fun concatenatingWithMiddleByte(
        @ForAll leftArray: ByteArray,
        @ForAll middleBit: Byte,
        @ForAll rightArray: ByteArray
    ) {
        val fullArray = ByteArray(leftArray.size + 1 + rightArray.size)
        leftArray.copyInto(fullArray)
        rightArray.copyInto(fullArray, leftArray.size + 1)
        fullArray[leftArray.size] = middleBit

        assertArrayEquals(fullArray, concatByteArraysWithMiddleByte(leftArray, middleBit, rightArray))
    }

    @Property
    fun concatenatingReverseLists(
        @ForAll reverseEntries: List<ByteArray>
    ) {
        val entries = reverseEntries.reversed()
        val arraySize = entries.sumOf { it.size }
        val fullArray = ByteArray(arraySize)
        var insertOffset = 0
        for (entry in entries) {
            entry.copyInto(fullArray, insertOffset)
            insertOffset += entry.size
        }
        assertArrayEquals(fullArray, concatByteArraysFromReverseList(listFromIterable(reverseEntries)))
    }

    @Test
    fun byteArrayButComparableHashCode() {
        val testByteArray = byteArrayOf(1, 2, 3, 4)
        assertEquals(testByteArray.contentHashCode(), ByteArrayButComparable(testByteArray).hashCode())
    }

    @Test
    fun byteArrayButComparableDifferentTypeEquality() {
        val testByteArray = byteArrayOf(1, 2, 3, 4)
        val testIntArray = intArrayOf(1, 2, 3, 4)
        assertFalse(ByteArrayButComparable(testByteArray).equals(testIntArray))
        // But since testByteArray isn't a ByteArrayButComparable, it won't be equal.
        assertFalse(ByteArrayButComparable(testByteArray).equals(testByteArray))
        // However, two ByteArrayButComparables should be equal.
        assertEquals(ByteArrayButComparable(testByteArray), ByteArrayButComparable(testByteArray))
        // And if it's another ByteArray but equivalent then they should still be equal.
        assertEquals(ByteArrayButComparable(byteArrayOf(1, 2, 3, 4)), ByteArrayButComparable(testByteArray))
    }
}