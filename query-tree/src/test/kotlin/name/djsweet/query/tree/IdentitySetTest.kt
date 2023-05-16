package name.djsweet.query.tree

import org.junit.jupiter.api.Test

import org.junit.jupiter.api.Assertions.*

class IdentitySetTest {
    @Test fun convertingIntsToBytes() {
        val oneTwentySeven = 0x0000007f
        assertArrayEquals(byteArrayOf(0, 0, 0, 127), byteArrayForInt(oneTwentySeven))

        val oneTwentyEight = 0x00000080
        assertArrayEquals(byteArrayOf(0, 0, 0, -128), byteArrayForInt(oneTwentyEight))

        val twoFiftyFive = 0x000000ff
        assertArrayEquals(byteArrayOf(0, 0, 0, -1), byteArrayForInt(twoFiftyFive))

        val twoFiftySix = 0x00000100
        assertArrayEquals(byteArrayOf(0, 0, 1, 0), byteArrayForInt(twoFiftySix))

        val threeTwoFiveOneTwo = 0x00007f00
        assertArrayEquals(byteArrayOf(0, 0, 127, 0), byteArrayForInt(threeTwoFiveOneTwo))

        val threeTwoSixThreeNine = 0x00007f7f
        assertArrayEquals(byteArrayOf(0, 0, 127, 127), byteArrayForInt(threeTwoSixThreeNine))

        val threeTwoSixFourOh = 0x00007f80
        assertArrayEquals(byteArrayOf(0, 0, 127, -128), byteArrayForInt(threeTwoSixFourOh))

        val threeTwoSevenSixSeven = 0x00007fff
        assertArrayEquals(byteArrayOf(0, 0, 127, -1), byteArrayForInt(threeTwoSevenSixSeven))

        val sixFiveFourOhSeven = 0x0000ff7f
        assertArrayEquals(byteArrayOf(0, 0, -1, 127), byteArrayForInt(sixFiveFourOhSeven))

        val sixFiveFiveThreeFive = 0x0000ffff
        assertArrayEquals(byteArrayOf(0, 0, -1, -1), byteArrayForInt(sixFiveFiveThreeFive))

        val eightThreeFourFourTwoThree = 0x007f7a77
        assertArrayEquals(byteArrayOf(0, 127, 122, 119), byteArrayForInt(eightThreeFourFourTwoThree))

        val eightFourTwoTwoSevenNineFour = 0x0080858a
        assertArrayEquals(byteArrayOf(0, -128, -123, -118), byteArrayForInt(eightFourTwoTwoSevenNineFour))

        val oneSixSevenFourFiveEightSixSix = 0x00ff858a
        assertArrayEquals(byteArrayOf(0, -1, -123, -118), byteArrayForInt(oneSixSevenFourFiveEightSixSix))

        val twoOneThreeEightEightSixFourTwoFourEight = 0x7f7c7a78
        assertArrayEquals(byteArrayOf(127, 124, 122, 120), byteArrayForInt(twoOneThreeEightEightSixFourTwoFourEight))

        val twoOneFiveFiveSixFourOneFourSixFour = (0x807c7a78).toInt()
        assertArrayEquals(byteArrayOf(-128, 124, 122, 120), byteArrayForInt(twoOneFiveFiveSixFourOneFourSixFour))

        val fourTwoThreeSixZeoOneSixTwoFourEight = (0xfc7c7a78).toInt()
        assertArrayEquals(byteArrayOf(-4, 124, 122, 120), byteArrayForInt(fourTwoThreeSixZeoOneSixTwoFourEight))
    }

    data class IntHolder(private val internal: Int)

    @Test fun identitySetBehavior() {
        val one = IntHolder(1)
        val two = IntHolder(2)
        val threeFirst = IntHolder(3)
        val threeSecond = IntHolder(3)
        val four = IntHolder(4)
        val five = IntHolder(5)

        assertEquals(threeFirst, threeSecond)
        assertEquals(threeFirst.hashCode(), threeSecond.hashCode())
        assertFalse(threeFirst === threeSecond)

        val addedItems = listOf(one, two, threeFirst, threeSecond, five)

        val gradual = IdentitySet<IntHolder>().add(one).add(two).add(threeFirst).add(threeSecond).add(five)
        val fromIterator = IdentitySet(addedItems)
        assertEquals(gradual.toList(), fromIterator.toList())

        val fromVisit = ArrayList<IntHolder>()
        fromIterator.visitAll {
            fromVisit.add(it)
        }
        assertEquals(fromIterator.toList(), fromVisit.toList())

        for (item in addedItems) {
            assertTrue(fromIterator.contains(item))
            val reAdded = fromIterator.add(item)
            assertTrue(reAdded === fromIterator)
        }

        assertFalse(fromIterator.contains(four))

        val removedThreeFirst = fromIterator.remove(threeFirst)
        assertFalse(removedThreeFirst === fromIterator)
        val keptItemsAfterThreeFirst = listOf(one, two, threeSecond, five)
        for (item in keptItemsAfterThreeFirst) {
            assertTrue(removedThreeFirst.contains(item))
            val reAdded = removedThreeFirst.add(item)
            assertTrue(reAdded === removedThreeFirst)
        }
        assertFalse(removedThreeFirst.contains(four))
        assertFalse(removedThreeFirst.contains(threeFirst))

        val removedTwo = removedThreeFirst.remove(two)
        assertFalse(removedTwo === removedThreeFirst)
        val keptItemsAfterTwo = listOf(one, threeSecond, five)
        for (item in keptItemsAfterTwo) {
            assertTrue(removedTwo.contains(item))
            val reAdded = removedTwo.add(item)
            assertTrue(reAdded === removedTwo)
        }
        assertFalse(removedTwo.contains(four))
        assertFalse(removedTwo.contains(threeFirst))
        assertFalse(removedTwo.contains(two))

        val reAddedThreeFirst = removedTwo.add(threeFirst)
        assertFalse(reAddedThreeFirst === removedTwo)
        val keptItemsWithThreeFirst = listOf(one, threeSecond, threeFirst, five)
        for (item in keptItemsWithThreeFirst) {
            assertTrue(reAddedThreeFirst.contains(item))
            val reAdded = reAddedThreeFirst.add(item)
            assertTrue(reAdded === reAddedThreeFirst)
        }
        assertFalse(reAddedThreeFirst.contains(four))
        assertFalse(reAddedThreeFirst.contains(two))
        assertEquals(keptItemsWithThreeFirst, reAddedThreeFirst.toList())
    }

    @Test fun identitySetRemoveNotPresent() {
        val set = IdentitySet(listOf(1, 2, 3))
        val removedSet = set.remove(4)
        assertTrue(set === removedSet)
        assertEquals(3, set.size)
        assertTrue(removedSet.contains(1))
        assertTrue(removedSet.contains(2))
        assertTrue(removedSet.contains(3))
        assertFalse(removedSet.contains(4))
    }
}