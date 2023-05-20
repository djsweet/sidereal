package name.djsweet.query.tree

import org.junit.jupiter.api.Test

import org.junit.jupiter.api.Assertions.*
import net.jqwik.api.*
import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.collections.HashSet

private fun <T: Comparable<T>>rangeOverlapsPairs(left: Pair<T, T>, right: Pair<T, T>): Boolean {
    return rangeOverlaps(left.first, left.second, right.first, right.second)
}

class IntervalTreeTest {
    @Test fun enforcingRangeInvariants() {
        // Note that if the range invariants are enforced, we shouldn't allocate a new pair!
        // They should be the exact same instance.

        // Equal keys are fine
        val equalPair = Pair(1, 1)
        val enforcedEqualPair = enforceRangeInvariants(equalPair)
        assertEquals(Pair(1, 1), enforcedEqualPair)
        assertSame(equalPair, enforcedEqualPair)

        // Left lower than right is fine
        val leftLowerPair = Pair(1, 2)
        val enforcedLeftLowerPair = enforceRangeInvariants(leftLowerPair)
        assertEquals(Pair(1, 2), enforcedLeftLowerPair)
        assertSame(leftLowerPair, enforcedLeftLowerPair)

        // Right lower than left is not fine and needs to be fixed
        val rightLowerPair = Pair(2, 1)
        val enforcedRightLowerPair = enforceRangeInvariants(rightLowerPair)
        assertEquals(Pair(1, 2), enforcedRightLowerPair)
        assertNotSame(rightLowerPair, enforcedRightLowerPair)
    }

    @Test fun overlappingRanges() {
        val testRight = Pair(7, 12)
        // Left fully lower than right, ends on itself
        assertFalse(rangeOverlapsPairs(Pair(1, 1), testRight))
        // Left fully lower than right, ends beyond itself
        assertFalse(rangeOverlapsPairs(Pair(1, 2), testRight))
        // Left starts outside right, ends starting at right
        assertTrue(rangeOverlapsPairs(Pair(1, testRight.first), testRight))
        // Left starts outside right, ends ending at right
        assertTrue(rangeOverlapsPairs(Pair(2, testRight.second), testRight))
        // Left starts outside right, ends right of right
        assertTrue(rangeOverlapsPairs(Pair(3, 13), testRight))
        // Left starts at right, ends starting at right
        assertTrue(rangeOverlapsPairs(Pair(testRight.first, testRight.first),testRight))
        // Left starts at right, ends inside right
        assertTrue(rangeOverlapsPairs(Pair(testRight.first, 9), testRight))
        // Left starts at right, ends ending at right
        assertTrue(rangeOverlapsPairs(Pair(testRight.first, testRight.second), testRight))
        // Left starts at right, ends right of right
        assertTrue(rangeOverlapsPairs(Pair(testRight.first, 14), testRight))
        // Left starts inside right, ends inside right
        assertTrue(rangeOverlapsPairs(Pair(9, 10), testRight))
        // Left starts inside right, ends ending at right
        assertTrue(rangeOverlapsPairs(Pair(10, testRight.second), testRight))
        // Left starts inside right, ends right of right
        assertTrue(rangeOverlapsPairs(Pair(8, 15), testRight))
        // Left starts ending at right, ends ending at right
        assertTrue(rangeOverlapsPairs(Pair(testRight.second, testRight.second), testRight))
        // Left starts ending at right, ends right of right
        assertTrue(rangeOverlapsPairs(Pair(testRight.second, 15), testRight))
        // Left starts outside of right, ends at itself
        assertFalse(rangeOverlapsPairs(Pair(13, 13), testRight))
        // Left starts outside of right, ends later
        assertFalse(rangeOverlapsPairs(Pair(13, 16), testRight))
    }

    @Test fun emptyIntervalTreeConstructor() {
        val tree = IntervalTree<String, String>()
        assertEquals(tree.size, 0)
        for (ent in tree) {
            fail<String>("This should not have happened!")
        }
        val exactLookup = tree.lookupExactRange(IntervalRange.fromBounds("one", "two"))
        assertNull(exactLookup)

        val pointLookup = tree.lookupPoint("one")
        assertEquals(0, pointLookup.asSequence().toList().size)

        val rangeLookup = tree.lookupRange(IntervalRange.fromBounds("one", "two"))
        assertEquals(0, rangeLookup.asSequence().toList().size)
    }

    @Test fun singleInsertIntervalTree() {
        val tree = IntervalTree<Int, String>().put(
            IntervalRange.fromBounds(0, 2),
            "only entry"
        )
        assertEquals(tree.size, 1)

        var seenIterators = 0
        for (ent in tree) {
            seenIterators++
            assertEquals(IntervalRange(0, 2), ent.key)
            assertEquals("only entry", ent.value)
            assertEquals(0, ent.balance)
        }
        assertEquals(1, seenIterators)

        val exactLookupEquals = tree.lookupExactRange(IntervalRange.fromBounds(0, 2))
        assertNotNull(exactLookupEquals)
        assertEquals("only entry", exactLookupEquals)
    }

    @Test fun tripleInsertUpdateRemoveIntervalTree() {
        var tree = IntervalTree(
            arrayListOf(
                Pair(IntervalRange.fromBounds(-1, 4), "leftmost entry"),
                Pair(IntervalRange.fromBounds(0, 2), "middle entry"),
                Pair(IntervalRange.fromBounds(1, 3), "right entry")
            )
        )

        var seenIterators = 0
        for (ent in tree) {
            when (seenIterators) {
                0 -> {
                    assertEquals(IntervalRange(-1, 4), ent.key)
                    assertEquals("leftmost entry", ent.value)
                    assertEquals(0, ent.balance)
                }
                1 -> {
                    assertEquals(IntervalRange(0, 2), ent.key)
                    assertEquals("middle entry", ent.value)
                    assertEquals(0, ent.balance)
                }
                2 -> {
                    assertEquals(IntervalRange(1, 3), ent.key)
                    assertEquals("right entry", ent.value)
                    assertEquals(0, ent.balance)
                }
            }
            seenIterators++
        }
        assertEquals(3, seenIterators)
        assertEquals(
            "leftmost entry",
            tree.lookupExactRange(IntervalRange.fromBounds(-1, 4))
        )
        assertNull(tree.lookupExactRange(IntervalRange.fromBounds(-1, 5)))
        assertEquals(
            "middle entry",
            tree.lookupExactRange(IntervalRange.fromBounds(0, 2))
        )
        assertNull(tree.lookupExactRange(IntervalRange.fromBounds(0, 1)))
        assertEquals(
            "right entry",
            tree.lookupExactRange(IntervalRange.fromBounds(1, 3))
        )
        assertNull(tree.lookupExactRange(IntervalRange.fromBounds(2, 3)))

        tree = tree.update(IntervalRange.fromBounds(1, 3)) { prev: String? ->
            assertEquals(prev, "right entry")
            "ok"
        }
        seenIterators = 0
        for (ent in tree) {
            when (seenIterators) {
                0 -> {
                    assertEquals(IntervalRange(-1, 4), ent.key)
                    assertEquals("leftmost entry", ent.value)
                    assertEquals(0, ent.balance)
                }
                1 -> {
                    assertEquals(IntervalRange(0, 2), ent.key)
                    assertEquals("middle entry", ent.value)
                    assertEquals(0, ent.balance)
                }
                2 -> {
                    assertEquals(IntervalRange(1, 3), ent.key)
                    assertEquals("ok", ent.value)
                    assertEquals(0, ent.balance)
                }
            }
            seenIterators++
        }
        assertEquals(3, seenIterators)
        assertEquals(
            "leftmost entry",
            tree.lookupExactRange(IntervalRange.fromBounds(-1, 4))
        )
        assertNull(tree.lookupExactRange(IntervalRange.fromBounds(-1, 5)))
        assertEquals(
            "middle entry",
            tree.lookupExactRange(IntervalRange.fromBounds(0, 2))
        )
        assertNull(tree.lookupExactRange(IntervalRange.fromBounds(0, 1)))
        assertEquals(
            "ok",
            tree.lookupExactRange(IntervalRange.fromBounds(1, 3))
        )
        assertNull(tree.lookupExactRange(IntervalRange.fromBounds(2, 3)))

        tree = tree.remove(IntervalRange.fromBounds(0, 2))
        seenIterators = 0
        for (ent in tree) {
            when (seenIterators) {
                0 -> {
                    assertEquals(IntervalRange(-1, 4), ent.key)
                    assertEquals("leftmost entry", ent.value)
                    assertEquals(1, ent.balance)
                }
                1 -> {
                    assertEquals(IntervalRange(1, 3), ent.key)
                    assertEquals("ok", ent.value)
                    assertEquals(0, ent.balance)
                }
            }
            seenIterators++
        }
        assertEquals(2, seenIterators)
        assertEquals(
            "leftmost entry",
            tree.lookupExactRange(IntervalRange.fromBounds(-1, 4))
        )
        assertNull(tree.lookupExactRange(IntervalRange.fromBounds(-1, 5)))
        assertNull( tree.lookupExactRange(IntervalRange.fromBounds(0, 2)))
        assertNull(tree.lookupExactRange(IntervalRange.fromBounds(0, 1)))
        assertEquals("ok", tree.lookupExactRange(IntervalRange.fromBounds(1, 3)))
        assertNull(tree.lookupExactRange(IntervalRange.fromBounds(2, 3)))
    }

    @Test fun lookupRangeEnsureBoundsArentNullAtCallSequence() {
        val tree = IntervalTree(arrayListOf(
            Pair(IntervalRange.fromBounds(0, -188), "")
        ))
        val results = tree.lookupRange(IntervalRange.fromBounds(0, 0)).asSequence().toList()
        assertEquals(1, results.size)
        assertEquals(IntervalRange(-188, 0), results[0].key)
        assertEquals("", results[0].value)
    }

    @Test fun doubleReplacementViaUpdateKeepsResults() {
        val someEmptyString = ""
        var tree = IntervalTree(arrayListOf(
            Pair(IntervalRange.fromBounds(0, -69), someEmptyString)
        ))
        val target = IntervalRange.fromBounds(0, -69)
        val results1 = tree.lookupRange(target).asSequence().toList()
        assertEquals(1, results1.size)
        assertEquals(IntervalRange(-69, 0), results1[0].key)
        assertEquals("", results1[0].value)

        tree = tree.remove(target).put(target, someEmptyString)
        val results2 = tree.lookupRange(target).asSequence().toList()
        assertEquals(1, results2.size)
        assertEquals(IntervalRange(-69, 0), results2[0].key)
        assertEquals("", results2[0].value)

        tree = tree.update(target) { someEmptyString }
        val results3 = tree.lookupRange(target).asSequence().toList()
        assertEquals(1, results3.size)
        assertEquals(IntervalRange(-69, 0), results3[0].key)
        assertEquals("", results3[0].value)

        tree = tree.put(target, someEmptyString)
        val results4 = tree.lookupRange(target).asSequence().toList()
        assertEquals(1, results4.size)
        assertEquals(IntervalRange(-69, 0), results4[0].key)
        assertEquals("", results4[0].value)
    }

    private fun testInOrderTraversal(tree: IntervalTree<Int, String>) {
        val parentStack = Stack<IntervalRange<Int>>()
        var lastHeight = 0
        for ((key, _, info) in tree.inOrderIterator()) {
            val (height, direction) = info
            var parent = if (parentStack.isEmpty()) { null } else { parentStack.peek() }
            if (parent == null) {
                assertEquals(0, height)
                assertEquals("root", direction)
                parentStack.push(key)
                continue
            }
            if (height < lastHeight) {
                for (i in 0 until lastHeight - height) {
                    parentStack.pop()
                }
                lastHeight = height
                parent = if (parentStack.isEmpty()) { null } else { parentStack.peek() }
            }
            if (parent == null) {
                fail<String>("Parent should not have been null after peeking")
                break
            }
            parentStack.push(key)
            if (direction == "left") {
                assertTrue(key <= parent)
            } else {
                assertTrue(key >= parent)
            }
        }
    }

    @Test fun removalsDoNotResultInDoubleEntries() {
        val basis = arrayListOf(
            Pair(Pair(0, -374), "a"),
            Pair(Pair(-374, 70), "b"),
            Pair(Pair(-1, 2147483646), "c"),
            Pair(Pair(0, 106), "d"),
            Pair(Pair(0, -2655710), "e")
        )
        var tree = IntervalTree(basis.map { IntervalRange.fromPair(it.first) to it.second })
        this.testInOrderTraversal(tree)
        // /*
        tree = tree.put(IntervalRange.fromPair(basis[0].first), "f")
        assertEquals(5, tree.size)
        this.testInOrderTraversal(tree)
        val firstTreeIterator = tree.iterator()

        assertTrue(firstTreeIterator.hasNext())
        val firstFirstEntry = firstTreeIterator.next()
        assertEquals(IntervalRange(-2655710, -0), firstFirstEntry.key)
        assertEquals(basis[4].second, firstFirstEntry.value)

        assertTrue(firstTreeIterator.hasNext())
        val secondFirstEntry = firstTreeIterator.next()
        assertEquals(IntervalRange(-374, 0), secondFirstEntry.key)
        assertEquals("f", secondFirstEntry.value)

        assertTrue(firstTreeIterator.hasNext())
        val thirdFirstEntry = firstTreeIterator.next()
        assertEquals(IntervalRange.fromPair(basis[1].first), thirdFirstEntry.key)
        assertEquals(basis[1].second, thirdFirstEntry.value)

        assertTrue(firstTreeIterator.hasNext())
        val fourthFirstEntry = firstTreeIterator.next()
        assertEquals(IntervalRange.fromPair(basis[2].first), fourthFirstEntry.key)
        assertEquals(basis[2].second, fourthFirstEntry.value)

        assertTrue(firstTreeIterator.hasNext())
        val fifthFirstEntry = firstTreeIterator.next()
        assertEquals(IntervalRange.fromPair(basis[3].first), fifthFirstEntry.key)
        assertEquals(basis[3].second, fifthFirstEntry.value)

        assertFalse(firstTreeIterator.hasNext())
        val treeAfterReplacement = tree
        // */

        try {
            tree = tree.remove(IntervalRange.fromPair(basis[1].first))
            assertEquals(4, tree.size)
            this.testInOrderTraversal(tree)
            val secondTreeIterator = tree.iterator()

            assertTrue(secondTreeIterator.hasNext())
            val firstSecondEntry = secondTreeIterator.next()
            assertEquals(IntervalRange(-2655710, -0), firstSecondEntry.key)
            assertEquals(basis[4].second, firstSecondEntry.value)

            assertTrue(secondTreeIterator.hasNext())
            val secondSecondEntry = secondTreeIterator.next()
            assertEquals(IntervalRange(-374, 0), secondSecondEntry.key)
            assertEquals("f", secondSecondEntry.value)

            assertTrue(secondTreeIterator.hasNext())
            val thirdSecondEntry = secondTreeIterator.next()
            assertEquals(IntervalRange.fromPair(basis[2].first), thirdSecondEntry.key)
            assertEquals(basis[2].second, thirdSecondEntry.value)

            assertTrue(secondTreeIterator.hasNext())
            val fourthSecondEntry = secondTreeIterator.next()
            assertEquals(IntervalRange.fromPair(basis[3].first), fourthSecondEntry.key)
            assertEquals(basis[3].second, fourthSecondEntry.value)

            assertFalse(secondTreeIterator.hasNext())
        } catch (e: Exception) {
            println("This is how the tree started")
            for (inOrderEntry in treeAfterReplacement.inOrderIterator()) {
                println(inOrderEntry)
            }
            println("We removed ${basis[1].first} from the tree")
            for (inOrderEntry in tree.inOrderIterator()) {
                println(inOrderEntry)
            }
            throw e
        }

        tree = tree.remove(IntervalRange.fromPair(basis[2].first))
        assertEquals(3, tree.size)
        val thirdTreeIterator = tree.iterator()
        this.testInOrderTraversal(tree)

        assertTrue(thirdTreeIterator.hasNext())
        val firstThirdEntry = thirdTreeIterator.next()
        assertEquals(IntervalRange(-2655710, -0), firstThirdEntry.key)
        assertEquals(basis[4].second, firstThirdEntry.value)

        assertTrue(thirdTreeIterator.hasNext())
        val secondThirdEntry = thirdTreeIterator.next()
        assertEquals(IntervalRange(-374, 0), secondThirdEntry.key)
        assertEquals("f", secondThirdEntry.value)

        assertTrue(thirdTreeIterator.hasNext())
        val thirdThirdEntry = thirdTreeIterator.next()
        assertEquals(IntervalRange.fromPair(basis[3].first), thirdThirdEntry.key)
        assertEquals(basis[3].second, thirdThirdEntry.value)

        assertFalse(thirdTreeIterator.hasNext())
    }

    @Test fun intervalTreeWithSameValueOnlyHasDistinctSize() {
        val tree = IntervalTree(listOf(
            Pair(IntervalRange.fromBounds(0, 1), "thing"),
            Pair(IntervalRange.fromBounds(1, 2), "stuff"),
            Pair(IntervalRange.fromBounds(0, 1), "other stuff")
        ))
        assertEquals(2, tree.size)
        assertEquals(
            "other stuff",
            tree.lookupExactRange(IntervalRange.fromBounds(0, 1))
        )
        assertEquals(
            "stuff",
            tree.lookupExactRange(IntervalRange.fromBounds(1, 2))
        )

        val resultingList = mutableListOf<Pair<Pair<Int, Int>, String>>()
        for ((key, value) in tree) {
            resultingList.add(Pair(key.toPair(), value))
        }
        assertArrayEquals(
            arrayOf(
                Pair(Pair(0, 1), "other stuff"),
                Pair(Pair(1, 2), "stuff")
            ),
            resultingList.toTypedArray()
        )
    }

    @Test fun removalOfNonexistentResultsInSameInstance() {
        val tree = IntervalTree(listOf(
            IntervalRange.fromBounds(0, 1) to "thing",
            IntervalRange.fromBounds(1, 2) to "stuff"
        ))
        val removedTree = tree.remove(IntervalRange.fromBounds(2, 3))

        assertEquals(2, removedTree.size)
        assertEquals(
            "thing",
            removedTree.lookupExactRange(IntervalRange.fromBounds(0, 1))
        )
        assertEquals(
            "stuff",
            removedTree.lookupExactRange(IntervalRange.fromBounds(1, 2))
        )
        assertNull(removedTree.lookupExactRange(IntervalRange.fromBounds(2, 3)))
        assertTrue(tree === removedTree)
    }

    @Test fun updatingTreeEntryToSameValueResultsInSameInstance() {
        val stuffString = "stuff"
        val tree = IntervalTree(listOf(
            Pair(0,1) to "thing",
            Pair(1, 2) to stuffString
        ).map { IntervalRange.fromPair(it.first) to it.second })
        val updatedTree = tree.update(IntervalRange.fromBounds(1, 2)) { stuffString }

        assertEquals(2, updatedTree.size)
        assertEquals(
            "thing",
            updatedTree.lookupExactRange(IntervalRange.fromBounds(0, 1))
        )
        assertEquals(
            "stuff",
            updatedTree.lookupExactRange(IntervalRange.fromBounds(1, 2))
        )
        assertEquals("stuff", updatedTree.lookupExactRange(IntervalRange(1, 2)))
        assertNull(updatedTree.lookupExactRange(IntervalRange.fromBounds(2, 3)))
        assertTrue(tree === updatedTree)
    }

    @Provide
    fun pairsWithUpdates(p: IntervalRange<Int>): Arbitrary<Pair<IntervalRange<Int>, ArrayList<String>>> {
        return Arbitraries.strings().list().ofMinSize(1).ofMaxSize(5) .map { Pair(p, ArrayList(it)) }
    }

    // jqwik reflection appears to be broken and cannot lift pairsWithUpdates into a kotlin List.
    @Provide
    fun listOfPairsWithUpdates(): Arbitrary<List<Pair<IntervalRange<Int>, ArrayList<String>>>> {
        return Arbitraries.integers().flatMap { first: Int ->
            Arbitraries.integers().flatMap {
                this.pairsWithUpdates(IntervalRange.fromBounds(first, it))
            }
        }.list()
    }

    // ... jqwik appears to not be able to lift Pairs into lists at all?
    @Provide
    fun listOfPairs(): Arbitrary<List<IntervalRange<Int>>> {
        return Arbitraries.integers().flatMap { first: Int -> Arbitraries.integers().map {
            IntervalRange.fromBounds(first, it)
        } }.list()
    }

    @Property
    fun fullLifecycle(
        @ForAll @From("listOfPairsWithUpdates") data: List<Pair<IntervalRange<Int>, ArrayList<String>>>,
        @ForAll @From("listOfPairs") ranges: List<IntervalRange<Int>>,
        @ForAll points: List<Int>
    ) {
        var maxIterations = 0
        var tree = IntervalTree<Int, String>()

        val entryMap = HashMap<IntervalRange<Int>, ArrayList<String>>()
        for (entry in data) {
            entryMap[entry.first] = entry.second
        }

        for (entry in entryMap.entries) {
            if (entry.value.size == 0) {
                continue
            }
            maxIterations = maxIterations.coerceAtLeast(entry.value.size)
            tree = tree.put(entry.key, entry.value[0])
        }
        for (i in 0 until maxIterations) {
            // First, run through all the entries in the data.
            // If they exist, check that you can get to them exactly.
            // If they don't, check that you can't get to them exactly.
            for (entry in entryMap.entries) {
                if (entry.value.size <= i) {
                    assertNull(tree.lookupExactRange(entry.key))
                } else {
                    assertEquals(entry.value[i], tree.lookupExactRange(entry.key))
                }
            }
            // Second, run through all the entries in the tree.
            // Make sure that the weight is always -1 <= w <= 1, and that
            // the values match up.
            var maxRange: IntervalRange<Int>? = null
            val expectedTreeEntries = ArrayList<IntervalTreeKeyValueWithNodeBalance<Int, String>>()
            for (entry in tree) {
                val (range, value, depth) = entry
                // Note that we can generate pairs that don't have the order invariant enforced,
                // but the order invariant will implicitly be enforced by the tree code.
                val entryFromMap = entryMap[range]
                assertNotNull(entryFromMap)
                if (entryFromMap == null) {
                    throw Exception("The assertion above should have been good enough")
                }
                assertTrue(entryFromMap.size > i)
                assertEquals(entryFromMap[i], value)
                assertTrue(-1 <= depth)
                assertTrue(depth <= 1)
                maxRange = range
                expectedTreeEntries.add(entry)
            }

            val seenTreeEntries = ArrayList<IntervalTreeKeyValueWithNodeBalance<Int, String>>()
            tree.visitAll {
                seenTreeEntries.add(it)
            }
            assertEquals(expectedTreeEntries.toList(), seenTreeEntries.toList())

            // Third, test min/max keys
            val firstItemIt = tree.iterator()
            if (firstItemIt.hasNext()) {
                assertEquals(firstItemIt.next().key, tree.minRange())
            } else {
                assertNull(tree.minRange())
            }
            if (maxRange != null) {
                assertEquals(maxRange, tree.maxRange())
            } else {
                assertNull(tree.maxRange())
            }

            // Fourth, test that all range lookups function as intended
            for (range in ranges) {
                val expectedRangeSet = HashSet<IntervalTreeKeyValue<Int, String>>()
                for (entry in entryMap.entries) {
                    if (entry.value.size <= i) {
                        continue
                    }
                    if (!range.overlaps(entry.key)) {
                        continue
                    }
                    expectedRangeSet.add(IntervalTreeKeyValue(entry.key, entry.value[i]))
                }
                val receivedRangeSet = HashSet<IntervalTreeKeyValue<Int, String>>()
                for (received in tree.lookupRange(range)) {
                    receivedRangeSet.add(received)
                }
                assertEquals(expectedRangeSet, receivedRangeSet)

                val visitedRangeSet = HashSet<IntervalTreeKeyValue<Int, String>>()
                tree.lookupRangeVisit(range) {
                    visitedRangeSet.add(it)
                }
                assertEquals(expectedRangeSet, visitedRangeSet)
            }

            // Fifth, test that all point lookups function as intended
            for (point in points) {
                val expectedPointSet = HashSet<IntervalTreeKeyValue<Int, String>>()
                val pointRange = IntervalRange.fromBounds(point, point)
                for (entry in entryMap.entries) {
                    if (entry.value.size <= i) {
                        continue
                    }
                    if (!pointRange.overlaps(entry.key)) {
                        continue
                    }
                    expectedPointSet.add(IntervalTreeKeyValue(entry.key, entry.value[i]))
                }
                val receivedRangeSet = HashSet<IntervalTreeKeyValue<Int, String>>()
                for (received in tree.lookupPoint(point)) {
                    receivedRangeSet.add(received)
                }
                assertEquals(expectedPointSet, receivedRangeSet)

                val visitedPointSet = HashSet<IntervalTreeKeyValue<Int, String>>()
                tree.lookupPointVisit(point) {
                    visitedPointSet.add(it)
                }
                assertEquals(expectedPointSet, visitedPointSet)
            }

            // Finally, prepare for the next major iteration
            val nextI = i + 1
            for (entry in entryMap.entries) {
                if (entry.value.size <= i) {
                    continue
                }
                tree = if (entry.value.size <= nextI) {
                    tree.remove(entry.key)
                } else {
                    tree.put(entry.key, entry.value[nextI])
                }
                if (entry.value.size > nextI) {
                    assertEquals(entry.value[nextI], tree.lookupExactRange(entry.key))
                    val allPossibleEntries = tree.lookupRange(entry.key)
                    var foundIt = false
                    for ((key) in allPossibleEntries) {
                        if (key != entry.key) {
                            continue
                        }
                        if (foundIt) {
                            println("Hoo boy how'd we do this?")
                            for (badEntry in tree) {
                                println("${badEntry.key} ${badEntry.balance}")
                            }
                        }
                        assertFalse(foundIt)
                        foundIt = true
                    }
                    if (!foundIt) {
                        println("Ok why didn't we find $entry in here?")
                    }
                    assertTrue(foundIt)
                }
            }
        }

        // Now that all iterations are over, expect the tree to be fully empty
        for (entry in entryMap.entries) {
            assertNull(tree.lookupExactRange(entry.key))
        }
        for (range in ranges) {
            val it = tree.lookupRange(range)
            assertFalse(it.hasNext())
        }
        for (point in points) {
            val it = tree.lookupPoint(point)
            assertFalse(it.hasNext())
        }
    }

    @Test fun minMaxRangesForEmpty() {
        val tree = IntervalTree<Int, Int>()
        assertNull(tree.minRange())
        assertNull(tree.maxRange())
    }

    @Test fun treeFromEmptyList() {
        val tree = IntervalTree<Int, Int>(listOf())
        assertEquals(0, tree.size)
        assertNull(tree.minRange())
        assertNull(tree.maxRange())
    }

    @Test fun treeRemoveNonexistentLefts() {
        val tree = IntervalTree(listOf(
            Pair(1 to 1, 1),
            Pair(3 to 3, 3),
            Pair(5 to 5, 5),
            Pair(7 to 7, 7),
            Pair(9 to 9, 9),
            Pair(11 to 11, 11),
            Pair(13 to 13, 13)
        ).map { IntervalRange.fromPair(it.first) to it.second })
        val origSize = tree.size
        for (i in 0 until 16 step 2) {
            val target = IntervalRange.fromBounds(i, i)
            val removeTree = tree.remove(target)
            assertTrue(removeTree === tree)
            assertEquals(origSize, removeTree.size)
            assertNull(removeTree.lookupExactRange(target))
            for (j in 1 until 15 step 2) {
                assertEquals(j, removeTree.lookupExactRange(IntervalRange.fromBounds(j, j)))
            }
        }
    }

    @Test fun treeRemoveNonexistentRights() {
        val tree = IntervalTree(listOf(
            Pair(2 to 2, 2),
            Pair(4 to 4, 4),
            Pair(6 to 6, 6),
            Pair(8 to 8, 8),
            Pair(10 to 10, 10),
            Pair(12 to 12, 12),
            Pair(14 to 14, 14)
        ).map { IntervalRange.fromPair(it.first) to it.second })
        val origSize = tree.size
        for (i in 1 until 17 step 2) {
            val removeTree = tree.remove(IntervalRange.fromBounds(i, i))
            assertTrue(removeTree === tree)
            assertEquals(origSize, removeTree.size)
            for (j in 2 until 16 step 2) {
                assertEquals(j, removeTree.lookupExactRange(IntervalRange.fromBounds(j, j)))
            }
        }
    }
}