package name.djsweet.query.tree

import org.junit.jupiter.api.Test

import org.junit.jupiter.api.Assertions.*
import net.jqwik.api.*


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
        val exactLookup = tree.lookupExactRange(Pair("one", "two"))
        assertNull(exactLookup)

        val pointLookup = tree.lookupPoint("one")
        assertEquals(0, pointLookup.asSequence().toList().size)

        val rangeLookup = tree.lookupRange(Pair("one", "two"))
        assertEquals(0, rangeLookup.asSequence().toList().size)
    }

    @Test fun singleInsertIntervalTree() {
        val tree = IntervalTree<Int, String>().put(Pair(0, 2), "only entry")
        assertEquals(tree.size, 1)

        var seenIterators = 0
        for (ent in tree) {
            seenIterators++
            assertEquals(Pair(0, 2), ent.first)
            assertEquals("only entry", ent.second)
            assertEquals(0, ent.third)
        }
        assertEquals(1, seenIterators)

        val exactLookupEquals = tree.lookupExactRange(Pair(0, 2))
        assertNotNull(exactLookupEquals)
        assertEquals("only entry", exactLookupEquals)
    }

    @Test fun tripleInsertUpdateRemoveIntervalTree() {
        var tree = IntervalTree(
            arrayListOf(
                Pair(Pair(-1, 4), "leftmost entry"),
                Pair(Pair(0, 2), "middle entry"),
                Pair(Pair(1, 3), "right entry")
            )
        )

        var seenIterators = 0
        for (ent in tree) {
            when (seenIterators) {
                0 -> {
                    assertEquals(Pair(-1, 4), ent.first)
                    assertEquals("leftmost entry", ent.second)
                    assertEquals(0, ent.third)
                }
                1 -> {
                    assertEquals(Pair(0, 2), ent.first)
                    assertEquals("middle entry", ent.second)
                    assertEquals(0, ent.third)
                }
                2 -> {
                    assertEquals(Pair(1, 3), ent.first)
                    assertEquals("right entry", ent.second)
                    assertEquals(0, ent.third)
                }
            }
            seenIterators++
        }
        assertEquals(3, seenIterators)
        assertEquals("leftmost entry", tree.lookupExactRange(Pair(-1, 4)))
        assertNull(tree.lookupExactRange(Pair(-1, 5)))
        assertEquals("middle entry", tree.lookupExactRange(Pair(0, 2)))
        assertNull(tree.lookupExactRange(Pair(0, 1)))
        assertEquals("right entry", tree.lookupExactRange(Pair(1, 3)))
        assertNull(tree.lookupExactRange(Pair(2, 3)))

        tree = tree.update(Pair(1, 3)) { prev: String? ->
            assertEquals(prev, "right entry")
            "ok"
        }
        seenIterators = 0
        for (ent in tree) {
            when (seenIterators) {
                0 -> {
                    assertEquals(Pair(-1, 4), ent.first)
                    assertEquals("leftmost entry", ent.second)
                    assertEquals(0, ent.third)
                }
                1 -> {
                    assertEquals(Pair(0, 2), ent.first)
                    assertEquals("middle entry", ent.second)
                    assertEquals(0 ,ent.third)
                }
                2 -> {
                    assertEquals(Pair(1, 3), ent.first)
                    assertEquals("ok", ent.second)
                    assertEquals(0, ent.third)
                }
            }
            seenIterators++
        }
        assertEquals(3, seenIterators)
        assertEquals("leftmost entry", tree.lookupExactRange(Pair(-1, 4)))
        assertNull(tree.lookupExactRange(Pair(-1, 5)))
        assertEquals("middle entry", tree.lookupExactRange(Pair(0, 2)))
        assertNull(tree.lookupExactRange(Pair(0, 1)))
        assertEquals("ok", tree.lookupExactRange(Pair(1, 3)))
        assertNull(tree.lookupExactRange(Pair(2, 3)))

        tree = tree.remove(Pair(0, 2))
        seenIterators = 0
        for (ent in tree) {
            when (seenIterators) {
                0 -> {
                    assertEquals(Pair(-1, 4), ent.first)
                    assertEquals("leftmost entry", ent.second)
                    assertEquals(1, ent.third)
                }
                1 -> {
                    assertEquals(Pair(1, 3), ent.first)
                    assertEquals("ok", ent.second)
                    assertEquals(0, ent.third)
                }
            }
            seenIterators++
        }
        assertEquals(2, seenIterators)
        assertEquals("leftmost entry", tree.lookupExactRange(Pair(-1, 4)))
        assertNull(tree.lookupExactRange(Pair(-1, 5)))
        assertNull( tree.lookupExactRange(Pair(0, 2)))
        assertNull(tree.lookupExactRange(Pair(0, 1)))
        assertEquals("ok", tree.lookupExactRange(Pair(1, 3)))
        assertNull(tree.lookupExactRange(Pair(2, 3)))
    }

    @Test fun lookupRangeEnsureBoundsArentNullAtCallSequence() {
        val tree = IntervalTree(arrayListOf(
            Pair(Pair(0, -188), "")
        ))
        val results = tree.lookupRange(Pair(0, 0)).asSequence().toList()
        assertEquals(1, results.size)
        assertEquals(Pair(-188, 0), results[0].first)
        assertEquals("", results[0].second)
    }

    @Test fun doubleReplacementViaUpdateKeepsResults() {
        val someEmptyString = ""
        var tree = IntervalTree(arrayListOf(
            Pair(Pair(0, -69), someEmptyString)
        ))
        val results1 = tree.lookupRange(Pair(0, -69)).asSequence().toList()
        assertEquals(1, results1.size)
        assertEquals(Pair(-69, 0), results1[0].first)
        assertEquals("", results1[0].second)

        tree = tree.remove(Pair(0, -69)).put(Pair(0, -69), someEmptyString)
        val results2 = tree.lookupRange(Pair(0, -69)).asSequence().toList()
        assertEquals(1, results2.size)
        assertEquals(Pair(-69, 0), results2[0].first)
        assertEquals("", results2[0].second)

        tree = tree.update(Pair(0, -69)) { someEmptyString }
        val results3 = tree.lookupRange(Pair(0, -69)).asSequence().toList()
        assertEquals(1, results3.size)
        assertEquals(Pair(-69, 0), results3[0].first)
        assertEquals("", results3[0].second)

        tree = tree.put(Pair(0, -69), someEmptyString)
        val results4 = tree.lookupRange(Pair(0, -69)).asSequence().toList()
        assertEquals(1, results4.size)
        assertEquals(Pair(-69, 0), results4[0].first)
        assertEquals("", results4[0].second)
    }

    @Test fun removalsDoNotResultInDoubleEntries() {
        val basis = arrayListOf(
            Pair(Pair(0, -374), "a"),
            Pair(Pair(-374, 70), "b"),
            Pair(Pair(-1, 2147483646), "c"),
            Pair(Pair(0, 106), "d"),
            Pair(Pair(0, -2655710), "e")
        )
        var tree = IntervalTree(basis)
        // /*
        tree = tree.put(basis[0].first, "f")
        assertEquals(5, tree.size)
        val firstTreeIterator = tree.iterator()

        assertTrue(firstTreeIterator.hasNext())
        val firstFirstEntry = firstTreeIterator.next()
        assertEquals(Pair(-2655710, -0), firstFirstEntry.first)
        assertEquals(basis[4].second, firstFirstEntry.second)

        assertTrue(firstTreeIterator.hasNext())
        val secondFirstEntry = firstTreeIterator.next()
        assertEquals(Pair(-374, 0), secondFirstEntry.first)
        assertEquals("f", secondFirstEntry.second)

        assertTrue(firstTreeIterator.hasNext())
        val thirdFirstEntry = firstTreeIterator.next()
        assertEquals(basis[1].first, thirdFirstEntry.first)
        assertEquals(basis[1].second, thirdFirstEntry.second)

        assertTrue(firstTreeIterator.hasNext())
        val fourthFirstEntry = firstTreeIterator.next()
        assertEquals(basis[2].first, fourthFirstEntry.first)
        assertEquals(basis[2].second, fourthFirstEntry.second)

        assertTrue(firstTreeIterator.hasNext())
        val fifthFirstEntry = firstTreeIterator.next()
        assertEquals(basis[3].first, fifthFirstEntry.first)
        assertEquals(basis[3].second, fifthFirstEntry.second)

        assertFalse(firstTreeIterator.hasNext())
        val treeAfterReplacement = tree
        // */

        try {
            tree = tree.remove(basis[1].first)
            assertEquals(4, tree.size)
            val secondTreeIterator = tree.iterator()

            assertTrue(secondTreeIterator.hasNext())
            val firstSecondEntry = secondTreeIterator.next()
            assertEquals(Pair(-2655710, -0), firstSecondEntry.first)
            assertEquals(basis[4].second, firstSecondEntry.second)

            assertTrue(secondTreeIterator.hasNext())
            val secondSecondEntry = secondTreeIterator.next()
            assertEquals(Pair(-374, 0), secondSecondEntry.first)
            assertEquals("f", secondSecondEntry.second)

            assertTrue(secondTreeIterator.hasNext())
            val thirdSecondEntry = secondTreeIterator.next()
            assertEquals(basis[2].first, thirdSecondEntry.first)
            assertEquals(basis[2].second, thirdSecondEntry.second)

            assertTrue(secondTreeIterator.hasNext())
            val fourthSecondEntry = secondTreeIterator.next()
            assertEquals(basis[3].first, fourthSecondEntry.first)
            assertEquals(basis[3].second, fourthSecondEntry.second)

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

        tree = tree.remove(basis[2].first)
        assertEquals(3, tree.size)
        val thirdTreeIterator = tree.iterator()

        assertTrue(thirdTreeIterator.hasNext())
        val firstThirdEntry = thirdTreeIterator.next()
        assertEquals(Pair(-2655710, -0), firstThirdEntry.first)
        assertEquals(basis[4].second, firstThirdEntry.second)

        assertTrue(thirdTreeIterator.hasNext())
        val secondThirdEntry = thirdTreeIterator.next()
        assertEquals(Pair(-374, 0), secondThirdEntry.first)
        assertEquals("f", secondThirdEntry.second)

        assertTrue(thirdTreeIterator.hasNext())
        val thirdThirdEntry = thirdTreeIterator.next()
        assertEquals(basis[3].first, thirdThirdEntry.first)
        assertEquals(basis[3].second, thirdThirdEntry.second)

        assertFalse(thirdTreeIterator.hasNext())
    }

    @Provide
    fun pairsWithUpdates(p: Pair<Int, Int>): Arbitrary<Pair<Pair<Int, Int>, ArrayList<String>>> {
        return Arbitraries.strings().list().ofMinSize(1).ofMaxSize(5) .map { Pair(p, ArrayList(it)) }
    }

    // jqwik reflection appears to be broken and cannot lift pairsWithUpdates into a kotlin List.
    @Provide
    fun listOfPairsWithUpdates(): Arbitrary<List<Pair<Pair<Int, Int>, ArrayList<String>>>> {
        return Arbitraries.integers().flatMap<Pair<Pair<Int, Int>, ArrayList<String>>> { first: Int ->
            Arbitraries.integers().flatMap {
                this.pairsWithUpdates(Pair(first, it))
            }
        }.list()
    }

    // ... jqwik appears to not be able to lift Pairs into lists at all?
    @Provide
    fun listOfPairs(): Arbitrary<List<Pair<Int, Int>>> {
        return Arbitraries.integers().flatMap<Pair<Int, Int>> { first: Int -> Arbitraries.integers().map {
            Pair(first, it)
        } }.list()
    }

    @Property
    fun fullLifecycle(
        @ForAll @From("listOfPairsWithUpdates") data: List<Pair<Pair<Int, Int>, ArrayList<String>>>,
        @ForAll @From("listOfPairs") ranges: List<Pair<Int, Int>>,
        @ForAll points: List<Int>
    ) {
        var maxIterations = 0
        var tree = IntervalTree<Int, String>()

        // We need to ensure that we're working with distinct pairs here.
        val entryMap = HashMap<Pair<Int, Int>, ArrayList<String>>()
        for (entry in data) {
            val swappedPair = Pair(entry.first.second, entry.first.first)
            if (entryMap[swappedPair] != null) {
                entryMap[swappedPair] = entry.second
            } else {
                entryMap[entry.first] = entry.second
            }
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
            for (info in tree) {
                // Note that we can generate pairs that don't have the order invariant enforced,
                // but the order invariant will implicitly be enforced by the tree code.
                val entryFromMap = entryMap[info.first] ?: entryMap[Pair(info.first.second, info.first.first)]
                assertNotNull(entryFromMap)
                if (entryFromMap == null) {
                    throw Exception("The assertion above should have been good enough")
                }
                assertTrue(entryFromMap.size > i)
                assertEquals(entryFromMap[i], info.second)
                assertTrue(-1 <= info.third)
                assertTrue(info.third <= 1)
            }

            // Third, test that all point lookups function as intended
            for (range in ranges) {
                val expectedRangeSet = HashSet<Pair<Pair<Int, Int>, String>>()
                val enforcedInvariantRange = enforceRangeInvariants(range)
                for (entry in entryMap.entries) {
                    if (entry.value.size <= i) {
                        continue
                    }
                    val enforcedInvariantEntry = enforceRangeInvariants(entry.key)
                    if (!rangeOverlapsPairs(enforcedInvariantRange, enforcedInvariantEntry)) {
                        continue
                    }
                    expectedRangeSet.add(Pair(enforcedInvariantEntry, entry.value[i]))
                }
                val receivedRangeSet = HashSet<Pair<Pair<Int, Int>, String>>()
                for (received in tree.lookupRange(range)) {
                    receivedRangeSet.add(received)
                }
                assertEquals(expectedRangeSet, receivedRangeSet)
            }

            // Fourth, test that all range lookups function as intended
            for (point in points) {
                val expectedRangeSet = HashSet<Pair<Pair<Int, Int>, String>>()
                val pointRange = Pair(point, point)
                for (entry in entryMap.entries) {
                    if (entry.value.size <= i) {
                        continue
                    }
                    val enforcedInvariantEntry = enforceRangeInvariants(entry.key)
                    if (!rangeOverlapsPairs(enforcedInvariantEntry, pointRange)) {
                        continue
                    }
                    expectedRangeSet.add(Pair(enforcedInvariantEntry, entry.value[i]))
                }
                val receivedRangeSet = HashSet<Pair<Pair<Int, Int>, String>>()
                for (received in tree.lookupPoint(point)) {
                    receivedRangeSet.add(received)
                }
                assertEquals(expectedRangeSet, receivedRangeSet)
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
                    val enforcedInvariantEntryKey = enforceRangeInvariants(entry.key)
                    for (treeEntry in allPossibleEntries) {
                        if (treeEntry.first != enforcedInvariantEntryKey) {
                            continue
                        }
                        if (foundIt) {
                            println("Hoo boy how'd we do this?")
                            for (badEntry in tree) {
                                println("${badEntry.first} ${badEntry.third}")
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
}