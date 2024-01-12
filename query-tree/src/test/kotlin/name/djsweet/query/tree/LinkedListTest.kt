// SPDX-FileCopyrightText: 2023 Dani Sweet <sidereal@djsweet.name>
//
// SPDX-License-Identifier: MIT

package name.djsweet.query.tree

import org.junit.jupiter.api.Test

import org.junit.jupiter.api.Assertions.*

class LinkedListTest {
    @Test fun hasByIdentity() {
        val one = 1
        val two = 2
        val three = 3
        val four = 4

        val target = listFromIterable(listOf(one, two, four))
        assertTrue(listHasByIdentity(target, one))
        assertTrue(listHasByIdentity(target, two))
        assertFalse(listHasByIdentity(target, three))
        assertTrue(listHasByIdentity(target, four))
    }

    @Test fun removeByIdentity() {
        val one = 1
        val two = 2
        val three = 3
        val four = 4

        val target = listFromIterable(listOf(one, two, four))
        val removeThree = listRemoveByIdentity(target, three)
        assertTrue(listEquals(target, removeThree))
        assertTrue(target === removeThree)

        val removeOneResult = listFromIterable(listOf(two, four))
        val removeOne = listRemoveByIdentity(target, one)
        assertTrue(listEquals(removeOneResult, removeOne))
        assertFalse(removeOneResult === removeOne)

        val removeTwoResult = listFromIterable(listOf(one, four))
        val removeTwo = listRemoveByIdentity(target, two)
        assertTrue(listEquals(removeTwoResult, removeTwo))
        assertFalse(removeTwoResult === removeTwo)

        val removeFourResult = listFromIterable(listOf(one, two))
        val removeFour = listRemoveByIdentity(target, four)
        assertTrue(listEquals(removeFourResult, removeFour))
        assertFalse(removeFourResult === removeFour)
    }
}