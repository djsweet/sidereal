// SPDX-FileCopyrightText: 2023 Dani Sweet <thorium@djsweet.name>
//
// SPDX-License-Identifier: MIT

package name.djsweet.query.tree

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

class ArrayListStackTest {
    @Test
    fun emptyArrayListStack() {
        val stack = ArrayListStack<Int>()
        assertEquals(0, stack.size)
        assertNull(stack.peek())
        assertNull(stack.pop())
        assertEquals(0, stack.size)
    }

    @Test
    fun simpleRunArrayListStack() {
        val stack = ArrayListStack<Int>()
        stack.push(1)
        assertEquals(1, stack.size)
        assertEquals(1, stack.peek())

        stack.push(2)
        assertEquals(2, stack.size)
        assertEquals(2, stack.peek())
        assertEquals(2, stack.pop())
        assertEquals(1, stack.size)

        stack.push(3)
        assertEquals(2, stack.size)
        assertEquals(3, stack.peek())
        assertEquals(3, stack.pop())
        assertEquals(1, stack.size)
        assertEquals(1, stack.peek())
        assertEquals(1, stack.pop())
        assertEquals(0, stack.size)
        assertNull(stack.peek())

        assertEquals(0, stack.size)
        assertNull(stack.pop())
        assertEquals(0, stack.size)
    }
}