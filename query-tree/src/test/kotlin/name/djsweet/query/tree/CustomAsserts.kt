// SPDX-FileCopyrightText: 2023 Dani Sweet <thorium@djsweet.name>
//
// SPDX-License-Identifier: MIT

package name.djsweet.query.tree

import org.junit.jupiter.api.Assertions

internal fun <T> assertListOfByteArrayValuePairsEquals(left: List<Pair<ByteArrayButComparable, T>>, right: List<Pair<ByteArrayButComparable, T>>) {
    Assertions.assertEquals(left.size, right.size)
    for (i in left.indices) {
        val leftItem = left[i]
        val rightItem = right[i]
        if (leftItem.first.compareTo(rightItem.first) != 0 || leftItem.second != rightItem.second) {
            Assertions.fail<String>("Lists differ at index [$i]; expected $left but was $right")
        }
    }
}

internal fun <T> assertListOfByteArrayValuePairsEquals(left: ArrayList<Pair<ByteArrayButComparable, T>>, right: ArrayList<Pair<ByteArrayButComparable, T>>) {
    Assertions.assertEquals(left.size, right.size)
    for (i in left.indices) {
        val leftItem = left[i]
        val rightItem = right[i]
        if (leftItem.first.compareTo(rightItem.first) != 0 || leftItem.second != rightItem.second) {
            Assertions.fail<String>("Lists differ at index [$i]; expected $left but was $right")
        }
    }
}