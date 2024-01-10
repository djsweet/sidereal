// SPDX-FileCopyrightText: 2023 Dani Sweet <thorium@djsweet.name>
//
// SPDX-License-Identifier: MIT

package name.djsweet.sidereal

import name.djsweet.query.tree.QPTrie
import name.djsweet.query.tree.QuerySpec

data class StartsWithVerification(
    val key: ByteArray,
    val byteOffset: Int,
    val mask: Byte,
    val expected: Byte
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as StartsWithVerification

        if (!key.contentEquals(other.key)) return false
        if (byteOffset != other.byteOffset) return false
        if (mask != other.mask) return false
        return expected == other.expected
    }

    override fun hashCode(): Int {
        var result = key.contentHashCode()
        result = 31 * result + byteOffset.hashCode()
        result = 31 * result + mask.hashCode()
        result = 31 * result + expected.hashCode()
        return result
    }
}

data class FullQuery(
    val treeSpec: QuerySpec,
    val arrayContains: QPTrie<QPTrie<Boolean>>,
    val notEquals: QPTrie<QPTrie<Boolean>>,
    val startsWithVerification: StartsWithVerification?
) {
    fun notEqualsMatchesData(data: QPTrie<ByteArray>): Boolean {
        var foundInequality = false
        data.visitUnsafeSharedKey { (key, value) ->
            val result = this.notEquals.get(key)?.get(value)
            foundInequality = foundInequality || result != null
        }
        return !foundInequality
    }

    fun startsWithMatchesAtOffsets(data: QPTrie<ByteArray>): Boolean {
        val swv = this.startsWithVerification ?: return true
        val (key, byteOffset, mask, expected) = swv
        val inData = data.get(key) ?: return false

        if (byteOffset >= inData.size) {
            return false
        }

        return inData[byteOffset].toInt().and(mask.toInt()).toByte() == expected
    }

    fun countArrayContainsConditions(): Long {
        var arrayContainsConditions = 0.toLong()
        this.arrayContains.visitUnsafeSharedKey { (_, values) ->
            arrayContainsConditions += values.size
        }
        return arrayContainsConditions
    }
}