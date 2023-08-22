package name.djsweet.thorium

import name.djsweet.query.tree.QPTrie
import name.djsweet.query.tree.QuerySpec

data class FullQuery(
    val treeSpec: QuerySpec,
    val arrayContains: QPTrie<QPTrie<Boolean>>,
    val notEquals: QPTrie<QPTrie<Boolean>>,
) {
    fun notEqualsMatchesData(data: QPTrie<ByteArray>): Boolean {
        var foundInequality = false
        data.visitUnsafeSharedKey { (key, value) ->
            val result = this.notEquals.get(key)?.get(value)
            foundInequality = foundInequality || result != null
        }
        return !foundInequality
    }
}