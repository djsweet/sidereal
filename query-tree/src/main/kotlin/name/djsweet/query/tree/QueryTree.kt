package name.djsweet.query.tree

import java.util.*

internal enum class IntermediateQueryTermKind {
    EQUALS,
    GREATER_OR_LESS,
    STARTS_WITH
}

// This is a data class to ease debugging in the test suite.
internal data class IntermediateQueryTerm(
    val kind: IntermediateQueryTermKind,
    val key: ByteArray,
    val lowerBound: ByteArray?,
    val upperBound: ByteArray?
) {
    companion object {
        fun equalityTerm(key: ByteArray, value: ByteArray): IntermediateQueryTerm {
            return IntermediateQueryTerm(
                IntermediateQueryTermKind.EQUALS,
                key,
                value,
                null
            )
        }

        fun greaterOrLessTerm(key: ByteArray, lowerBound: ByteArray?, upperBound: ByteArray?): IntermediateQueryTerm {
            return IntermediateQueryTerm(
                IntermediateQueryTermKind.GREATER_OR_LESS,
                key,
                lowerBound,
                upperBound
            )
        }

        fun startsWithTerm(key: ByteArray, startsWith: ByteArray): IntermediateQueryTerm {
            return IntermediateQueryTerm(
                IntermediateQueryTermKind.STARTS_WITH,
                key,
                startsWith,
                null
            )
        }
    }

    private fun lowerBoundEquals(other: IntermediateQueryTerm): Boolean {
        return if (this.lowerBound == null && other.lowerBound == null) {
            true
        } else if (this.lowerBound != null && other.lowerBound != null) {
            Arrays.equals(this.lowerBound, other.lowerBound)
        } else {
            false
        }
    }

    private fun upperBoundEquals(other: IntermediateQueryTerm): Boolean {
        return if (this.upperBound == null && other.upperBound == null) {
            true
        } else if (this.upperBound != null && other.upperBound != null) {
            Arrays.equals(this.upperBound, other.upperBound)
        } else {
            false
        }
    }

    override fun equals(other: Any?): Boolean {
        return if (this === other) { true
        } else if (!IntermediateQueryTerm::class.java.isInstance(other)) {
            super.equals(other)
        } else {
            val otherTerm = other as IntermediateQueryTerm
            if (otherTerm.kind != this.kind) {
                false
            } else if (Arrays.compareUnsigned(otherTerm.key, this.key) != 0) {
                false
            } else if (!this.lowerBoundEquals(otherTerm)) {
                false
            } else {
                this.upperBoundEquals(other)
            }
        }
    }

    override fun hashCode(): Int {
        var result = kind.hashCode()
        result = 31 * result + key.contentHashCode()
        result = 31 * result + (lowerBound?.contentHashCode() ?: 0)
        result = 31 * result + (upperBound?.contentHashCode() ?: 0)
        return result
    }
}

class QuerySpec private constructor(
    internal val equalityTerms: QPTrie<ByteArray>,
    internal val inequalityTerm: IntermediateQueryTerm?
) {
    constructor(): this(QPTrie(), null)

    val cardinality: Int
        get() = this.equalityTerms.size.toInt() + if (this.inequalityTerm == null) { 0 } else { 1 }

    fun withEqualityTerm(key: ByteArray, value: ByteArray): QuerySpec {
        return QuerySpec(this.equalityTerms.put(key, value), this.inequalityTerm)
    }

    internal fun withoutEqualityTerm(key: ByteArray): QuerySpec {
        val replacement = this.equalityTerms.remove(key)
        return if (replacement === this.equalityTerms) {
            this
        } else {
            QuerySpec(replacement, this.inequalityTerm)
        }
    }

    fun withLessThanOrEqualToTerm(key: ByteArray, lessThanOrEqualTo: ByteArray): QuerySpec {
        val term = IntermediateQueryTerm.greaterOrLessTerm(key.copyOf(), null, lessThanOrEqualTo)
        return QuerySpec(this.equalityTerms.remove(key), term)
    }

    fun withBetweenOrEqualToTerm(key: ByteArray, maybeLowerBound: ByteArray, maybeUpperBound: ByteArray): QuerySpec {
        val swapBounds = Arrays.compareUnsigned(maybeLowerBound, maybeUpperBound) == 1
        val lowerBound = if (swapBounds) { maybeUpperBound } else { maybeLowerBound }
        val upperBound = if (swapBounds) { maybeLowerBound } else { maybeUpperBound }
        val term = IntermediateQueryTerm.greaterOrLessTerm(key.copyOf(), lowerBound, upperBound)
        return QuerySpec(this.equalityTerms.remove(key), term)
    }

    fun withGreaterThanOrEqualToTerm(key: ByteArray, greaterThanOrEqualTo: ByteArray): QuerySpec {
        val term = IntermediateQueryTerm.greaterOrLessTerm(key.copyOf(), greaterThanOrEqualTo, null)
        return QuerySpec(this.equalityTerms.remove(key), term)
    }

    fun withStartsWithTerm(key: ByteArray, startsWith: ByteArray): QuerySpec {
        val term = IntermediateQueryTerm.startsWithTerm(key.copyOf(), startsWith)
        return QuerySpec(this.equalityTerms.remove(key), term)
    }

    fun matchesData(data: QPTrie<ByteArray>): Boolean {
        for ((key, value) in this.equalityTerms) {
            val dataAtKey = data.get(key) ?: return false
            if (!value.contentEquals(dataAtKey)) {
                return false
            }
        }
        val inequalityTerm = this.inequalityTerm ?: return true
        val dataAtInequality = data.get(inequalityTerm.key) ?: return false
        return when (inequalityTerm.kind) {
            IntermediateQueryTermKind.EQUALS -> (inequalityTerm.lowerBound!!).contentEquals(dataAtInequality)
            IntermediateQueryTermKind.STARTS_WITH -> {
                val boundSize = inequalityTerm.lowerBound!!.size
                if (boundSize > dataAtInequality.size) {
                    false
                } else {
                    Arrays.equals(
                        inequalityTerm.lowerBound,
                        0,
                        boundSize,
                        dataAtInequality,
                        0,
                        boundSize
                    )
                }
            }
            IntermediateQueryTermKind.GREATER_OR_LESS -> if (inequalityTerm.lowerBound == null && inequalityTerm.upperBound == null) {
                false
            } else if (inequalityTerm.lowerBound != null && inequalityTerm.upperBound != null) {
                Arrays.compareUnsigned(
                    dataAtInequality, inequalityTerm.lowerBound
                ) >= 0 && Arrays.compareUnsigned(
                    dataAtInequality, inequalityTerm.upperBound
                ) <= 0
            } else if (inequalityTerm.upperBound != null) {
                Arrays.compareUnsigned(dataAtInequality, inequalityTerm.upperBound) <= 0
            } else {
                Arrays.compareUnsigned(dataAtInequality, inequalityTerm.lowerBound!!) >= 0
            }
        }
    }

    private fun equalityTermsForComparison(): Array<Pair<ByteArrayButComparable, ByteArrayButComparable>> {
        return this.equalityTerms.toList().map { (key, value) ->
            Pair(ByteArrayButComparable(key), ByteArrayButComparable(value))
        }.toTypedArray()
    }

    override fun equals(other: Any?): Boolean {
        return if (this === other) {
            true
        } else if (QuerySpec::class.java.isInstance(other)) {
            val otherSpec = other as QuerySpec
            if (this.inequalityTerm != otherSpec.inequalityTerm) {
                false
            } else {
                this.equalityTermsForComparison().contentEquals(other.equalityTermsForComparison())
            }
        } else {
            super.equals(other)
        }
    }

    override fun toString(): String {
        val equalityTerms = this.equalityTerms.iteratorUnsafeSharedKey().asSequence().map {
            (key, value) -> Pair(ByteArrayButComparable(key), ByteArrayButComparable(value))
        } .toList()
        return "QuerySpec(equalityTerms=${equalityTerms}, inequalityTerm=${inequalityTerm})"
    }

    override fun hashCode(): Int {
        var result = equalityTerms.hashCode()
        result = 31 * result + (inequalityTerm?.hashCode() ?: 0)
        return result
    }
}

class QueryPath internal constructor(internal val breadcrumbs: ListNode<IntermediateQueryTerm>?) {
    internal constructor(path: Iterable<IntermediateQueryTerm>): this(listFromIterable(path))
    internal constructor(): this(null)

    internal fun first(): IntermediateQueryTerm? {
        return listFirst(this.breadcrumbs)
    }

    internal fun rest(): QueryPath {
        return QueryPath(listRest(this.breadcrumbs))
    }

    internal fun prepend(t: IntermediateQueryTerm): QueryPath {
        return QueryPath(listPrepend(t, this.breadcrumbs))
    }

    override fun equals(other: Any?): Boolean {
        return if (this === other) {
            true
        } else if (QueryPath::class.java.isInstance(other)) {
            other as QueryPath
            listEquals(this.breadcrumbs, other.breadcrumbs)
        } else {
            super.equals(other)
        }
    }

    override fun hashCode(): Int {
        return breadcrumbs.hashCode()
    }

    override fun toString(): String {
        return "QueryPath(${this.breadcrumbs})"
    }
}

interface SizeComputable {
    fun computeSize(): Long
}

data class QueryPathValue<V> internal constructor (val path: QueryPath, val value: V)

internal class QueryTreeTerms<V: SizeComputable>(
    val equalityTerms: QPTrie<QueryTreeNode<V>>?,
    val lessThanTerms: QPTrie<V>?,
    val rangeTerms: IntervalTree<ByteArrayButComparable, V>?,
    val greaterThanTerms: QPTrie<V>?,
    val startsWithTerms: QPTrie<V>?,
) {
    constructor(): this(null, null, null, null, null)

    private fun hasAnyTerms(): Boolean {
        return when {
            this.equalityTerms != null -> true
            this.lessThanTerms != null -> true
            this.rangeTerms != null -> true
            this.greaterThanTerms != null -> true
            this.startsWithTerms != null -> true
            else -> false
        }
    }

    fun replaceEqualityTerms(newTerms: QPTrie<QueryTreeNode<V>>?): QueryTreeTerms<V>? {
        val newTreeTerms = QueryTreeTerms(
            if (newTerms?.size == 0L) { null } else { newTerms },
            this.lessThanTerms,
            this.rangeTerms,
            this.greaterThanTerms,
            this.startsWithTerms
        )
        return if (newTreeTerms.hasAnyTerms()) {
            newTreeTerms
        } else {
            null
        }
    }

    fun replaceLessThanTerms(newTerms: QPTrie<V>?): QueryTreeTerms<V>? {
        val newTreeTerms = QueryTreeTerms(
            this.equalityTerms,
            if (newTerms?.size == 0L) { null } else { newTerms },
            this.rangeTerms,
            this.greaterThanTerms,
            this.startsWithTerms
        )
        return if (newTreeTerms.hasAnyTerms()) {
            newTreeTerms
        } else {
            null
        }
    }

    fun replaceRangeTerms(newTerms: IntervalTree<ByteArrayButComparable, V>?): QueryTreeTerms<V>? {
        val newTreeTerms = QueryTreeTerms(
            this.equalityTerms,
            this.lessThanTerms,
            if (newTerms?.size == 0L) { null } else { newTerms },
            this.greaterThanTerms,
            this.startsWithTerms
        )
        return if (newTreeTerms.hasAnyTerms()) {
            newTreeTerms
        } else {
            null
        }
    }

    fun replaceGreaterThanTerms(newTerms: QPTrie<V>?): QueryTreeTerms<V>? {
        val newTreeTerms = QueryTreeTerms(
            this.equalityTerms,
            this.lessThanTerms,
            this.rangeTerms,
            if (newTerms?.size == 0L) { null } else { newTerms },
            this.startsWithTerms,
        )
        return if (newTreeTerms.hasAnyTerms()) {
            newTreeTerms
        } else {
            null
        }
    }

    fun replaceStartsWithTerms(newTerms: QPTrie<V>?): QueryTreeTerms<V>? {
        val newTreeTerms = QueryTreeTerms(
            this.equalityTerms,
            this.lessThanTerms,
            this.rangeTerms,
            this.greaterThanTerms,
            if (newTerms?.size == 0L) { null } else { newTerms },
        )
        return if (newTreeTerms.hasAnyTerms()) {
            newTreeTerms
        } else {
            null
        }
    }
}

internal class QueryTreeNode<V  : SizeComputable>(
    val value: V?,
    val keys: QPTrie<QueryTreeTerms<V>>,
    val size: Long
) {
    companion object {
        fun <V: SizeComputable>emptyInstance(): QueryTreeNode<V> {
            return QueryTreeNode(
                null,
                QPTrie(),
                0
            )
        }
    }

    private fun replaceValue(value: V?): QueryTreeNode<V>? {
        return if (value === this.value) {
            this
        } else if (value == null && this.keys.size == 0L) {
            null
        } else {
            val sizeDiff = (value?.computeSize() ?: 0) - (this.value?.computeSize() ?: 0)
            val newSize = this.size + sizeDiff
            QueryTreeNode(value, this.keys, newSize)
        }
    }

    private fun replaceKeys(newKeys: QPTrie<QueryTreeTerms<V>>, sizeDiff: Long): QueryTreeNode<V>? {
        return if (newKeys === this.keys) {
            this
        } else if (this.value == null && newKeys.size == 0L) {
            null
        } else {
            val newSize = this.size + sizeDiff
            QueryTreeNode(this.value, newKeys, newSize)
        }
    }

    private fun updateTrieTerms(
        keys: QPTrie<QueryTreeTerms<V>>,
        target: QPTrie<V>?,
        key: ByteArray,
        value: ByteArray,
        updater: (prior: V?) -> V?,
        replacer: (terms: QPTrie<V>) -> QueryTreeTerms<V>?
    ): QueryTreeNode<V>? {
        val existingResultByValue = target?.get(value)
        val newResult = updater(existingResultByValue)
        if (newResult === existingResultByValue) {
            return this
        }

        val sizeDiff = (newResult?.computeSize() ?: 0) - (existingResultByValue?.computeSize() ?: 0)
        val newStartsWithTerms = (target ?: QPTrie()).update(value) { newResult }
        val newTerms = replacer(newStartsWithTerms)
        val newKeys = keys.update(key) { newTerms }
        return this.replaceKeys(newKeys, sizeDiff)
    }

    private fun updateRangeTerms(
        keys: QPTrie<QueryTreeTerms<V>>,
        termsByKey: QueryTreeTerms<V>,
        key: ByteArray,
        lowerBound: ByteArray,
        upperBound: ByteArray,
        updater: (prior: V?) -> V?
    ): QueryTreeNode<V>? {
        val rangeTerms = termsByKey.rangeTerms
        val lowerBoundComparable = ByteArrayButComparable(lowerBound)
        val upperBoundComparable = ByteArrayButComparable(upperBound)
        val rangeValue = lowerBoundComparable to upperBoundComparable
        val existingResultByValue = rangeTerms?.lookupExactRange(rangeValue)
        val newResult = updater(existingResultByValue)
        if (newResult === existingResultByValue) {
            return this
        }
        val sizeDiff = (newResult?.computeSize() ?: 0) - (existingResultByValue?.computeSize() ?: 0)
        val newRangeTerms = (rangeTerms ?: IntervalTree()).update(rangeValue) { newResult }
        val newTerms = termsByKey.replaceRangeTerms(newRangeTerms)
        val newKeys = keys.update(key) { newTerms }
        return this.replaceKeys(newKeys, sizeDiff)
    }

    fun updateByPath(path: QueryPath, updater: (prior: V?) -> V?): QueryTreeNode<V>? {
        val currentPath = path.first() ?: return this.replaceValue(updater(this.value))
        val keys = this.keys
        val currentKey = currentPath.key
        val termsByKey = this.keys.get(currentKey) ?: QueryTreeTerms()
        when (currentPath.kind) {
            IntermediateQueryTermKind.EQUALS -> {
                if (currentPath.lowerBound == null) {
                    return this
                }
                val value = currentPath.lowerBound
                val equalityTerms = termsByKey.equalityTerms ?: QPTrie()
                val existingNodeByValue = equalityTerms.get(value)

                // This is an entirely new value, so we'll be strictly adding to the key set.
                if (existingNodeByValue == null) {
                    val updateValue = updater(null) ?: return this
                    val subNode = emptyInstance<V>().updateByPath(path.rest()) { updateValue } ?: return this
                    val newEqualityTerms = equalityTerms.update(value) { subNode }
                    val newTerms = termsByKey.replaceEqualityTerms(newEqualityTerms)
                    val newKeys = keys.update(currentKey) { newTerms }
                    return this.replaceKeys(newKeys, updateValue.computeSize())
                }

                val newNodeByValue = existingNodeByValue.updateByPath(path.rest(), updater)
                val sizeDiff = (newNodeByValue?.size ?: 0) - existingNodeByValue.size
                val newKeys = this.keys.update(currentKey) {
                    // We know we had prior terms, because we were able to find a value
                    // in the equalityTerms above. This could not have happened for entirely
                    // new terms.
                    val priorTerms = it!!
                    val priorEqualityTerms = priorTerms.equalityTerms!!
                    val newEqualityTerms = priorEqualityTerms.update(value) { newNodeByValue }
                    if (newEqualityTerms === priorEqualityTerms) {
                        priorTerms
                    } else {
                        priorTerms.replaceEqualityTerms(newEqualityTerms)
                    }
                }
                return this.replaceKeys(newKeys, sizeDiff)
            }
            IntermediateQueryTermKind.GREATER_OR_LESS -> {
                val lowerBound = currentPath.lowerBound
                val upperBound = currentPath.upperBound
                if (lowerBound == null && upperBound == null) {
                    return this
                }
                if (lowerBound == null) {
                    // upperBound != null
                    return this.updateTrieTerms(
                        keys,
                        termsByKey.lessThanTerms,
                        currentKey,
                        upperBound!!,
                        updater
                    ) {
                        termsByKey.replaceLessThanTerms(it)
                    }
                }
                if (upperBound == null) {
                    // lowerBound != null
                    return this.updateTrieTerms(
                        keys,
                        termsByKey.greaterThanTerms,
                        currentKey,
                        lowerBound,
                        updater
                    ) {
                        termsByKey.replaceGreaterThanTerms(it)
                    }
                }

                // We now have to update the range terms.
                return this.updateRangeTerms(
                    keys,
                    termsByKey,
                    currentKey,
                    lowerBound,
                    upperBound,
                    updater
                )
            }
            IntermediateQueryTermKind.STARTS_WITH -> {
                val value = currentPath.lowerBound ?: return this
                return this.updateTrieTerms(
                    keys,
                    termsByKey.startsWithTerms,
                    currentKey,
                    value,
                    updater
                ) {
                    termsByKey.replaceStartsWithTerms(it)
                }
            }
        }
    }

    fun getByPath(queryPath: QueryPath): V? {
        val currentPath = queryPath.first() ?: return this.value
        when (currentPath.kind) {
            IntermediateQueryTermKind.EQUALS -> {
                if (currentPath.lowerBound == null) {
                    return null
                }
                val subNode = this.keys.get(currentPath.key)?.equalityTerms?.get(currentPath.lowerBound) ?: return null
                return subNode.getByPath(queryPath.rest())
            }
            IntermediateQueryTermKind.GREATER_OR_LESS -> {
                return if (currentPath.lowerBound == null && currentPath.upperBound == null) {
                    null
                } else if (currentPath.lowerBound != null && currentPath.upperBound != null) {
                    val lowerBoundComparable = ByteArrayButComparable(currentPath.lowerBound)
                    val upperBoundComparable = ByteArrayButComparable(currentPath.upperBound)
                    val targetRange = Pair(lowerBoundComparable, upperBoundComparable)
                    this.keys.get(currentPath.key)?.rangeTerms?.lookupExactRange(targetRange)
                } else if (currentPath.lowerBound != null) {
                    // currentPath.upperBound == null
                    this.keys.get(currentPath.key)?.greaterThanTerms?.get(currentPath.lowerBound)
                } else {
                    // currentPath.lowerBound == null, currentPath.upperBound != null
                    this.keys.get(currentPath.key)?.lessThanTerms?.get(currentPath.upperBound!!)
                }
            }
            IntermediateQueryTermKind.STARTS_WITH -> {
                if (currentPath.lowerBound == null) {
                    return null
                }
                return this.keys.get(currentPath.key)?.startsWithTerms?.get(currentPath.lowerBound)
            }
        }
    }

    private fun updateForBestKeyValuePair(
        bestKeyValue: QPTrieKeyValue<ByteArray>,
        updater: (prior: V?) -> V?,
        querySpec: QuerySpec,
    ): Pair<QueryPath, QueryTreeNode<V>>? {
        val (bestKey, bestValue) = bestKeyValue
        val keys = this.keys
        val target = keys.get(bestKey)?.equalityTerms?.get(bestValue) ?: emptyInstance()
        val queryTerm = IntermediateQueryTerm.equalityTerm(bestKey, bestValue)
        val replacementSpecs = target.updateByQuery(querySpec.withoutEqualityTerm(bestKey), updater)
        val sizeDiff = (replacementSpecs?.second?.size ?: 0) - target.size
        val newKeys = keys.update(bestKey) {
            val terms = it ?: QueryTreeTerms()
            val equalityTerms = terms.equalityTerms ?: QPTrie()
            val newEqualityTerms = equalityTerms.update(bestValue) { replacementSpecs?.second }
            if (newEqualityTerms === equalityTerms) {
                it
            } else {
                terms.replaceEqualityTerms(newEqualityTerms)
            }
        }
        val selfReplacement = if (newKeys === this.keys) {
            this
        } else {
            QueryTreeNode(
                this.value,
                newKeys,
                this.size + sizeDiff
            )
        }
        return if (selfReplacement.size == 0L) {
            null
        } else if (replacementSpecs == null) {
            Pair(QueryPath(ListNode(queryTerm)), selfReplacement)
        } else {
            Pair(replacementSpecs.first.prepend(queryTerm), selfReplacement)
        }
    }

    fun updateByQuery(querySpec: QuerySpec, updater: (prior: V?) -> V?): Pair<QueryPath, QueryTreeNode<V>>? {
        val currentTerms = querySpec.equalityTerms
        if (currentTerms.size == 0L) {
            val inequalityTerm = querySpec.inequalityTerm
            // If we don't have any further inequality checks to perform on this value,
            // and we're all out of equality terms, the value is present on this.
            if (inequalityTerm == null) {
                val updated = updater(this.value)
                return if (updated === this.value) {
                    Pair(QueryPath(), this)
                } else {
                    val replacement = this.replaceValue(updated) ?: return null
                    Pair(QueryPath(), replacement)
                }
            }
            val resultPath = QueryPath(ListNode(inequalityTerm))
            when (inequalityTerm.kind) {
                IntermediateQueryTermKind.EQUALS -> {
                    // This should not have happened.
                    return Pair(resultPath, this)
                }
                IntermediateQueryTermKind.STARTS_WITH -> {
                    val value = inequalityTerm.lowerBound
                    return if (value == null) {
                        Pair(QueryPath(), this)
                    } else {
                        val key = inequalityTerm.key
                        val keys = this.keys
                        val termsForKey = keys.get(key) ?: QueryTreeTerms()
                        val newNode = this.updateTrieTerms(
                            keys,
                            termsForKey.startsWithTerms,
                            key,
                            value,
                            updater
                        ) {
                            termsForKey.replaceStartsWithTerms(it)
                        } ?: return null
                        Pair(resultPath, newNode)
                    }
                }
                IntermediateQueryTermKind.GREATER_OR_LESS -> {
                    val lowerBound = inequalityTerm.lowerBound
                    val upperBound = inequalityTerm.upperBound
                    val key = inequalityTerm.key
                    val keys = this.keys
                    val termsForKey = keys.get(key) ?: QueryTreeTerms()
                    val newNode = (if (lowerBound == null && upperBound == null) {
                        this
                    } else if (lowerBound != null && upperBound != null) {
                        this.updateRangeTerms(
                            keys,
                            termsForKey,
                            key,
                            lowerBound,
                            upperBound,
                            updater
                        )
                    } else if (lowerBound != null) {
                        this.updateTrieTerms(
                            keys,
                            termsForKey.greaterThanTerms,
                            key,
                            lowerBound,
                            updater
                        ) {
                            termsForKey.replaceGreaterThanTerms(it)
                        }
                    } else {
                        this.updateTrieTerms(
                            keys,
                            termsForKey.lessThanTerms,
                            key,
                            upperBound!!,
                            updater
                        ) {
                            termsForKey.replaceLessThanTerms(it)
                        }
                    }) ?: return null
                    return Pair(resultPath, newNode)
                }
            }
        }
        // We now know there is at least one equality term to try.
        // The heuristic is:
        // - If there's any matching key-value pairs at all, update the key-value pair with the lowest size
        // - If there's no matching values at all, but we have entries for any keys, update the key
        //   with the lowest-size subtree
        // - If there's no matching keys at all, choose whichever key and keep going.
        var bestKeyValue: QPTrieKeyValue<ByteArray>? = null
        var bestKeyValueSize = Long.MAX_VALUE
        var bestKeyOnly: QPTrieKeyValue<ByteArray>? = null
        var bestKeyOnlySize = Long.MAX_VALUE
        var firstKeyValue: QPTrieKeyValue<ByteArray>? = null
        for (kvp in currentTerms) {
            if (firstKeyValue == null) {
                firstKeyValue = kvp
            }
            val termPairs = this.keys.get(kvp.key) ?: continue
            val valuePairs = termPairs.equalityTerms ?: continue
            if (valuePairs.size <= bestKeyOnlySize) {
                bestKeyOnly = kvp
                bestKeyOnlySize = valuePairs.size
            }
            val priorSubNode = valuePairs.get(kvp.value) ?: continue
            if (priorSubNode.size <= bestKeyValueSize) {
                bestKeyValue = kvp
                bestKeyValueSize = priorSubNode.size
            }
        }
        val updateKeyValue = (bestKeyValue ?: (bestKeyOnly ?: firstKeyValue)) ?: return null
        return this.updateForBestKeyValuePair(updateKeyValue, updater, querySpec)
    }
}

class QueryTree<V: SizeComputable> private constructor(
    private val root: QueryTreeNode<V>?
): Iterable<QueryPathValue<V>> {
    data class PathTreeResult<V: SizeComputable> internal constructor(val path: QueryPath, val tree: QueryTree<V>)

    val size: Long = this.root?.size ?: 0

    constructor(): this(null)

    fun getByPath(queryPath: QueryPath): V? {
        return if (this.root == null) {
            null
        } else {
            this.root.getByPath(queryPath)
        }
    }

    fun updateByPath(queryPath: QueryPath, updater: (prior: V?) -> V?): QueryTree<V> {
        val oldRoot = (this.root ?: QueryTreeNode.emptyInstance())
        val newRoot = oldRoot.updateByPath(queryPath, updater)
        if (oldRoot === newRoot) {
            return this
        }
        val newSize = newRoot?.size ?: 0
        return QueryTree(
            if (newSize == 0L) { null } else { newRoot }
        )
    }

    fun updateByQuery(querySpec: QuerySpec, updater: (prior: V?) -> V?): PathTreeResult<V> {
        val oldRoot = (this.root ?: QueryTreeNode.emptyInstance())
        val (newPath, newRoot) = oldRoot.updateByQuery(querySpec, updater) ?: return PathTreeResult(
            QueryPath(),
            QueryTree()
        )
        return if (newRoot === oldRoot) {
            PathTreeResult(newPath, this)
        } else {
            PathTreeResult(newPath, QueryTree(newRoot))
        }
    }

    fun getByData(data: QPTrie<ByteArray>): Iterator<QueryPathValue<V>> {
        return if (this.root == null) {
            EmptyIterator()
        } else {
            GetByDataIterator(this.root, data)
        }
    }

    override fun iterator(): Iterator<QueryPathValue<V>> {
        return if (this.root == null) {
            EmptyIterator()
        } else {
            FullTreeIterator(this.root, null)
        }
    }
}

internal enum class GetTermsByDataIteratorState {
    EQUALITY,
    LESS_THAN,
    RANGE,
    GREATER_THAN,
    STARTS_WITH,
    DONE
}

internal class GetTermsByDataIterator<V : SizeComputable> private constructor(
    private val key: ByteArray,
    private val value: ByteArray,
    private val terms: QueryTreeTerms<V>,
    private val reversePath: ListNode<IntermediateQueryTerm>?,
    private val fullData: QPTrie<ByteArray>,
    private var state: GetTermsByDataIteratorState
): ConcatenatedIterator<QueryPathValue<V>>() {

    constructor(
        key: ByteArray,
        value: ByteArray,
        terms: QueryTreeTerms<V>,
        fullData: QPTrie<ByteArray>,
        reversePath: ListNode<IntermediateQueryTerm>?
    ): this(key, value, terms, reversePath, fullData, GetTermsByDataIteratorState.EQUALITY)

    override fun iteratorForOffset(offset: Int): Iterator<QueryPathValue<V>>? {
        val terms = this.terms
        val key = this.key
        val value = this.value
        val reversePath = this.reversePath
        val fullData = this.fullData
        while (this.state != GetTermsByDataIteratorState.DONE) {
            when (this.state) {
                GetTermsByDataIteratorState.EQUALITY -> {
                    this.state = GetTermsByDataIteratorState.LESS_THAN
                    val subNode = terms.equalityTerms?.get(value) ?: continue
                    val term = IntermediateQueryTerm.equalityTerm(key, value)
                    return this.registerChild(GetByDataIterator(subNode, fullData, listPrepend(term, reversePath)))
                }

                GetTermsByDataIteratorState.LESS_THAN -> {
                    this.state = GetTermsByDataIteratorState.RANGE
                    val result = terms.lessThanTerms?.iteratorGreaterThanOrEqualUnsafeSharedKey(value) ?: continue
                    return mapSequence(result) {
                        val term = IntermediateQueryTerm.greaterOrLessTerm(key, null, it.key)
                        QueryPathValue(QueryPath(listReverse(listPrepend(term, reversePath))), it.value)
                    }
                }

                GetTermsByDataIteratorState.RANGE -> {
                    this.state = GetTermsByDataIteratorState.GREATER_THAN
                    val result = terms.rangeTerms?.lookupPoint(ByteArrayButComparable(value)) ?: continue
                    return mapSequence(result) {
                        val bounds = it.key
                        val term = IntermediateQueryTerm.greaterOrLessTerm(
                            key,
                            bounds.lowerBound.array,
                            bounds.upperBound.array
                        )
                        QueryPathValue(QueryPath(listReverse(listPrepend(term, reversePath))), it.value)
                    }
                }

                GetTermsByDataIteratorState.GREATER_THAN -> {
                    this.state = GetTermsByDataIteratorState.STARTS_WITH
                    val result = terms.greaterThanTerms?.iteratorLessThanOrEqualUnsafeSharedKey(value) ?: continue
                    return mapSequence(result) {
                        val term = IntermediateQueryTerm.greaterOrLessTerm(key, it.key, null)
                        QueryPathValue(QueryPath(listReverse(listPrepend(term, reversePath))), it.value)
                    }
                }

                GetTermsByDataIteratorState.STARTS_WITH -> {
                    this.state = GetTermsByDataIteratorState.DONE
                    val result = terms.startsWithTerms?.iteratorPrefixOfOrEqualToUnsafeSharedKey(value) ?: continue
                    return mapSequence(result) {
                        val term = IntermediateQueryTerm.startsWithTerm(key, it.key)
                        QueryPathValue(QueryPath(listReverse(listPrepend(term, reversePath))), it.value)
                    }
                }

                GetTermsByDataIteratorState.DONE -> {
                    break
                }
            }
        }
        return null
    }
}

internal enum class GetByDataIteratorState {
    VALUE,
    TERMS,
    DONE
}

internal class GetByDataIterator<V: SizeComputable> private constructor(
    private val node: QueryTreeNode<V>,
    private val reversePath: ListNode<IntermediateQueryTerm>?,
    private val fullData: QPTrie<ByteArray>,
    private var state: GetByDataIteratorState,
): ConcatenatedIterator<QueryPathValue<V>>() {
    private val keys = workingDataForAvailableKeys(fullData, node.keys).keysIntoUnsafeSharedKey(ArrayList())
    private var keyOffset = 0

    constructor(
        node: QueryTreeNode<V>,
        fullData: QPTrie<ByteArray>,
        reversePath: ListNode<IntermediateQueryTerm>?
    ): this(node, reversePath, fullData, GetByDataIteratorState.VALUE)

    constructor(
        node: QueryTreeNode<V>,
        fullData: QPTrie<ByteArray>
    ): this(node, fullData, null)

    override fun iteratorForOffset(offset: Int): Iterator<QueryPathValue<V>>? {
        if (this.state != GetByDataIteratorState.VALUE && this.fullData.size == 0L) {
            return null
        }
        while (this.state != GetByDataIteratorState.DONE) {
            val node = this.node
            val reversePath = this.reversePath
            when (this.state) {
                GetByDataIteratorState.VALUE -> {
                    this.state = GetByDataIteratorState.TERMS
                    if (node.value != null) {
                        return SingleElementIterator(
                            QueryPathValue(QueryPath(listReverse(reversePath)), node.value)
                        )
                    }
                }

                GetByDataIteratorState.TERMS -> {
                    val keyOffset = this.keyOffset
                    val keys = this.keys
                    if (keyOffset >= keys.size) {
                        this.state = GetByDataIteratorState.DONE
                    } else {
                        val targetKey = keys[keyOffset]
                        val fullData = this.fullData
                        this.keyOffset++
                        val targetTerms = node.keys.get(targetKey) ?: continue
                        val value = fullData.get(targetKey) ?: continue
                        return this.registerChild(GetTermsByDataIterator(
                            targetKey,
                            value,
                            targetTerms,
                            fullData.remove(targetKey),
                            reversePath
                        ))
                    }
                }

                GetByDataIteratorState.DONE -> {
                    break
                }
            }
        }
        return null
    }
}

internal class FullTermsByDataIterator<V : SizeComputable> private constructor (
    private val key: ByteArray,
    private val terms: QueryTreeTerms<V>,
    private val reversePath: ListNode<IntermediateQueryTerm>?,
    private var state: GetTermsByDataIteratorState
): ConcatenatedIterator<QueryPathValue<V>>() {
    constructor(
        key: ByteArray,
        terms: QueryTreeTerms<V>,
        reversePath: ListNode<IntermediateQueryTerm>?
    ): this(key, terms, reversePath, GetTermsByDataIteratorState.EQUALITY)

    override fun iteratorForOffset(offset: Int): Iterator<QueryPathValue<V>>? {
        val key = this.key
        val reversePath = this.reversePath

        while (this.state != GetTermsByDataIteratorState.DONE) {
            when (this.state) {
                GetTermsByDataIteratorState.EQUALITY -> {
                    this.state = GetTermsByDataIteratorState.LESS_THAN
                    val equalityTerms = this.terms.equalityTerms ?: continue
                    return FlattenIterator(mapSequence(equalityTerms) {
                        val term = IntermediateQueryTerm.equalityTerm(key, it.key)
                        this.registerChild(FullTreeIterator(it.value, listPrepend(term, reversePath)))
                    })
                }

                GetTermsByDataIteratorState.LESS_THAN -> {
                    this.state = GetTermsByDataIteratorState.RANGE
                    val lessThanTerms = this.terms.lessThanTerms ?: continue
                    return mapSequence(lessThanTerms) {
                        val term = IntermediateQueryTerm.greaterOrLessTerm(key,null, it.key)
                        QueryPathValue(QueryPath(listReverse(listPrepend(term, reversePath))), it.value)
                    }
                }

                GetTermsByDataIteratorState.RANGE -> {
                    this.state = GetTermsByDataIteratorState.GREATER_THAN
                    val rangeTerms = this.terms.rangeTerms ?: continue
                    return mapSequence(rangeTerms) {
                        val value = it.first
                        val term = IntermediateQueryTerm.greaterOrLessTerm(
                            key,
                            value.lowerBound.array,
                            value.upperBound.array
                        )
                        QueryPathValue(QueryPath(listReverse(listPrepend(term, reversePath))), it.second)
                    }
                }

                GetTermsByDataIteratorState.GREATER_THAN -> {
                    this.state = GetTermsByDataIteratorState.STARTS_WITH
                    val greaterThanTerms = this.terms.greaterThanTerms ?: continue
                    return mapSequence(greaterThanTerms) {
                        val term = IntermediateQueryTerm.greaterOrLessTerm(key, it.key, null)
                        QueryPathValue(QueryPath(listReverse(listPrepend(term, reversePath))), it.value)
                    }
                }

                GetTermsByDataIteratorState.STARTS_WITH -> {
                    this.state = GetTermsByDataIteratorState.DONE
                    val startsWithTerms = this.terms.startsWithTerms ?: continue
                    return mapSequence(startsWithTerms) {
                        val term = IntermediateQueryTerm.startsWithTerm(key, it.key)
                        QueryPathValue(QueryPath(listReverse(listPrepend(term, reversePath))), it.value)
                    }
                }

                GetTermsByDataIteratorState.DONE -> {
                    break
                }
            }
        }
        return null
    }
}

internal class FullTreeIterator<V: SizeComputable> private constructor(
    private val node: QueryTreeNode<V>,
    private val reversePath: ListNode<IntermediateQueryTerm>?,
    private var state: GetByDataIteratorState
): ConcatenatedIterator<QueryPathValue<V>>() {
    constructor (
        node: QueryTreeNode<V>,
        reversePath: ListNode<IntermediateQueryTerm>?
    ): this(node, reversePath, GetByDataIteratorState.VALUE)

    override fun iteratorForOffset(offset: Int): Iterator<QueryPathValue<V>>? {
        val node = this.node
        val reversePath = this.reversePath
        while (this.state != GetByDataIteratorState.DONE) {
            when (this.state) {
                GetByDataIteratorState.VALUE -> {
                    this.state = GetByDataIteratorState.TERMS
                    if (node.value != null) {
                        return SingleElementIterator(
                            QueryPathValue(QueryPath(listReverse(reversePath)), node.value)
                        )
                    }
                }

                GetByDataIteratorState.TERMS -> {
                    this.state = GetByDataIteratorState.DONE
                    return FlattenIterator(mapSequence(node.keys) {
                        this.registerChild(FullTermsByDataIterator(it.key, it.value, reversePath))
                    })
                }

                GetByDataIteratorState.DONE -> {
                    break
                }
            }
        }
        return null
    }
}

internal class SetWithCardinality<V> private constructor(
    val set: IdentitySet<V>,
): SizeComputable, Iterable<V> {
    constructor(): this(IdentitySet())

    override fun computeSize(): Long {
        return this.set.size
    }

    fun add(elem: V): SetWithCardinality<V> {
        return if (this.contains(elem)) {
            this
        } else {
            SetWithCardinality(this.set.add(elem))
        }
    }

    fun contains(elem: V): Boolean {
        return this.set.contains(elem)
    }

    fun remove(elem: V): SetWithCardinality<V> {
        return if (!this.contains(elem)) {
            this
        } else {
            SetWithCardinality(this.set.remove(elem))
        }
    }

    override fun iterator(): Iterator<V> {
        return this.set.iterator()
    }

    override fun toString(): String {
        return "SetWithCardinality(${this.set.toList()})"
    }
}

class QuerySetTree<V> private constructor(
    private val queryTree: QueryTree<SetWithCardinality<V>>,
): Iterable<QueryPathValue<V>> {
    data class PathTreeResult<V> internal constructor(val path: QueryPath, val tree: QuerySetTree<V>)

    val size: Long = this.queryTree.size

    constructor(): this(QueryTree())

    fun getByPath(queryPath: QueryPath): Iterator<V> {
        val setResult = this.queryTree.getByPath(queryPath)
        return setResult?.iterator() ?: EmptyIterator()
    }

    fun addElementByPath(queryPath: QueryPath, elem: V): QuerySetTree<V> {
        val newTree = this.queryTree.updateByPath(queryPath) { prior ->
            (prior ?: SetWithCardinality()).add(elem)
        }
        return if (newTree === this.queryTree) {
            this
        } else {
            QuerySetTree(newTree)
        }
    }

    fun removeElementByPath(queryPath: QueryPath, elem: V): QuerySetTree<V> {
        val newTree = this.queryTree.updateByPath(queryPath) { prior ->
            if (prior == null) {
                null
            } else {
                val next = prior.remove(elem)
                if (next.set.size == 0L) {
                    null
                } else {
                    next
                }
            }
        }
        return if (newTree === this.queryTree) {
            this
        } else {
            QuerySetTree(newTree)
        }
    }

    fun addElementByQuery(querySpec: QuerySpec, elem: V): PathTreeResult<V> {
        val (newPath, newTree) = this.queryTree.updateByQuery(querySpec) { prior ->
            (prior ?: SetWithCardinality()).add(elem)
        }
        return if (newTree === this.queryTree) {
            PathTreeResult(newPath, this)
        } else {
            PathTreeResult(newPath, QuerySetTree(newTree))
        }
    }

    fun getByData(data: QPTrie<ByteArray>): Iterator<QueryPathValue<V>> {
        return FlattenIterator(mapSequence(this.queryTree.getByData(data)) { (path, valueSet) ->
            mapSequence(valueSet) { QueryPathValue(path, it) }
        })
    }

    override fun iterator(): Iterator<QueryPathValue<V>> {
        return FlattenIterator(mapSequence(this.queryTree) { (path, valueSet) ->
            mapSequence(valueSet) { QueryPathValue(path, it) }
        })
    }
}