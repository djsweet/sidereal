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

internal class QueryTreeNode<V  : SizeComputable>(
    val value: V?,
    val equalityTerms: QPTrie<QPTrie<QueryTreeNode<V>>>?,
    val lessThanTerms: QPTrie<QPTrie<V>>?,
    val rangeTerms: QPTrie<IntervalTree<ByteArrayButComparable, V>>?,
    val greaterThanTerms: QPTrie<QPTrie<V>>?,
    val startsWithTerms: QPTrie<QPTrie<V>>?,
    val size: Long
) {
    companion object {
        fun <V: SizeComputable>emptyInstance(): QueryTreeNode<V> {
            return QueryTreeNode(
                null,
                null,
                null,
                null,
                null,
                null,
                0
            )
        }
    }

    private fun hasOtherTerms(): Boolean {
        if ((this.equalityTerms != null) && (this.equalityTerms.size > 0)) {
            return true
        }
        if ((this.lessThanTerms != null) && (this.lessThanTerms.size > 0)) {
            return true
        }
        if ((this.rangeTerms != null) && (this.rangeTerms.size > 0)) {
            return true
        }
        if ((this.greaterThanTerms != null) && (this.greaterThanTerms.size > 0)) {
            return true
        }
        return (this.startsWithTerms != null) && (this.startsWithTerms.size > 0)
    }

    private fun replaceValue(newValue: V?): QueryTreeNode<V> {
        return QueryTreeNode(
            newValue,
            this.equalityTerms,
            this.lessThanTerms,
            this.rangeTerms,
            this.greaterThanTerms,
            this.startsWithTerms,
            this.size - (this.value?.computeSize() ?: 0) + (newValue?.computeSize() ?: 0)
        )
    }

    private fun replaceEqualityTerms(newTerms: QPTrie<QPTrie<QueryTreeNode<V>>>?, sizeDiff: Long): QueryTreeNode<V> {
        return QueryTreeNode(
            this.value,
            if (newTerms?.size == 0L) { null } else { newTerms },
            this.lessThanTerms,
            this.rangeTerms,
            this.greaterThanTerms,
            this.startsWithTerms,
            this.size + sizeDiff
        )
    }

    private fun replaceLessThanTerms(newTerms: QPTrie<QPTrie<V>>?, sizeDiff: Long): QueryTreeNode<V> {
        return QueryTreeNode(
            this.value,
            this.equalityTerms,
            if (newTerms?.size == 0L) { null } else { newTerms },
            this.rangeTerms,
            this.greaterThanTerms,
            this.startsWithTerms,
            this.size + sizeDiff
        )
    }

    private fun replaceRangeTerms(newTerms: QPTrie<IntervalTree<ByteArrayButComparable, V>>?, sizeDiff: Long): QueryTreeNode<V> {
        return QueryTreeNode(
            this.value,
            this.equalityTerms,
            this.lessThanTerms,
            if (newTerms?.size == 0L) { null } else { newTerms },
            this.greaterThanTerms,
            this.startsWithTerms,
            this.size + sizeDiff
        )
    }

    private fun replaceGreaterThanTerms(newTerms: QPTrie<QPTrie<V>>?, sizeDiff: Long): QueryTreeNode<V> {
        return QueryTreeNode(
            this.value,
            this.equalityTerms,
            this.lessThanTerms,
            this.rangeTerms,
            if (newTerms?.size == 0L) { null } else { newTerms },
            this.startsWithTerms,
            this.size + sizeDiff
        )
    }

    private fun replaceStartsWithTerms(newTerms: QPTrie<QPTrie<V>>?, sizeDiff: Long): QueryTreeNode<V> {
        return QueryTreeNode(
            this.value,
            this.equalityTerms,
            this.lessThanTerms,
            this.rangeTerms,
            this.greaterThanTerms,
            if (newTerms?.size == 0L) { null } else { newTerms },
            this.size + sizeDiff
        )
    }

    private fun updateByPathForSingleBound(
        key: ByteArray,
        bound: ByteArray,
        terms: QPTrie<QPTrie<V>>?,
        updater: (prior: V?) -> V?,
        replacer: (newTree: QPTrie<QPTrie<V>>?, sizeDiff: Long) -> QueryTreeNode<V>
    ): QueryTreeNode<V>? {
        val priorTermsForKey = terms?.get(key)
        val priorTerm = priorTermsForKey?.get(bound)
        if (priorTerm == null) {
            val updateValue = updater(null) ?: return this
            val bottomTree = (priorTermsForKey ?: QPTrie()).put(bound, updateValue)
            val topTree = (terms ?: QPTrie()).put(key, bottomTree)
            return replacer(topTree, updateValue.computeSize())
        }

        var sizeDiff = 0L
        val replacementTerms = terms.update(key) { valuesTree ->
            val nextValuesTree = (valuesTree ?: QPTrie()).update(bound) { priorValue ->
                val updateValue = updater(priorValue)
                sizeDiff = (updateValue?.computeSize() ?: 0) - (priorValue?.computeSize() ?: 0)
                updateValue
            }
            if (nextValuesTree.size == 0L) { null } else { nextValuesTree }
        }
        if (replacementTerms === terms) {
            return this
        }
        val possibly = replacer(replacementTerms, sizeDiff)
        return if (possibly.value == null && !possibly.hasOtherTerms()) {
            null
        } else {
            possibly
        }
    }

    private fun updateByPathForLessThan(
        key: ByteArray,
        upperBound: ByteArray,
        updater: (prior: V?) -> V?
    ): QueryTreeNode<V>? {
        return this.updateByPathForSingleBound(key, upperBound, this.lessThanTerms, updater) { newTree, sizeDiff ->
            this.replaceLessThanTerms(newTree, sizeDiff)
        }
    }

    private fun updateByPathForRange(
        key: ByteArray,
        lowerBound: ByteArray,
        upperBound: ByteArray,
        updater: (prior: V?) -> V?
    ): QueryTreeNode<V>? {
        val lowerBoundComparable = ByteArrayButComparable(lowerBound)
        val upperBoundComparable = ByteArrayButComparable(upperBound)
        val targetRange = Pair(lowerBoundComparable, upperBoundComparable)
        val priorRangeTermsForKey = this.rangeTerms?.get(key)
        val priorRangeTerm = priorRangeTermsForKey?.lookupExactRange(targetRange)
        if (priorRangeTerm == null) {
            val updateValue = updater(null) ?: return this
            val bottomTree = (priorRangeTermsForKey ?: IntervalTree()).put(targetRange, updateValue)
            val topTree = (this.rangeTerms ?: QPTrie()).put(key, bottomTree)
            return this.replaceRangeTerms(topTree, updateValue.computeSize())
        }

        var sizeDiff = 0L
        val replacementTerms = (this.rangeTerms ?: QPTrie()).update(key) { valuesTree ->
            val nextValuesTree = (valuesTree ?: IntervalTree()).update(targetRange) {priorValue ->
                val updateValue = updater(priorValue)
                sizeDiff = (updateValue?.computeSize() ?: 0) - (priorValue?.computeSize() ?: 0)
                updateValue
            }
            if (nextValuesTree.size == 0L) { null } else { nextValuesTree }
        }
        if (replacementTerms === this.rangeTerms) {
            return this
        }
        val possibly = this.replaceRangeTerms(replacementTerms, sizeDiff)
        return if (possibly.value == null && !possibly.hasOtherTerms()) {
            null
        } else {
            possibly
        }
    }

    private fun updateByPathForGreaterThan(
        key: ByteArray,
        lowerBound: ByteArray,
        updater: (prior: V?) -> V?
    ): QueryTreeNode<V>? {
        return this.updateByPathForSingleBound(key, lowerBound, this.greaterThanTerms, updater) { newTree, sizeDiff ->
            this.replaceGreaterThanTerms(newTree, sizeDiff)
        }
    }

    private fun updateByPathForStartsWith(
        key: ByteArray,
        lowerBound: ByteArray,
        updater: (prior: V?) -> V?
    ): QueryTreeNode<V>? {
        return this.updateByPathForSingleBound(key, lowerBound, this.startsWithTerms, updater) { newTree, sizeDiff ->
            this.replaceStartsWithTerms(newTree, sizeDiff)
        }
    }

    fun updateByPath(path: QueryPath, updater: (prior: V?) -> V?): QueryTreeNode<V>? {
        val currentPath = path.first()
        if (currentPath == null) {
            val update = updater(this.value)
            if (update === this.value) {
                return this
            }
            if (update == null && !this.hasOtherTerms()) {
                return null
            }
            return this.replaceValue(update)
        }
        when (currentPath.kind) {
            IntermediateQueryTermKind.EQUALS -> {
                if (currentPath.lowerBound == null) {
                    return this
                }
                val priorEqualityTermsForKey = this.equalityTerms?.get(currentPath.key)
                val priorEqualityTerm = priorEqualityTermsForKey?.get(currentPath.lowerBound)
                if (priorEqualityTerm == null) {
                    val updateValue = updater(null) ?: return this
                    val subNode = emptyInstance<V>().updateByPath(path.rest()) { updateValue } ?: return this
                    val bottomTree = (priorEqualityTermsForKey ?: QPTrie()).put(currentPath.lowerBound, subNode)
                    val topTree = (this.equalityTerms ?: QPTrie()).put(currentPath.key, bottomTree)
                    return this.replaceEqualityTerms(
                        topTree,
                        updateValue.computeSize()
                    )
                }

                var sizeDiff = 0L
                val replacementTerms = (this.equalityTerms ?: QPTrie()).update(currentPath.key) { valuesTree ->
                    val nextValuesTree = (valuesTree ?: QPTrie()).update(currentPath.lowerBound) { subNode ->
                        if (subNode == null) {
                            val updateValue = updater(null)
                            if (updateValue == null) {
                                null
                            } else {
                                sizeDiff = updateValue.computeSize()
                                emptyInstance<V>().updateByPath(path.rest()) { updateValue }
                            }
                        } else {
                            val newSubNode = subNode.updateByPath(path.rest(), updater)
                            sizeDiff = (newSubNode?.size ?: 0) - subNode.size
                            newSubNode
                        }
                    }
                    if (nextValuesTree.size == 0L) { null } else { nextValuesTree }
                }
                if (replacementTerms === this.equalityTerms) {
                    return this
                }
                val possibly = this.replaceEqualityTerms(replacementTerms, sizeDiff)
                return if (possibly.value == null && !possibly.hasOtherTerms()) {
                    null
                } else {
                    possibly
                }
            }
            IntermediateQueryTermKind.GREATER_OR_LESS -> {
                return if (currentPath.lowerBound == null && currentPath.upperBound == null) {
                    this
                } else if (currentPath.lowerBound != null && currentPath.upperBound != null) {
                    this.updateByPathForRange(
                        currentPath.key,
                        currentPath.lowerBound,
                        currentPath.upperBound,
                        updater
                    )
                } else if (currentPath.lowerBound != null) {
                    // currentPath.upperBound == null
                    this.updateByPathForGreaterThan(currentPath.key, currentPath.lowerBound, updater)
                } else {
                    // currentPath.lowerBound == null, currentPath.upperBound != null
                    this.updateByPathForLessThan(currentPath.key, currentPath.upperBound!!, updater)
                }
            }
            IntermediateQueryTermKind.STARTS_WITH -> {
                return if (currentPath.lowerBound == null) {
                    this
                } else {
                    this.updateByPathForStartsWith(currentPath.key, currentPath.lowerBound, updater)
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
                val subNode = this.equalityTerms?.get(currentPath.key)?.get(currentPath.lowerBound) ?: return null
                return subNode.getByPath(queryPath.rest())
            }
            IntermediateQueryTermKind.GREATER_OR_LESS -> {
                return if (currentPath.lowerBound == null && currentPath.upperBound == null) {
                    null
                } else if (currentPath.lowerBound != null && currentPath.upperBound != null) {
                    val lowerBoundComparable = ByteArrayButComparable(currentPath.lowerBound)
                    val upperBoundComparable = ByteArrayButComparable(currentPath.upperBound)
                    val targetRange = Pair(lowerBoundComparable, upperBoundComparable)
                    this.rangeTerms?.get(currentPath.key)?.lookupExactRange(targetRange)
                } else if (currentPath.lowerBound != null) {
                    // currentPath.upperBound == null
                    this.greaterThanTerms?.get(currentPath.key)?.get(currentPath.lowerBound)
                } else {
                    // currentPath.lowerBound == null, currentPath.upperBound != null
                    this.lessThanTerms?.get(currentPath.key)?.get(currentPath.upperBound!!)
                }
            }
            IntermediateQueryTermKind.STARTS_WITH -> {
                if (currentPath.lowerBound == null) {
                    return null
                }
                return this.startsWithTerms?.get(currentPath.key)?.get(currentPath.lowerBound)
            }
        }
    }

    private fun updateForBestKeyValuePair(
        bestKeyValue: QPTrieKeyValue<ByteArray>,
        updater: (prior: V?) -> V?,
        querySpec: QuerySpec,
    ): Pair<QueryPath, QueryTreeNode<V>>? {
        val (bestKey, bestValue) = bestKeyValue
        val target = this.equalityTerms?.get(bestKey)?.get(bestValue) ?: emptyInstance()
        val queryTerm = IntermediateQueryTerm.equalityTerm(bestKey, bestValue)
        val replacementSpecs = target.updateByQuery(querySpec.withoutEqualityTerm(bestKey), updater)
        val sizeDiff = (replacementSpecs?.second?.size ?: 0) - target.size
        val newEqualityTerms = (this.equalityTerms ?: QPTrie()).update(bestKey) {
            val newByValue = (it ?: QPTrie()).update(bestValue) { replacementSpecs?.second }
            if (newByValue.size == 0L) {
                null
            } else {
                newByValue
            }
        }
        val selfReplacement = if (newEqualityTerms === this.equalityTerms) {
            this
        } else {
            this.replaceEqualityTerms(
                if (newEqualityTerms.size == 0L) {
                    null
                } else {
                    newEqualityTerms
                },
                sizeDiff
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
                return Pair(QueryPath(), if (updated === this.value) { this } else { this.replaceValue(updated) })
            }
            val resultPath = QueryPath(ListNode(inequalityTerm))
            when (inequalityTerm.kind) {
                IntermediateQueryTermKind.EQUALS -> {
                    // This should not have happened.
                    return Pair(resultPath, this)
                }
                IntermediateQueryTermKind.STARTS_WITH -> {
                    return if (inequalityTerm.lowerBound == null) {
                        Pair(QueryPath(), this)
                    } else {
                        val newNode = this.updateByPathForStartsWith(
                            inequalityTerm.key,
                            inequalityTerm.lowerBound,
                            updater
                        ) ?: return null
                        Pair(resultPath, newNode)
                    }
                }
                IntermediateQueryTermKind.GREATER_OR_LESS -> {
                    val lowerBound = inequalityTerm.lowerBound
                    val upperBound = inequalityTerm.upperBound
                    val key = inequalityTerm.key
                    val newNode = (if (lowerBound == null && upperBound == null) {
                        this
                    } else if (lowerBound != null && upperBound != null) {
                        this.updateByPathForRange(key, lowerBound, upperBound, updater)
                    } else if (lowerBound != null) {
                        this.updateByPathForGreaterThan(key, lowerBound, updater)
                    } else {
                        this.updateByPathForLessThan(key, upperBound!!, updater)
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
            if (this.equalityTerms == null) {
                break
            }
            val valuePairs = this.equalityTerms.get(kvp.key) ?: continue
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
            FullTreeIterator(this.root)
        }
    }
}

internal enum class GetByDataIteratorState {
    VALUE,
    EQUALITY,
    LESS_THAN,
    RANGE,
    GREATER_THAN,
    STARTS_WITH,
    DONE
}

internal class GetByDataIterator<V: SizeComputable> private constructor(
    private val node: QueryTreeNode<V>,
    private val reversePath: ListNode<IntermediateQueryTerm>?,
    private val fullData: QPTrie<ByteArray>,
    private var state: GetByDataIteratorState,
    private var currentData: QPTrie<ByteArray>,
): ConcatenatedIterator<QueryPathValue<V>>() {
    private constructor(
        node: QueryTreeNode<V>,
        fullData: QPTrie<ByteArray>,
        reversePath: ListNode<IntermediateQueryTerm>?
    ): this(node, reversePath, fullData, GetByDataIteratorState.VALUE, fullData)

    constructor(
        node: QueryTreeNode<V>,
        fullData: QPTrie<ByteArray>
    ): this(node, fullData, null)

    private fun <T>workingDataForKeys(keyTargets: QPTrie<T>?): QPTrie<ByteArray> {
        return workingDataForAvailableKeys(this.fullData, keyTargets)
    }

    private fun pathForState(
        state: GetByDataIteratorState,
        key: ByteArray,
        value: ByteArray,
        result: V
    ): QueryPathValue<V> {
        val term = when (state) {
            GetByDataIteratorState.LESS_THAN -> IntermediateQueryTerm.greaterOrLessTerm(key, null, value)
            GetByDataIteratorState.GREATER_THAN -> IntermediateQueryTerm.greaterOrLessTerm(key, value, null)
            GetByDataIteratorState.STARTS_WITH -> IntermediateQueryTerm.startsWithTerm(key, value)
            else -> IntermediateQueryTerm.startsWithTerm(key, value)
        }
        val pathList = listPrepend(term, this.reversePath)
        return QueryPathValue(QueryPath(listReverse(pathList)), result)
    }

    private fun pathForRange(
        key: ByteArray,
        value: IntervalRange<ByteArrayButComparable>,
        result: V
    ): QueryPathValue<V> {
        val (lowerBound, upperBound) = value
        val term = IntermediateQueryTerm.greaterOrLessTerm(key, lowerBound.array, upperBound.array)
        val pathList = listPrepend(term, this.reversePath)
        return QueryPathValue(QueryPath(listReverse(pathList)), result)
    }

    override fun iteratorForOffset(offset: Int): Iterator<QueryPathValue<V>>? {
        if (this.state != GetByDataIteratorState.VALUE && this.fullData.size == 0L) {
            return null
        }
        while (this.state != GetByDataIteratorState.DONE) {
            when (this.state) {
                GetByDataIteratorState.VALUE -> {
                    this.state = GetByDataIteratorState.EQUALITY
                    this.currentData = this.workingDataForKeys(this.node.equalityTerms)
                    if (this.node.value != null) {
                        return SingleElementIterator(
                            QueryPathValue(QueryPath(listReverse(this.reversePath)), this.node.value)
                        )
                    }
                }

                GetByDataIteratorState.EQUALITY -> {
                    if (currentData.size == 0L) {
                        this.state = GetByDataIteratorState.LESS_THAN
                        this.currentData = this.workingDataForKeys(this.node.lessThanTerms)
                    } else {
                        val (key, value) = this.currentData.minKeyValueUnsafeSharedKey()!!
                        this.currentData = this.currentData.remove(key)
                        val subNode = this.node.equalityTerms?.get(key)?.get(value) ?: continue
                        val newReversePath = listPrepend(
                            IntermediateQueryTerm.equalityTerm(key, value),
                            this.reversePath
                        )
                        return this.registerChild(GetByDataIterator(subNode, this.fullData.remove(key), newReversePath))
                    }
                }

                GetByDataIteratorState.LESS_THAN -> {
                    if (currentData.size == 0L) {
                        this.state = GetByDataIteratorState.RANGE
                        this.currentData = this.workingDataForKeys(this.node.rangeTerms)
                    } else {
                        val (key, value) = this.currentData.minKeyValueUnsafeSharedKey()!!
                        this.currentData = this.currentData.remove(key)
                        val maybe = this.node.lessThanTerms?.get(
                            key
                        )?.iteratorGreaterThanOrEqualUnsafeSharedKey(
                            value
                        )
                        if (maybe != null) {
                            return mapSequence(maybe) { (predicateValue, result) ->
                                this.pathForState(GetByDataIteratorState.LESS_THAN, key, predicateValue, result)
                            }
                        }
                    }
                }

                GetByDataIteratorState.RANGE -> {
                    if (currentData.size == 0L) {
                        this.state = GetByDataIteratorState.GREATER_THAN
                        this.currentData = this.workingDataForKeys(this.node.greaterThanTerms)
                    } else {
                        val (key, value) = this.currentData.minKeyValueUnsafeSharedKey()!!
                        this.currentData = this.currentData.remove(key)
                        val maybe = this.node.rangeTerms?.get(key)?.lookupPoint(
                            ByteArrayButComparable(value)
                        )
                        if (maybe != null) {
                            return mapSequence(maybe) { (predicateValue, result) ->
                                this.pathForRange(key, predicateValue, result)
                            }
                        }
                    }
                }

                GetByDataIteratorState.GREATER_THAN -> {
                    if (currentData.size == 0L) {
                        this.state = GetByDataIteratorState.STARTS_WITH
                        this.currentData = this.workingDataForKeys(this.node.startsWithTerms)
                    } else {
                        val (key, value) = this.currentData.minKeyValueUnsafeSharedKey()!!
                        this.currentData = this.currentData.remove(key)
                        val maybe = this.node.greaterThanTerms?.get(
                            key
                        )?.iteratorLessThanOrEqualUnsafeSharedKey(
                            value
                        )
                        if (maybe != null) {
                            return mapSequence(maybe) { (predicateValue, result) ->
                                this.pathForState(GetByDataIteratorState.GREATER_THAN, key, predicateValue, result)
                            }
                        }
                    }
                }

                GetByDataIteratorState.STARTS_WITH -> {
                    if (currentData.size == 0L) {
                        this.state = GetByDataIteratorState.DONE
                    } else {
                        val (key, value) = this.currentData.minKeyValueUnsafeSharedKey()!!
                        this.currentData = this.currentData.remove(key)
                        val maybe = this.node.startsWithTerms?.get(
                            key
                        )?.iteratorPrefixOfOrEqualToUnsafeSharedKey(
                            value
                        )
                        if (maybe != null) {
                            return mapSequence(maybe) { (predicateValue, result) ->
                                this.pathForState(GetByDataIteratorState.STARTS_WITH, key, predicateValue, result)
                            }
                        }
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

internal class FullTreeIterator<V: SizeComputable> private constructor(
    private val node: QueryTreeNode<V>,
    private val reversePath: ListNode<IntermediateQueryTerm>?,
    private var state: GetByDataIteratorState
): ConcatenatedIterator<QueryPathValue<V>>() {
    constructor (node: QueryTreeNode<V>): this(node, null, GetByDataIteratorState.VALUE)

    override fun iteratorForOffset(offset: Int): Iterator<QueryPathValue<V>>? {
        while (this.state != GetByDataIteratorState.DONE) {
            when (this.state) {
                GetByDataIteratorState.VALUE -> {
                    this.state = GetByDataIteratorState.EQUALITY
                    if (this.node.value != null) {
                        return SingleElementIterator(
                            QueryPathValue(QueryPath(listReverse(this.reversePath)), this.node.value)
                        )
                    }
                }

                GetByDataIteratorState.EQUALITY -> {
                    this.state = GetByDataIteratorState.LESS_THAN
                    if (this.node.equalityTerms != null) {
                        return FlattenIterator(mapSequence(this.node.equalityTerms) { (key, values) ->
                            FlattenIterator(mapSequence(values) { (value, child) ->
                                val term = IntermediateQueryTerm.equalityTerm(key, value)
                                this.registerChild(
                                    FullTreeIterator(
                                        child,
                                        listPrepend(term, this.reversePath),
                                        GetByDataIteratorState.VALUE
                                    )
                                )
                            })
                        })
                    }
                }

                GetByDataIteratorState.LESS_THAN -> {
                    this.state = GetByDataIteratorState.RANGE
                    if (this.node.lessThanTerms != null) {
                        return FlattenIterator(mapSequence(this.node.lessThanTerms) { (key, values) ->
                            mapSequence(values) { (value, result) ->
                                val term = IntermediateQueryTerm.greaterOrLessTerm(key, null, value)
                                QueryPathValue(
                                    QueryPath(listReverse(listPrepend(term, this.reversePath))),
                                    result
                                )
                            }
                        })
                    }
                }

                GetByDataIteratorState.RANGE -> {
                    this.state = GetByDataIteratorState.GREATER_THAN
                    if (this.node.rangeTerms != null) {
                        return FlattenIterator(mapSequence(this.node.rangeTerms) { (key, values) ->
                            mapSequence(values) { (bounds, result, _) ->
                                val (lowerBound, upperBound) = bounds
                                val term = IntermediateQueryTerm.greaterOrLessTerm(
                                    key,
                                    lowerBound.array,
                                    upperBound.array
                                )
                                QueryPathValue(
                                    QueryPath(listReverse(listPrepend(term, this.reversePath))),
                                    result
                                )
                            }
                        })
                    }
                }

                GetByDataIteratorState.GREATER_THAN -> {
                    this.state = GetByDataIteratorState.STARTS_WITH
                    if (this.node.greaterThanTerms != null) {
                        return FlattenIterator(mapSequence(this.node.greaterThanTerms) { (key, values) ->
                            mapSequence(values) { (value, result) ->
                                val term = IntermediateQueryTerm.greaterOrLessTerm(key, value, null)
                                QueryPathValue(
                                    QueryPath(listReverse(listPrepend(term, this.reversePath))),
                                    result
                                )
                            }
                        })
                    }
                }

                GetByDataIteratorState.STARTS_WITH -> {
                    this.state = GetByDataIteratorState.DONE
                    if (this.node.startsWithTerms != null) {
                        return FlattenIterator(mapSequence(this.node.startsWithTerms) { (key, values) ->
                            mapSequence(values) { (value, result) ->
                                val term = IntermediateQueryTerm.startsWithTerm(key, value)
                                QueryPathValue(
                                    QueryPath(listReverse(listPrepend(term, this.reversePath))),
                                    result
                                )
                            }
                        })
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