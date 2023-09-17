package name.djsweet.query.tree

import java.util.*

/*
 * QueryTree: A Reactive Reverse-Index for Simplified Data Queries.
 *
 * Traditional databases index persistent data to accelerate the evaluation of queries. QueryTree indexes persistent
 * queries to accelerate their evaluation with transient, streaming data.
 *
 * Data is defined to be a [QPTrie<ByteArray>] from ByteArray keys to ByteArray values. A query over this data can
 * include zero or more "equality terms" of key=value, and up to one "inequality term" of the form:
 * - Less than or equal to
 * - Between in an inclusive closed interval
 * - Greater than or equal to
 * - Starts with or equal to
 *
 * See the definition of [QuerySpec] for more details regarding the specification of queries.
 *
 * Consider that this query encoding logically takes the form
 *
 *   (k1=v1) ^ (k2=v2) ^ (k3=v3) ^ ... ^ (kN-1=vN-1) ^ (kN?vN), where ? is not =
 *
 * If we do not immediately consider the ? term, we can see that every = term needs to be matched for the query
 * to succeed. This means we can index the terms such that we can disregard all queries where any constituent term
 * does not evaluate to `true`. The order of equality terms additionally does not matter, so we are free to reorder
 * these terms in the construction of the index.
 *
 * Attempting to index every possible permutation of the terms would require N! space, which quickly becomes
 * untenable. However, because we can reorder all terms without altering the semantics of the query, and because
 * every term needs to evaluate to `true` in order to service the query, we can choose an arbitrary ordering of
 * the equality terms, establish a singular "path" for these terms in the tree, and add the query exactly once.
 *
 * We decide where in the tree to anchor this "path" with the following heuristics, starting with the root node:
 * - Any terms previously used to decide the "path" prefix are ignored.
 * - If there are no available keys corresponding to either equality or inequality terms, we save the query as the value
 *   of the current node in the tree.
 * - If there are no available keys corresponding to equality terms, but there is an available key corresponding to an
 *   inequality term, we save the query in the corresponding map for the kind of inequality term.
 * - If none of the available keys corresponding to the equality terms are present in this node, we create a new child
 *   node with the lexicographically lowest equality key, store this child node as a child of the current node, and
 *   recurse into this new node, removing the chosen key as an "available key".
 * - Otherwise, we choose the available equality key that corresponds to the child node with the lowest cardinality,
 *   and recurse into this child node, removing this equality key as an "available key".
 */

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
        return if (this === other) {
            true
        } else if (other !is IntermediateQueryTerm) {
            super.equals(other)
        } else {
            if (other.kind != this.kind) {
                false
            } else if (Arrays.compareUnsigned(other.key, this.key) != 0) {
                false
            } else if (!this.lowerBoundEquals(other)) {
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

/**
 * Describes a query to be indexed in a [QueryTree].
 *
 * Queries indexed by QueryTree can contain zero or more "equality" terms, and zero or one of the following
 * kinds of terms:
 * - Less than or equal to the given value
 * - Between the given closed-interval range
 * - Greater than or equal to the given value
 * - Starting with or equal to the given value
 *
 * However, a `key` can only be present once in a QuerySpec. Mutating methods that refer to a key already used
 * by a contained term will remove the prior term.
 */
class QuerySpec private constructor(
    internal val equalityTerms: QPTrie<ByteArray>,
    internal val inequalityTerm: IntermediateQueryTerm?
) {
    companion object {
        /**
         * A [QuerySpec] with no terms.
         *
         * [QuerySpec.matchesData] will return `true` for any data passed to this instance.
         */
        val empty = QuerySpec(QPTrie(), null)

        /**
         * Returns a [QuerySpec] expecting the data at the given [key] to be equal to the given [value].
         */
        fun withEqualityTerm(key: ByteArray, value: ByteArray): QuerySpec {
            return this.empty.withEqualityTerm(key, value)
        }

        /**
         * Returns a [QuerySpec] expecting the data at the given [key] to be less than or equal to the given
         * [lessThanOrEqualTo].
         */
        fun withLessThanOrEqualToTerm(key: ByteArray, lessThanOrEqualTo: ByteArray): QuerySpec {
            return this.empty.withLessThanOrEqualToTerm(key, lessThanOrEqualTo)
        }

        /**
         * Returns a [QuerySpec] expecting the data at the given [key] to be between the bounds established
         * by [maybeLowerBound] and [maybeUpperBound], including equal to `maybeLowerBound` or `maybeUpperBound`.
         *
         * If `maybeLowerBound` > `maybeUpperBound`, the resulting term considers the range between `maybeUpperBound`
         * and `maybeLowerBound` instead, such that the expected lower bound is always less than or equal to the
         * expected upper bound.
         */
        fun withBetweenOrEqualToTerm(
            key: ByteArray,
            maybeLowerBound: ByteArray,
            maybeUpperBound: ByteArray
        ): QuerySpec {
            return this.empty.withBetweenOrEqualToTerm(key, maybeLowerBound, maybeUpperBound)
        }

        /**
         * Returns a [QuerySpec] expecting the data at the given [key] to be greater than or equal to the given
         * [greaterThanOrEqualTo].
         */
        fun withGreaterThanOrEqualToTerm(key: ByteArray, greaterThanOrEqualTo: ByteArray): QuerySpec {
            return this.empty.withGreaterThanOrEqualToTerm(key, greaterThanOrEqualTo)
        }

        /**
         * Returns a [QuerySpec] expecting the data at the given [key] to start with or equal the given
         * [startsWith].
         */
        fun withStartsWithTerm(key: ByteArray, startsWith: ByteArray): QuerySpec {
            return this.empty.withStartsWithTerm(key, startsWith)
        }
    }

    /**
     * Returns the count of the number of terms encoded in this [QuerySpec].
     */
    val cardinality: Int
        get() = this.equalityTerms.size.toInt() + if (this.inequalityTerm == null) { 0 } else { 1 }

    /**
     * Returns a new [QuerySpec] containing the terms of this QuerySpec, but with an equality term expecting
     * the value at [key] to equal [value].
     *
     * If this QuerySpec already contains an equality term corresponding to `key`, this term is replaced with
     * the new term in the new QuerySpec. If this QuerySpec contains an inequality term corresponding to `key`, it is
     * removed in the new QuerySpec.
     */
    fun withEqualityTerm(key: ByteArray, value: ByteArray): QuerySpec {
        var nextInequalityTerm = this.inequalityTerm
        if (nextInequalityTerm != null && nextInequalityTerm.key.contentEquals(key)) {
            nextInequalityTerm = null
        }
        return QuerySpec(this.equalityTerms.put(key, value), nextInequalityTerm)
    }

    /**
     * Returns whether this [QuerySpec] has an equality term for the given [key].
     */
    fun hasEqualityTerm(key: ByteArray): Boolean {
        return this.equalityTerms.get(key) != null
    }

    // Used to construct entries in the QueryTree
    internal fun withoutEqualityTerm(key: ByteArray): QuerySpec {
        val replacement = this.equalityTerms.remove(key)
        return if (replacement === this.equalityTerms) {
            this
        } else {
            QuerySpec(replacement, this.inequalityTerm)
        }
    }

    /**
     * Returns a new [QuerySpec] containing the terms of this QuerySpec, but with an inequality term expecting
     * the value at [key] to be less than or equal to [lessThanOrEqualTo].
     *
     * If this QuerySpec already contains an equality term corresponding to `key`, this term is removed in the new
     * QuerySpec. If this QuerySpec already contains an inequality term, it is replaced with the new inequality term
     * in the new QuerySpec.
     */
    fun withLessThanOrEqualToTerm(key: ByteArray, lessThanOrEqualTo: ByteArray): QuerySpec {
        val term = IntermediateQueryTerm.greaterOrLessTerm(key.copyOf(), null, lessThanOrEqualTo)
        return QuerySpec(this.equalityTerms.remove(key), term)
    }

    /**
     * Returns a new [QuerySpec] containing the terms of this QuerySpec, but with an inequality term expecting the
     * value at [key] to be between the interval bounded by [maybeLowerBound] and [maybeUpperBound], closed at both
     * points.
     *
     * If `maybeLowerBound` > `maybeUpperBound`, the interval is considered to be bounded by `maybeUpperBound` to
     * `maybeLowerBound` instead.
     *
     * If this QuerySpec already contains an equality term corresponding to `key`, this term is removed in the new
     * QuerySpec. If this QuerySpec already contains an inequality term, it is replaced with the new inequality term
     * in the new QuerySpec.
     */
    fun withBetweenOrEqualToTerm(key: ByteArray, maybeLowerBound: ByteArray, maybeUpperBound: ByteArray): QuerySpec {
        val swapBounds = Arrays.compareUnsigned(maybeLowerBound, maybeUpperBound) == 1
        val lowerBound = if (swapBounds) { maybeUpperBound } else { maybeLowerBound }
        val upperBound = if (swapBounds) { maybeLowerBound } else { maybeUpperBound }
        val term = IntermediateQueryTerm.greaterOrLessTerm(key.copyOf(), lowerBound, upperBound)
        return QuerySpec(this.equalityTerms.remove(key), term)
    }

    /**
     * Returns a new [QuerySpec] containing the terms of this QuerySpec, but with an inequality term expecting
     * the value at [key] to be greater than or equal to [greaterThanOrEqualTo].
     *
     * If this QuerySpec already contains an equality term corresponding to `key`, this term is removed in the new
     * QuerySpec. If this QuerySpec already contains an inequality term, it is replaced with the new inequality term
     * in the new QuerySpec.
     */
    fun withGreaterThanOrEqualToTerm(key: ByteArray, greaterThanOrEqualTo: ByteArray): QuerySpec {
        val term = IntermediateQueryTerm.greaterOrLessTerm(key.copyOf(), greaterThanOrEqualTo, null)
        return QuerySpec(this.equalityTerms.remove(key), term)
    }

    /**
     * Returns a new [QuerySpec] containing the terms of this QuerySpec, but with an inequality term expecting
     * the value at [key] to start with or be equal to [startsWith].
     *
     * If this QuerySpec already contains an equality term corresponding to `key`, this term is removed in the new
     * QuerySpec. If this QuerySpec already contains an inequality term, it is replaced with the new inequality term
     * in the new QuerySpec.
     */
    fun withStartsWithTerm(key: ByteArray, startsWith: ByteArray): QuerySpec {
        val term = IntermediateQueryTerm.startsWithTerm(key.copyOf(), startsWith)
        return QuerySpec(this.equalityTerms.remove(key), term)
    }

    /**
     * Returns `true` if the given [data] satisfies all terms of this [QuerySpec], or `false` otherwise.
     */
    fun matchesData(data: QPTrie<ByteArray>): Boolean {
        for ((key, value) in this.equalityTerms) {
            val dataAtKey = data.get(key) ?: return false
            if (!value.contentEquals(dataAtKey)) {
                return false
            }
        }
        val inequalityTerm = this.inequalityTerm ?: return true
        val dataAtInequality = data.get(inequalityTerm.key) ?: return false
        return if (inequalityTerm.kind == IntermediateQueryTermKind.STARTS_WITH) {
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

        } else {
            // inequalityTerm.kind == IntermediateQueryTermKind.GREATER_OR_LESS
            if (inequalityTerm.lowerBound != null && inequalityTerm.upperBound != null) {
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
        } else if (other is QuerySpec) {
            if (this.inequalityTerm != other.inequalityTerm) {
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

/**
 * Represents an opaque evaluation path for which a [QueryTree] stores a value.
 *
 * [QueryTree.updateByQuery] will update values in a non-deterministic fashion from the perspective of the caller.
 * It returns a [QueryPath] instance that can be used to address this value using [QueryTree.getByPath] and
 * [QueryTree.updateByPath].
 */
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
        } else if (other is QueryPath) {
            listEquals(this.breadcrumbs, other.breadcrumbs)
        } else {
            super.equals(other)
        }
    }

    override fun hashCode(): Int {
        return listHashCode(breadcrumbs)
    }

    override fun toString(): String {
        return "QueryPath(${this.breadcrumbs})"
    }

    fun keys(): Iterator<ByteArray> {
        return listIterator(this.breadcrumbs).asSequence().map { it.key.copyOf() }.iterator()
    }
}

/**
 * Exposes per-value cardinality, for use with the query indexing heuristic used by [QueryTree].
 */
interface SizeComputable {
    fun computeSize(): Long
}

/**
 * Represents a key/value pair held by a [QueryTree], corresponding to an indexed query.
 */
data class QueryPathValue<V> internal constructor (val path: QueryPath, val value: V)

private class QueryTreeTerms<V: SizeComputable>(
    val equalityTerms: QPTrie<QueryTreeNode<V>>?,
    val lessThanTerms: QPTrie<QueryPathValue<V>>?,
    val rangeTerms: IntervalTree<ByteArrayButComparable, QueryPathValue<V>>?,
    val greaterThanTerms: QPTrie<QueryPathValue<V>>?,
    val startsWithTerms: QPTrie<QueryPathValue<V>>?,
) {
     constructor(): this(
        null,
        null,
        null,
        null,
        null
    )

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

    fun replaceLessThanTerms(newTerms: QPTrie<QueryPathValue<V>>?): QueryTreeTerms<V>? {
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

    fun replaceRangeTerms(newTerms: IntervalTree<ByteArrayButComparable, QueryPathValue<V>>?): QueryTreeTerms<V>? {
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

    fun replaceGreaterThanTerms(newTerms: QPTrie<QueryPathValue<V>>?): QueryTreeTerms<V>? {
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

    fun replaceStartsWithTerms(newTerms: QPTrie<QueryPathValue<V>>?): QueryTreeTerms<V>? {
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

private class QueryTreeNode<V  : SizeComputable> private constructor(
    val path: QueryPath,
    val value: V?,
    val keys: QPTrie<QueryTreeTerms<V>>,
    val size: Long
) {
    companion object {
        fun <V: SizeComputable>emptyInstance(path: QueryPath): QueryTreeNode<V> {
            return QueryTreeNode(
                path,
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
            QueryTreeNode(this.path, value, this.keys, newSize)
        }
    }

    private fun replaceKeys(newKeys: QPTrie<QueryTreeTerms<V>>, sizeDiff: Long): QueryTreeNode<V>? {
        return if (newKeys === this.keys) {
            this
        } else if (this.value == null && newKeys.size == 0L) {
            null
        } else {
            val newSize = this.size + sizeDiff
            QueryTreeNode(this.path, this.value, newKeys, newSize)
        }
    }

    private fun updateTrieTerms(
        keys: QPTrie<QueryTreeTerms<V>>,
        target: QPTrie<QueryPathValue<V>>?,
        key: ByteArray,
        value: ByteArray,
        knownPath: QueryPath,
        updater: (prior: V?) -> V?,
        replacer: (terms: QPTrie<QueryPathValue<V>>) -> QueryTreeTerms<V>?
    ): QueryTreeNode<V>? {
        val existingResultByValue = target?.get(value)
        val existingResultValueByValue = existingResultByValue?.value
        val newResult = updater(existingResultValueByValue)
        if (newResult === existingResultValueByValue) {
            return this
        }

        val sizeDiff = (newResult?.computeSize() ?: 0) - (existingResultValueByValue?.computeSize() ?: 0)
        val newTrieTerms = (target ?: QPTrie()).update(value) {
            if (newResult == null) {
                null
            } else {
                QueryPathValue(knownPath, newResult)
            }
        }
        val newTerms = replacer(newTrieTerms)
        val newKeys = keys.update(key) { newTerms }
        return this.replaceKeys(newKeys, sizeDiff)
    }

    private fun updateRangeTerms(
        keys: QPTrie<QueryTreeTerms<V>>,
        termsByKey: QueryTreeTerms<V>,
        key: ByteArray,
        lowerBound: ByteArray,
        upperBound: ByteArray,
        knownPath: QueryPath,
        updater: (prior: V?) -> V?
    ): QueryTreeNode<V>? {
        val rangeTerms = termsByKey.rangeTerms
        val lowerBoundComparable = ByteArrayButComparable(lowerBound)
        val upperBoundComparable = ByteArrayButComparable(upperBound)
        val rangeValue = IntervalRange.fromBounds(lowerBoundComparable, upperBoundComparable)
        val existingResultByValue = rangeTerms?.lookupExactRange(rangeValue)
        val existingResultValueByValue = existingResultByValue?.value
        val newResult = updater(existingResultValueByValue)
        if (newResult === existingResultValueByValue) {
            return this
        }
        val sizeDiff = (newResult?.computeSize() ?: 0) - (existingResultValueByValue?.computeSize() ?: 0)
        val newRangeTerms = (rangeTerms ?: IntervalTree()).update(rangeValue) {
            if (newResult == null) {
                null
            } else {
                QueryPathValue(knownPath, newResult)
            }
        }
        val newTerms = termsByKey.replaceRangeTerms(newRangeTerms)
        val newKeys = keys.update(key) { newTerms }
        return this.replaceKeys(newKeys, sizeDiff)
    }

    fun updateByPath(
        remainingPath: QueryPath,
        priorPath: ListNode<IntermediateQueryTerm>?,
        fullPath: QueryPath,
        updater: (prior: V?) -> V?
    ): QueryTreeNode<V>? {
        val currentPathElement = remainingPath.first() ?: return this.replaceValue(updater(this.value))
        val currentPath = listPrepend(currentPathElement, priorPath)
        val keys = this.keys
        val currentKey = currentPathElement.key
        val termsByKey = this.keys.get(currentKey) ?: QueryTreeTerms()
        when (currentPathElement.kind) {
            IntermediateQueryTermKind.EQUALS -> {
                val value = currentPathElement.lowerBound!!
                val equalityTerms = termsByKey.equalityTerms ?: QPTrie()
                val existingNodeByValue = equalityTerms.get(value)

                // This is an entirely new value, so we'll be strictly adding to the key set.
                if (existingNodeByValue == null) {
                    val updateValue = updater(null) ?: return this
                    val subNode = (emptyInstance<V>(
                        QueryPath(listReverse(currentPath))
                    ).updateByPath(remainingPath.rest(), currentPath, fullPath) {
                        updateValue
                    })!!
                    val newEqualityTerms = equalityTerms.update(value) { subNode }
                    val newTerms = termsByKey.replaceEqualityTerms(newEqualityTerms)
                    val newKeys = keys.update(currentKey) { newTerms }
                    return this.replaceKeys(newKeys, updateValue.computeSize())
                }

                val newNodeByValue = existingNodeByValue.updateByPath(
                    remainingPath.rest(),
                    currentPath,
                    fullPath,
                    updater
                )
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
                val lowerBound = currentPathElement.lowerBound
                val upperBound = currentPathElement.upperBound
                if (lowerBound == null) {
                    // upperBound != null
                    return this.updateTrieTerms(
                        keys,
                        termsByKey.lessThanTerms,
                        currentKey,
                        upperBound!!,
                        fullPath,
                        updater,
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
                        fullPath,
                        updater,
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
                    fullPath,
                    updater,
                )
            }
            IntermediateQueryTermKind.STARTS_WITH -> {
                val value = currentPathElement.lowerBound ?: return this
                return this.updateTrieTerms(
                    keys,
                    termsByKey.startsWithTerms,
                    currentKey,
                    value,
                    fullPath,
                    updater
                ) {
                    termsByKey.replaceStartsWithTerms(it)
                }
            }
        }
    }

    fun getByPath(queryPath: QueryPath): V? {
        val currentPath = queryPath.first() ?: return this.value
        return when (currentPath.kind) {
            IntermediateQueryTermKind.EQUALS -> {
                val subNode = this.keys.get(
                    currentPath.key
                )?.equalityTerms?.get(
                    currentPath.lowerBound!!
                ) ?: return null
                subNode.getByPath(queryPath.rest())
            }
            IntermediateQueryTermKind.GREATER_OR_LESS -> {
                if (currentPath.lowerBound != null && currentPath.upperBound != null) {
                    val lowerBoundComparable = ByteArrayButComparable(currentPath.lowerBound)
                    val upperBoundComparable = ByteArrayButComparable(currentPath.upperBound)
                    val targetRange = IntervalRange.fromBounds(lowerBoundComparable, upperBoundComparable)
                    this.keys.get(currentPath.key)?.rangeTerms?.lookupExactRange(targetRange)?.value
                } else if (currentPath.lowerBound != null) {
                    // currentPath.upperBound == null
                    this.keys.get(currentPath.key)?.greaterThanTerms?.get(currentPath.lowerBound)?.value
                } else {
                    // currentPath.lowerBound == null, currentPath.upperBound != null
                    this.keys.get(currentPath.key)?.lessThanTerms?.get(currentPath.upperBound!!)?.value
                }
            }
            IntermediateQueryTermKind.STARTS_WITH -> {
                this.keys.get(currentPath.key)?.startsWithTerms?.get(currentPath.lowerBound!!)?.value
            }
        }
    }

    private fun updateForBestKeyValuePair(
        bestKeyValue: QPTrieKeyValue<ByteArray>,
        querySpec: QuerySpec,
        reversePath: ListNode<IntermediateQueryTerm>?,
        updater: (prior: V?) -> V?,
    ): Pair<QueryPath, QueryTreeNode<V>>? {
        val (bestKey, bestValue) = bestKeyValue
        val keys = this.keys
        val queryTerm = IntermediateQueryTerm.equalityTerm(bestKey, bestValue)
        val newPath = listPrepend(queryTerm, reversePath)
        val target = keys.get(bestKey)?.equalityTerms?.get(bestValue) ?: emptyInstance(QueryPath(listReverse(newPath)))
        val replacementSpecs = target.updateByQuery(
            querySpec.withoutEqualityTerm(bestKey),
            newPath,
            updater
        )
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
                this.path,
                this.value,
                newKeys,
                this.size + sizeDiff
            )
        }
        return if (selfReplacement.size == 0L) {
            null
        } else {
            Pair(replacementSpecs?.first ?: selfReplacement.path, selfReplacement)
        }
    }

    fun updateByQuery(
        querySpec: QuerySpec,
        reversePath: ListNode<IntermediateQueryTerm>?,
        updater: (prior: V?) -> V?
    ): Pair<QueryPath, QueryTreeNode<V>>? {
        val currentTerms = querySpec.equalityTerms
        if (currentTerms.size == 0L) {
            val inequalityTerm = querySpec.inequalityTerm
            // If we don't have any further inequality checks to perform on this value,
            // and we're all out of equality terms, the value is present on this.
            if (inequalityTerm == null) {
                val updated = updater(this.value)
                return if (updated === this.value) {
                    Pair(this.path, this)
                } else {
                    val replacement = this.replaceValue(updated) ?: return null
                    Pair(this.path, replacement)
                }
            }
            val resultPath = QueryPath(listReverse(listPrepend(inequalityTerm, reversePath)))
            if (inequalityTerm.kind == IntermediateQueryTermKind.STARTS_WITH) {
                val key = inequalityTerm.key
                val keys = this.keys
                val termsForKey = keys.get(key) ?: QueryTreeTerms()
                val newNode = this.updateTrieTerms(
                    keys,
                    termsForKey.startsWithTerms,
                    key,
                    inequalityTerm.lowerBound!!,
                    resultPath,
                    updater
                ) {
                    termsForKey.replaceStartsWithTerms(it)
                } ?: return null
                return Pair(resultPath, newNode)
            } else {
                // inequalityTerm.kind == IntermediateQueryTermKind.GREATER_OR_LESS
                val lowerBound = inequalityTerm.lowerBound
                val upperBound = inequalityTerm.upperBound
                val key = inequalityTerm.key
                val keys = this.keys
                val termsForKey = keys.get(key) ?: QueryTreeTerms()
                val newNode = (if (lowerBound != null && upperBound != null) {
                    this.updateRangeTerms(
                        keys,
                        termsForKey,
                        key,
                        lowerBound,
                        upperBound,
                        resultPath,
                        updater
                    )
                } else if (lowerBound != null) {
                    this.updateTrieTerms(
                        keys,
                        termsForKey.greaterThanTerms,
                        key,
                        lowerBound,
                        resultPath,
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
                        resultPath,
                        updater
                    ) {
                        termsForKey.replaceLessThanTerms(it)
                    }
                }) ?: return null
                return Pair(resultPath, newNode)
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
        return this.updateForBestKeyValuePair(updateKeyValue, querySpec, reversePath, updater)
    }
}

/**
 * A persistent, immutable map from [QuerySpec] or [QueryPath] to arbitrary values, with [QuerySpec] being converted
 * into a [QueryPath] using a simplified cardinality-minimizing heuristic.
 *
 * The heuristic for converting a [QuerySpec] into a [QueryPath], as used by [QueryTree.updateByQuery], is:
 * - Any terms previously used to decide the "path" prefix are ignored.
 * - If there are no available keys corresponding to either equality or inequality terms, we save the query as the value
 *   of the current node in the tree.
 * - If there are no available keys corresponding to equality terms, but there is an available key corresponding to an
 *   inequality term, we save the query in the corresponding map for the kind of inequality term.
 * - If none of the available keys corresponding to the equality terms are present in this node, we create a new child
 *   node with the lexicographically lowest equality key, store this child node as a child of the current node, and
 *   recurse into this new node, removing the chosen key as an "available key".
 * - Otherwise, we choose the available equality key that corresponds to the child node with the lowest cardinality,
 *   and recurse into this child node, removing this equality key as an "available key".
 *
 */
class QueryTree<V: SizeComputable> private constructor(
    private val root: QueryTreeNode<V>?
): Iterable<QueryPathValue<V>> {
    data class PathTreeResult<V: SizeComputable> internal constructor(val path: QueryPath, val tree: QueryTree<V>)

    /**
     * Returns the number of key/value pairs present in this [QueryTree].
     *
     * This is a Long instead of the standard Int to support more than ~2 billion members.
     */
    val size: Long = this.root?.size ?: 0

    constructor(): this(null)

    /**
     * Returns the value corresponding to the given [queryPath], or `null` if such a `queryPath` is not present in
     * this [QueryTree].
     */
    fun getByPath(queryPath: QueryPath): V? {
        val root = this.root ?: return null
        return root.getByPath(queryPath)
    }

    /**
     * Returns a new [QueryTree] representing the result of replacing the prior value at [queryPath]
     * with the value computed by [updater].
     *
     * [updater] is passed the current value of this QueryTree at the given `queryPath` as its sole argument. If this
     * QueryTree does not have a value corresponding to the given `queryPath`, this argument is `null`. If `updater`
     * returns `null` instead of a value, this is treated as a request to remove the corresponding value.
     *
     * The resulting QueryTree may be this QueryTree if the value returned by `updater` is identical to the value
     * in this QueryTree corresponding to `updatePath`. This is permissible because the two QueryTrees would otherwise
     * be functionally equivalent.
     */
    fun updateByPath(queryPath: QueryPath, updater: (prior: V?) -> V?): QueryTree<V> {
        val oldRoot = (this.root ?: QueryTreeNode.emptyInstance(QueryPath()))
        val newRoot = oldRoot.updateByPath(queryPath, null, queryPath, updater)
        if (oldRoot === newRoot) {
            return this
        }
        val newSize = newRoot?.size ?: 0
        return QueryTree(
            if (newSize == 0L) { null } else { newRoot }
        )
    }

    /**
     * Returns a new [QueryTree] updated with the same semantics as [QueryTree.updateByPath], converting [querySpec]
     * into a [QueryPath] based on the documented conversion heuristics.
     *
     * Note that if [updater] returns `null`, this is treated as a request to remove the value corresponding to the
     * converted QueryPath in this QueryTree.
     */
    fun updateByQuery(querySpec: QuerySpec, updater: (prior: V?) -> V?): PathTreeResult<V> {
        val oldRoot = (this.root ?: QueryTreeNode.emptyInstance(QueryPath()))
        val (newPath, newRoot) = oldRoot.updateByQuery(querySpec, null, updater) ?: return PathTreeResult(
            QueryPath(),
            QueryTree()
        )
        return if (newRoot === oldRoot) {
            PathTreeResult(newPath, this)
        } else {
            PathTreeResult(newPath, QueryTree(newRoot))
        }
    }

    /**
     * Returns an iterator over all query/value pairs contained in this [QueryTree] whose underlying query matched the
     * given [data] according to the definition of [QuerySpec.matchesData].
     */
    fun getByData(data: QPTrie<ByteArray>): Iterator<QueryPathValue<V>> {
        val root = this.root ?: return EmptyIterator()
        return GetByDataIterator(root, data)
    }

    /**
     * Calls [receiver] for all query/value pairs contained in this [QueryTree] whose underlying query matched the
     * given [data] according to the definition of [QuerySpec.matchesData].
     *
     * This is functionally similar to [QueryTree.getByData], but is significantly faster. However, it is implemented
     * recursively, so care must be taken to ensure that the underlying [QueryPath] is small enough to prevent
     * [StackOverflowError]s.
     */
    fun visitByData(data: QPTrie<ByteArray>, receiver: (result: QueryPathValue<V>) -> Unit) {
        val root = this.root ?: return
        visitTermsByDataImpl(root, data, receiver)
    }

    override fun iterator(): Iterator<QueryPathValue<V>> {
        val root = this.root ?: return EmptyIterator()
        return FullTreeIterator(root)
    }
}

private fun<V: SizeComputable> visitTermsByDataImpl(
    node: QueryTreeNode<V>,
    fullData: QPTrie<ByteArray>,
    receiver: (result: QueryPathValue<V>) -> Unit
) {
    val nodeValue = node.value
    if (nodeValue != null) {
        receiver(QueryPathValue(node.path, nodeValue))
    }
    val nodeKeys = node.keys
    val keys = keysForValueUnion(fullData, nodeKeys)
    val handleTrieResult = { value: QPTrieKeyValue<QueryPathValue<V>> -> receiver(value.value) }
    val handleRangeResult = { value: IntervalTreeKeyValue<ByteArrayButComparable, QueryPathValue<V>> ->
        receiver(value.value)
    }
    for (targetKey in keys) {
        val terms = nodeKeys.get(targetKey) ?: continue
        val fullDataValue = fullData.get(targetKey) ?: continue
        val maybeEquality = terms.equalityTerms?.get(fullDataValue)
        if (maybeEquality != null) {
            visitTermsByDataImpl(maybeEquality, fullData.remove(targetKey), receiver)
        }
        terms.lessThanTerms?.visitGreaterThanOrEqualUnsafeSharedKey(fullDataValue, handleTrieResult)
        terms.rangeTerms?.lookupPointVisit(ByteArrayButComparable(fullDataValue), handleRangeResult)
        terms.greaterThanTerms?.visitLessThanOrEqualUnsafeSharedKey(fullDataValue, handleTrieResult)
        terms.startsWithTerms?.visitPrefixOfOrEqualToUnsafeSharedKey(fullDataValue, handleTrieResult)
    }
}

private enum class GetTermsByDataIteratorState {
    EQUALITY,
    LESS_THAN,
    RANGE,
    GREATER_THAN,
    STARTS_WITH,
    DONE
}

private class GetTermsByDataIterator<V : SizeComputable> private constructor(
    private val value: ByteArray,
    private val terms: QueryTreeTerms<V>,
    private val fullData: QPTrie<ByteArray>,
    private var state: GetTermsByDataIteratorState
): ConcatenatedIterator<QueryPathValue<V>>() {
    constructor(
        value: ByteArray,
        terms: QueryTreeTerms<V>,
        fullData: QPTrie<ByteArray>,
    ): this(value, terms, fullData, GetTermsByDataIteratorState.EQUALITY)

    override fun iteratorForOffset(offset: Int): Iterator<QueryPathValue<V>>? {
        val terms = this.terms
        val value = this.value
        val fullData = this.fullData
        while (true) {
            when (this.state) {
                GetTermsByDataIteratorState.EQUALITY -> {
                    this.state = GetTermsByDataIteratorState.LESS_THAN
                    val subNode = terms.equalityTerms?.get(value) ?: continue
                    return this.registerChild(GetByDataIterator(subNode, fullData))
                }

                GetTermsByDataIteratorState.LESS_THAN -> {
                    this.state = GetTermsByDataIteratorState.RANGE
                    val result = terms.lessThanTerms?.iteratorGreaterThanOrEqualUnsafeSharedKey(value) ?: continue
                    return mapSequence(result) { it.value }
                }

                GetTermsByDataIteratorState.RANGE -> {
                    this.state = GetTermsByDataIteratorState.GREATER_THAN
                    val result = terms.rangeTerms?.lookupPoint(ByteArrayButComparable(value)) ?: continue
                    return mapSequence(result) { it.value }
                }

                GetTermsByDataIteratorState.GREATER_THAN -> {
                    this.state = GetTermsByDataIteratorState.STARTS_WITH
                    val result = terms.greaterThanTerms?.iteratorLessThanOrEqualUnsafeSharedKey(value) ?: continue
                    return mapSequence(result) { it.value }
                }

                GetTermsByDataIteratorState.STARTS_WITH -> {
                    this.state = GetTermsByDataIteratorState.DONE
                    val result = terms.startsWithTerms?.iteratorPrefixOfOrEqualToUnsafeSharedKey(value) ?: continue
                    return mapSequence(result) { it.value }
                }

                GetTermsByDataIteratorState.DONE -> {
                    break
                }
            }
        }
        return null
    }
}

private enum class GetByDataIteratorState {
    VALUE,
    TERMS,
    DONE
}

private class GetByDataIterator<V: SizeComputable> private constructor(
    private val node: QueryTreeNode<V>,
    private val fullData: QPTrie<ByteArray>,
    private var state: GetByDataIteratorState,
): ConcatenatedIterator<QueryPathValue<V>>() {
    private val keys = keysForValueUnion(fullData, node.keys)
    private var keyOffset = 0

    constructor(
        node: QueryTreeNode<V>,
        fullData: QPTrie<ByteArray>,
    ): this(node, fullData, GetByDataIteratorState.VALUE)

    override fun iteratorForOffset(offset: Int): Iterator<QueryPathValue<V>>? {
        if (this.state != GetByDataIteratorState.VALUE && this.fullData.size == 0L) {
            return null
        }
        while (true) {
            val node = this.node
            when (this.state) {
                GetByDataIteratorState.VALUE -> {
                    this.state = GetByDataIteratorState.TERMS
                    val nodeValue = node.value
                    if (nodeValue != null) {
                        return SingleElementIterator(QueryPathValue(node.path, nodeValue))
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
                            value,
                            targetTerms,
                            fullData.remove(targetKey)
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

private class FullTermsByDataIterator<V : SizeComputable> private constructor (
    private val terms: QueryTreeTerms<V>,
    private var state: GetTermsByDataIteratorState
): ConcatenatedIterator<QueryPathValue<V>>() {
    constructor(
        terms: QueryTreeTerms<V>,
    ): this(terms, GetTermsByDataIteratorState.EQUALITY)

    override fun iteratorForOffset(offset: Int): Iterator<QueryPathValue<V>>? {
        while (true) {
            when (this.state) {
                GetTermsByDataIteratorState.EQUALITY -> {
                    this.state = GetTermsByDataIteratorState.LESS_THAN
                    val equalityTerms = this.terms.equalityTerms ?: continue
                    return FlattenIterator(mapSequence(equalityTerms) {
                        this.registerChild(FullTreeIterator(it.value))
                    })
                }

                GetTermsByDataIteratorState.LESS_THAN -> {
                    this.state = GetTermsByDataIteratorState.RANGE
                    val lessThanTerms = this.terms.lessThanTerms ?: continue
                    return mapSequence(lessThanTerms) {
                        it.value
                    }
                }

                GetTermsByDataIteratorState.RANGE -> {
                    this.state = GetTermsByDataIteratorState.GREATER_THAN
                    val rangeTerms = this.terms.rangeTerms ?: continue
                    return mapSequence(rangeTerms) { it.value }
                }

                GetTermsByDataIteratorState.GREATER_THAN -> {
                    this.state = GetTermsByDataIteratorState.STARTS_WITH
                    val greaterThanTerms = this.terms.greaterThanTerms ?: continue
                    return mapSequence(greaterThanTerms) { it.value }
                }

                GetTermsByDataIteratorState.STARTS_WITH -> {
                    this.state = GetTermsByDataIteratorState.DONE
                    val startsWithTerms = this.terms.startsWithTerms ?: continue
                    return mapSequence(startsWithTerms) { it.value }
                }

                GetTermsByDataIteratorState.DONE -> {
                    break
                }
            }
        }
        return null
    }
}

private class FullTreeIterator<V: SizeComputable> private constructor(
    private val node: QueryTreeNode<V>,
    private var state: GetByDataIteratorState
): ConcatenatedIterator<QueryPathValue<V>>() {
    constructor (node: QueryTreeNode<V>): this(node, GetByDataIteratorState.VALUE)

    override fun iteratorForOffset(offset: Int): Iterator<QueryPathValue<V>>? {
        val node = this.node
        while (true) {
            when (this.state) {
                GetByDataIteratorState.VALUE -> {
                    this.state = GetByDataIteratorState.TERMS
                    if (node.value != null) {
                        return SingleElementIterator(QueryPathValue(node.path, node.value))
                    }
                }

                GetByDataIteratorState.TERMS -> {
                    this.state = GetByDataIteratorState.DONE
                    return FlattenIterator(mapSequence(node.keys) {
                        this.registerChild(FullTermsByDataIterator(it.value))
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

/**
 * A persistent, immutable multimap from [QuerySpec] or [QueryPath] to arbitrary values, with [QuerySpec] being
 * converted into a [QueryPath] using a simplified cardinality-minimizing heuristic.
 *
 * The difference between a [QuerySetTree] and a [QueryTree] is that a QuerySetTree saves a set of entries as a
 * value instead of a single entry.
 *
 * The heuristic used by [QuerySetTree] to convert a QuerySpec into a QueryPath is as described for [QueryTree].
 * The only difference is in the interface: [QueryTree.updateByPath] is instead [QuerySetTree.addElementByPath] and
 * [QuerySetTree.removeElementByPath], and [QueryTree.updateByQuery] is instead [QuerySetTree.addElementByQuery].
 */
class QuerySetTree<V> private constructor(
    private val queryTree: QueryTree<SetWithCardinality<V>>,
): Iterable<QueryPathValue<V>> {
    data class PathTreeResult<V> internal constructor(val path: QueryPath, val tree: QuerySetTree<V>)

    /**
     * Returns the number of key/value pairs present in this [QuerySetTree].
     *
     * This is a Long instead of the standard Int to support more than ~2 billion members.
     */
    val size: Long = this.queryTree.size

    constructor(): this(QueryTree())

    /**
     * Returns the values corresponding to the given [queryPath] in this [QuerySetTree].
     */
    fun getByPath(queryPath: QueryPath): Iterator<V> {
        val setResult = this.queryTree.getByPath(queryPath)
        return setResult?.iterator() ?: EmptyIterator()
    }

    /**
     * Returns a new [QuerySetTree], wherein the entry set at [queryPath] in the new tree contains [elem].
     *
     * The new QuerySetTree will actually be this QuerySetTree if the current QuerySetTree already has an entry set
     * corresponding to `queryPath`, and this entry set already `contains` elem. This is permissible because the
     * QuerySetTrees would otherwise be equivalent.
     */
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

    /**
     * Returns a new [QuerySetTree], wherein the entry set at [queryPath] in the new tree does not contain [elem].
     *
     * If the entry set resulting from the removal of `elem` is empty, this entry set is removed from the new
     * QuerySetTree.
     *
     * The new QuerySetTree will actually be this QuerySetTree if either there was already not an entry set
     * corresponding to `queryPath`, or the entry set corresponding to `queryPath` did not contain `elem`. This is
     * permissible because the QuerySetTrees would otherwise be equivalent.
     */
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

    /**
     * Returns a new [QuerySetTree] updated with the same semantics as [QuerySetTree.addElementByPath], converting
     * [querySpec] into a [QueryPath] based on the documented conversion heuristics.
     */
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

    /**
     * Returns an iterator over all query/value pairs contained in this [QuerySetTree] whose underlying query matched
     * the given [data] according to the definition of [QuerySpec.matchesData].
     */
    fun getByData(data: QPTrie<ByteArray>): Iterator<QueryPathValue<V>> {
        return FlattenIterator(mapSequence(this.queryTree.getByData(data)) { (path, valueSet) ->
            mapSequence(valueSet) { QueryPathValue(path, it) }
        })
    }

    /**
     * Calls [receiver] for all query/value pairs contained in this [QuerySetTree] whose underlying query matched the
     * given [data] according to the definition of [QuerySpec.matchesData].
     *
     * Unlike [QuerySetTree.getByData] and [QueryTree.visitByData], `receiver` receives `path` and `value` as separate
     * arguments. This is an unfortunate inconsistency, but is necessary to avoid extra memory allocations.
     *
     * This is functionally similar to [QuerySetTree.getByData], but is significantly faster. However, it is implemented
     * recursively, so care must be taken to ensure that the underlying [QueryPath] is small enough to prevent
     * [StackOverflowError]s.
     */
    fun visitByData(data: QPTrie<ByteArray>, receiver: (path: QueryPath, value: V) -> Unit) {
        this.queryTree.visitByData(data) { queryTreeResult ->
            val (path, value) = queryTreeResult
            value.set.visitAll {
                receiver(path, it)
            }
        }
    }

    override fun iterator(): Iterator<QueryPathValue<V>> {
        return FlattenIterator(mapSequence(this.queryTree) { (path, valueSet) ->
            mapSequence(valueSet) { QueryPathValue(path, it) }
        })
    }
}