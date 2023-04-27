package name.djsweet.query.tree

internal data class ListNode<T>(val value: T, val tail: ListNode<T>?)

internal fun<T> listPrepend(value: T, tail: ListNode<T>?): ListNode<T> {
    return ListNode(value, tail)
}

internal fun<T> listFirst(list: ListNode<T>?): T? {
    return list?.value
}

internal fun<T> listRest(list: ListNode<T>?): ListNode<T>? {
    return list?.tail
}

private class ListIterator<T>(var node: ListNode<T>?): Iterator<T> {
    override fun hasNext(): Boolean {
        return this.node != null
    }

    override fun next(): T {
        val curNode = this.node
        val result = curNode?.value ?: throw NoSuchElementException()
        this.node = listRest(curNode)
        return result
    }
}

internal fun<T> listIterator(list: ListNode<T>?): Iterator<T> {
    return ListIterator(list)
}

internal fun<T> listFromIterable(it: Iterable<T>): ListNode<T>? {
    var reverseResult: ListNode<T>? = null
    for (entry in it) {
        reverseResult = listPrepend(entry, reverseResult)
    }
    return listReverse(reverseResult)
}

internal fun<T> listReverse(list: ListNode<T>?): ListNode<T>? {
    var result: ListNode<T>? = null
    var cur = list
    while (cur != null) {
        result = listPrepend(cur.value, result)
        cur = listRest(cur)
    }
    return result
}

internal fun<T> listEquals(left: ListNode<T>?, right: ListNode<T>?): Boolean {
    var curLeft = left
    var curRight = right
    while (curLeft != null && curRight != null) {
        val leftFirst = listFirst(curLeft)
        val rightFirst = listFirst(curRight)
        if (leftFirst != rightFirst) {
            return false
        }
        curLeft = listRest(curLeft)
        curRight = listRest(curRight)
    }
    return curLeft == null && curRight == null
}

internal fun<T> listSize(list: ListNode<T>?): Int {
    var result = 0
    var cur = list
    while (cur != null) {
        result++
        cur = listRest(cur)
    }
    return result
}