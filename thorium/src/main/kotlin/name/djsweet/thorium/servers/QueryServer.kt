package name.djsweet.thorium.servers

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import io.vertx.core.AbstractVerticle
import io.vertx.core.DeploymentOptions
import io.vertx.core.Future
import io.vertx.core.eventbus.Message
import io.vertx.core.Vertx
import io.vertx.core.eventbus.EventBus
import io.vertx.core.eventbus.MessageConsumer
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.json.jsonObjectOf
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.vertxFuture
import kotlinx.collections.immutable.PersistentMap
import name.djsweet.query.tree.IdentitySet
import name.djsweet.query.tree.QPTrie
import name.djsweet.query.tree.QueryPath
import name.djsweet.query.tree.QuerySetTree
import name.djsweet.thorium.*
import name.djsweet.thorium.convertStringToByteArray
import name.djsweet.thorium.reestablishByteBudget
import java.util.*
import kotlin.collections.ArrayList
import kotlin.coroutines.suspendCoroutine
import kotlin.math.ceil

data class QueryResponderSpec(
    val query: FullQuery,
    val respondTo: String,
    val clientID: String,
    val addedAt: Long,
    val arrayContainsCounter: Long,
)

abstract class ServerVerticle(protected val verticleOffset: Int): AbstractVerticle() {
    private var byteBudgetResetConsumer: MessageConsumer<ResetByteBudget>? = null
    private var lastFuture = Future.succeededFuture<Unit>()
    protected var byteBudget = 0

    protected abstract fun resetForNewByteBudget()

    protected fun handleStackOverflowWithNewByteBudget() {
        val newByteBudget = reestablishByteBudget(this.vertx.sharedData())
        this.byteBudget = newByteBudget
        this.resetForNewByteBudget()
        this.vertx
            .eventBus()
            .publish(addressForByteBudgetReset, ResetByteBudget(newByteBudget), localRequestOptions)
    }

    private suspend fun<T, U> runCoroutineHandlingStackOverflow(message: Message<T>, handler: suspend (T) -> U): U {
        try {
            return handler(message.body())
        } catch (e: StackOverflowError) {
            this.handleStackOverflowWithNewByteBudget()
            throw e
        }
    }

    protected fun<T, U> runCoroutineAndReply(message: Message<T>, handler: suspend (T) -> U) {
        val self = this
        self.lastFuture = self.lastFuture.eventually { _ ->
            vertxFuture { self.runCoroutineHandlingStackOverflow(message, handler) }.onComplete {
                if (it.succeeded()) {
                    message.reply(it.result())
                } else {
                    message.fail(500, it.cause().message)
                }
            }
        }
    }

    protected fun <T, U> runBlockingAndReply(message: Message<T>, handler: (T) -> U) {
        try {
            val result = handler(message.body())
            message.reply(result)
        } catch (e: StackOverflowError) {
            this.handleStackOverflowWithNewByteBudget()
            message.fail(500, e.message)
        } catch (e: Error) {
            message.fail(500, e.message)
        }
    }

    override fun start() {
        super.start()
        val vertx = this.vertx

        this.byteBudget = getByteBudget(vertx.sharedData())
        this.byteBudgetResetConsumer = vertx.eventBus().localConsumer(addressForByteBudgetReset) { message ->
            this.lastFuture = this.lastFuture.eventually {
                val (newByteBudget) = message.body()
                if (this.byteBudget > newByteBudget) {
                    this.byteBudget = newByteBudget
                    this.resetForNewByteBudget()
                }
                Future.succeededFuture<Void>()
            }
        }
    }

    override fun stop() {
        this.byteBudgetResetConsumer?.unregister()
        super.stop()
    }
}

class ChannelInfo {
    var queryTree = QuerySetTree<QueryResponderSpec>()
    val queriesByClientID = HashMap<String, Pair<QueryResponderSpec, QueryPath>>()

    fun registerQuery(query: FullQuery, clientID: String, respondTo: String): QueryPath {
        val queryTree = this.queryTree
        val queriesByClientID = this.queriesByClientID
        val priorQuery = queriesByClientID[clientID]

        val basisQueryTree = if (priorQuery == null) {
            queryTree
        } else {
            val (priorResponder, priorPath) = priorQuery
            queryTree.removeElementByPath(priorPath, priorResponder)
        }
        val responderSpec = QueryResponderSpec(
            query,
            respondTo,
            clientID,
            0,
            query.countArrayContainsConditions()
        )
        val (newPath, newQueryTree) = basisQueryTree.addElementByQuery(query.treeSpec, responderSpec)

        this.queryTree = newQueryTree
        queriesByClientID[clientID] = responderSpec to newPath
        return newPath
    }

    fun unregisterQuery(clientID: String): QueryPath? {
        val queriesByClientID = this.queriesByClientID
        val priorQuery = queriesByClientID[clientID] ?: return null
        val (responder, path) = priorQuery
        this.queryTree = this.queryTree.removeElementByPath(path, responder)
        queriesByClientID.remove(clientID)
        return path
    }

    fun isEmpty(): Boolean {
        return this.queriesByClientID.isEmpty()
    }
}

inline fun extractKeyPathsFromQueryPath(qp: QueryPath, increment: Int, withKeyPath: (Pair<List<String>, Int>) -> Unit) {
    for (pathComponent in qp.keys()) {
        val decoder = Radix64LowLevelDecoder(pathComponent)
        val sublist = mutableListOf<String>()
        while (decoder.withString { sublist.add(it) }) {
            // This seems silly, but withString already causes a break on `false`.
        }
        withKeyPath(Pair(sublist, increment))
    }
}

const val idempotencyKeyLoadFactor = 0.75

class QueryRouterVerticle(
    private val counters: GlobalCounterContext,
    metrics: MeterRegistry,
    verticleOffset: Int,
    private val idempotencyKeyCacheSize: Int
): ServerVerticle(verticleOffset) {
    private val routerTimer = Timer.builder(routerTimerName)
        .description(routerTimerDescription)
        .tag("router", verticleOffset.toString())
        .register(metrics)

    private val channels = HashMap<String, ChannelInfo>()
    private val idempotencyKeys = HashMap<String, HashSet<String>>(
        ceil(idempotencyKeyCacheSize / idempotencyKeyLoadFactor).toInt(),
        idempotencyKeyLoadFactor.toFloat()
    )
    private val idempotencyKeyRemovalSchedule = LongKeyedBinaryMinHeap<Pair<String, String>>(idempotencyKeyCacheSize)
    private val responders = ArrayList<QueryResponderSpec>()
    private val arrayResponderReferenceCounts = mutableMapOf<QueryResponderSpec, Long>()
    private val arrayResponderInsertionPairs = ArrayList<Pair<ByteArray, ByteArray>>()

    private var idempotencyCleanupTimerID: Long? = null
    private var idempotencyExpirationMS = 0
    private var maxQueryTerms = 0
    private var queryHandler: MessageConsumer<Any>? = null
    private var dataHandler: MessageConsumer<ReportDataList>? = null

    init {
        Gauge.builder(idempotencyKeyCacheSizeName) { idempotencyKeys.size }
            .description(idempotencyKeyCacheSizeDescription)
            .tag("router", verticleOffset.toString())
            .register(metrics)
        Gauge.builder(queryCountName) { this.counters.getQueryCountForThread(this.verticleOffset) }
            .description(queryCountDescription)
            .tag("router", verticleOffset.toString())
            .register(metrics)
    }

    override fun resetForNewByteBudget() {
        val eventBus = this.vertx.eventBus()
        val counters = this.counters
        for ((channel, channelInfo) in this.channels) {
            val removePathIncrements = ArrayList<Pair<List<String>, Int>>()
            for ((_, responderSpecToPath) in channelInfo.queriesByClientID) {
                val (responderSpec, queryPath) = responderSpecToPath
                val (_, respondTo, clientID) = responderSpec
                eventBus.publish(respondTo, UnregisterQueryRequest(channel, clientID))
                extractKeyPathsFromQueryPath(queryPath, -1) {
                    removePathIncrements.add(it)
                }
            }
            counters.updateKeyPathReferenceCountsForChannel(channel, removePathIncrements)
        }
        this.channels.clear()
        this.idempotencyKeys.clear()
        this.idempotencyKeyRemovalSchedule.clear()
        this.counters.resetQueryCountForThread(this.verticleOffset)
    }

    private fun idempotencyKeyTimeoutIsBeforeOrAt(target: Long): Boolean {
        val removalTarget = this.idempotencyKeyRemovalSchedule.peek() ?: return false
        return removalTarget.first <= target
    }

    private inline fun removeIdempotencyEntriesWhile(condition: () -> Boolean) {
        val idempotencyKeys = this.idempotencyKeys
        val idempotencyKeyRemovalSchedule = this.idempotencyKeyRemovalSchedule
        while (condition()) {
            val removalTarget = idempotencyKeyRemovalSchedule.pop() ?: break
            val (_, keyChannel) = removalTarget
            val (idempotencyKey, channel) = keyChannel

            val targetSet = idempotencyKeys[idempotencyKey]
            if (targetSet == null || !targetSet.contains(channel)) {
                continue
            }
            if (targetSet.size <= 1) {
                this.idempotencyKeys.remove(idempotencyKey)
            } else {
                targetSet.remove(channel)
            }
        }
    }

    private fun setupIdempotencyCleanupTimer() {
        val minItem = this.idempotencyKeyRemovalSchedule.peek() ?: return
        val cleanupAt = minItem.first
        val waitFor = cleanupAt - monotonicNowMS()
        val currentTimerID = this.idempotencyCleanupTimerID
        if (currentTimerID != null) {
            this.vertx.cancelTimer(currentTimerID)
        }
        // We're going to sleep at least 1 millisecond between attempts, as a guard against CPU exhaustion
        // in the event that this removal process is buggy.
        val cleanupNow = monotonicNowMS()
        this.idempotencyCleanupTimerID = this.vertx.setTimer(waitFor.coerceAtLeast(1)) {
            this.removeIdempotencyEntriesWhile { idempotencyKeyTimeoutIsBeforeOrAt(cleanupNow) }
            this.setupIdempotencyCleanupTimer()
        }
    }

    private suspend fun registerQuery(req: RegisterQueryRequest): HttpProtocolErrorOrJson {
        // This is a suspend function because of the weird whenError/whenSuccess callback mechanisms.
        // We're luckily suspending anyway, but... whoops.
        return suspendCoroutine { cont ->
            while (true) {
                val possiblyFullQuery: HttpProtocolErrorOr<FullQueryAndAffectedKeyIncrements>
                try {
                    possiblyFullQuery = convertQueryStringToFullQuery(
                        req.queryParams,
                        this.maxQueryTerms,
                        this.byteBudget
                    )
                } catch (x: StackOverflowError) {
                    // This is an intentional early trapping of StackOverflowError, even though
                    // it's being handled transparently in runAndReply. Doing this here allows us
                    // to re-attempt without a crash, which should result in .whenError getting
                    // an appropriate HTTP status code.
                    this.handleStackOverflowWithNewByteBudget()
                    continue
                }
                possiblyFullQuery
                    .whenError { cont.resumeWith(Result.success(HttpProtocolErrorOrJson.ofError(it))) }
                    .whenSuccess { (query, increments) ->
                        val (channel) = req
                        val channels = this.channels
                        val counters = this.counters
                        val channelInfo = channels[channel] ?: ChannelInfo()
                        val basisQueryTreeSize = channelInfo.queryTree.size
                        channelInfo.registerQuery(query, req.clientID, req.returnAddress)
                        channels[channel] = channelInfo
                        counters.alterQueryCountForThread(
                            this.verticleOffset,
                            channelInfo.queryTree.size - basisQueryTreeSize
                        )
                        counters.updateKeyPathReferenceCountsForChannel(channel, increments)
                        cont.resumeWith(
                            Result.success(
                                HttpProtocolErrorOrJson.ofSuccess(
                                    jsonObjectOf(
                                        "channel" to req.channel,
                                        "clientID" to req.clientID
                                    )
                                )
                            )
                        )
                    }
                break
            }
        }
    }

    private fun unregisterQuery(
        channel: String,
        clientID: String,
        withRemovedPath: (QueryPath) -> Unit
    ): HttpProtocolErrorOrJson {
        val channelInfo = this.channels[channel] ?: return HttpProtocolErrorOrJson.ofError(
            HttpProtocolError(
                404, jsonObjectOf(
                    "code" to "missing-channel",
                    "channel" to channel
                )
            )
        )

        val basisQueryTreeSize = channelInfo.queryTree.size
        val involvedPath = channelInfo.unregisterQuery(clientID)
        return if (involvedPath == null) {
            HttpProtocolErrorOrJson.ofError(
                HttpProtocolError(
                    404, jsonObjectOf(
                        "code" to "missing-client-id",
                        "channel" to channel,
                        "clientID" to clientID
                    )
                )
            )
        } else {
            withRemovedPath(involvedPath)
            this.counters.alterQueryCountForThread(
                this.verticleOffset,
                channelInfo.queryTree.size - basisQueryTreeSize
            )
            if (channelInfo.isEmpty()) {
                this.channels.remove(channel)
            }
            HttpProtocolErrorOrJson.ofSuccess(
                jsonObjectOf(
                    "channel" to channel,
                    "clientID" to clientID
                )
            )
        }
    }

    private fun unregisterQuery(req: UnregisterQueryRequest): HttpProtocolErrorOrJson {
        return this.unregisterQuery(req.channel, req.clientID) { queryPath ->
            val decrements = mutableListOf<Pair<List<String>, Int>>()
            extractKeyPathsFromQueryPath(queryPath, -1) {
                decrements.add(it)
            }
            this.counters.updateKeyPathReferenceCountsForChannel(req.channel, decrements)
        }
    }

    private fun trySendDataToResponder(
        eventBus: EventBus,
        data: ReportData,
        responder: QueryResponderSpec,
        prior: Future<Message<Any>>,
        removedPathIncrements: MutableList<Pair<List<String>, Int>>
    ): Future<Message<Any>> {
        val (_, respondTo, clientID) = responder
        val current = eventBus.request<Any>(respondTo, data, localRequestOptions).onFailure {
            this.unregisterQuery(data.channel, responder.clientID) { queryPath ->
                extractKeyPathsFromQueryPath(queryPath, -1) {
                    removedPathIncrements.add(it)
                }
            }
            eventBus.publish(respondTo, UnregisterQueryRequest(data.channel, clientID))
        }
        return prior.eventually { current }
    }

    private fun respondToData(response: ReportData) {
        val responders = this.responders
        val idempotencyKeys = this.idempotencyKeys
        val idempotencyKeyCacheSize = this.idempotencyKeyCacheSize
        val arrayResponderReferenceCounts = this.arrayResponderReferenceCounts
        val arrayResponderInsertionPairs = this.arrayResponderInsertionPairs
        var respondFutures = Future.succeededFuture<Message<Any>>()
        val removedPathIncrements = ArrayList<Pair<List<String>, Int>>()
        val (channel, idempotencyKey, queryableScalarData, queryableArrayData) = response

        try {
            val eventBus = this.vertx.eventBus()
            val respondingChannel = this.channels[channel] ?: return
            val queryTree = respondingChannel.queryTree
            val idempotencySet = idempotencyKeys[idempotencyKey]
            if (idempotencySet?.contains(channel) == true) {
                return
            }

            val cleanupStart = monotonicNowMS()
            this.removeIdempotencyEntriesWhile {
                idempotencyKeys.size >= idempotencyKeyCacheSize || idempotencyKeyTimeoutIsBeforeOrAt(cleanupStart)
            }

            val idempotencyKeyExpiration = monotonicNowMS() + this.idempotencyExpirationMS
            val priorRemovalScheduleSize = this.idempotencyKeyRemovalSchedule.size
            val updateSet = (idempotencySet ?: HashSet())
            updateSet.add(channel)
            idempotencyKeys[idempotencyKey] = updateSet
            this.idempotencyKeyRemovalSchedule.push(idempotencyKeyExpiration to (idempotencyKey to channel))
            if (priorRemovalScheduleSize == 0) {
                this.setupIdempotencyCleanupTimer()
            }

            queryTree.visitByData(queryableScalarData.trie) { _, responder ->
                responders.add(responder)
            }

            var arrayResponderQueries = QPTrie<QPTrie<IdentitySet<QueryResponderSpec>>>()
            for (responder in responders) {
                val (query, _, _, _, arrayContainsCount) = responder
                if (!query.notEqualsMatchesData(queryableScalarData.trie)) {
                    continue
                }
                // If we have any arrayContains we have to inspect, save this for another phase
                if (arrayContainsCount > 0) {
                    arrayResponderReferenceCounts[responder] = arrayContainsCount
                    query.arrayContains.visitUnsafeSharedKey { (key, values) ->
                        values.visitAscendingUnsafeSharedKey { (value) ->
                            arrayResponderInsertionPairs.add(Pair(key, value))
                        }
                    }

                    for ((key, value) in arrayResponderInsertionPairs) {
                        arrayResponderQueries = arrayResponderQueries.update(key) { valueTrie ->
                            (valueTrie ?: QPTrie()).update(value) {
                                (it ?: IdentitySet()).add(responder)
                            }
                        }
                    }

                    arrayResponderInsertionPairs.clear()
                    continue
                }
                respondFutures = this.trySendDataToResponder(
                    eventBus,
                    response,
                    responder,
                    respondFutures,
                    removedPathIncrements
                )
            }
            responders.clear()

            if (arrayResponderReferenceCounts.size > 0L) {
                queryableArrayData.trie.visitUnsafeSharedKey { (key, values) ->
                    val respondersForKey = arrayResponderQueries.get(key) ?: return@visitUnsafeSharedKey
                    for (value in values) {
                        val respondersForValue = respondersForKey.get(value) ?: continue
                        var nextQueriesForValue = respondersForValue
                        respondersForValue.visitAll { responder ->
                            nextQueriesForValue = nextQueriesForValue.remove(responder)
                            val nextReferenceCount = (arrayResponderReferenceCounts[responder] ?: 0) - 1
                            if (nextReferenceCount == 0L) {
                                arrayResponderReferenceCounts.remove(responder)
                                respondFutures =
                                    this.trySendDataToResponder(
                                        eventBus,
                                        response,
                                        responder,
                                        respondFutures,
                                        removedPathIncrements
                                    )
                            } else {
                                arrayResponderReferenceCounts[responder] = nextReferenceCount
                            }
                        }
                        arrayResponderQueries = arrayResponderQueries.update(key) { valuesTrie ->
                            valuesTrie?.update(value) {
                                if (nextQueriesForValue.size > 0) {
                                    nextQueriesForValue
                                } else {
                                    null
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            // Backpressure: we don't return until all clients either accept the message, or we time out.
            // If we time out, we remove the query and attempt to notify the client that we have done so.
            // However, this won't block us from processing any other messages in this worker.
            respondFutures.onComplete {
                val counters = this.counters

                counters.updateKeyPathReferenceCountsForChannel(channel, removedPathIncrements)
                counters.decrementOutstandingDataCount(1)
            }

            responders.clear()
            arrayResponderInsertionPairs.clear()
            arrayResponderReferenceCounts.clear()
        }
    }

    private fun respondToDataTimed(reportData: ReportData) {
        return this.routerTimer.record<Unit> { this.respondToData(reportData) }!!
    }

    private fun respondToDataList(reportDataList: ReportDataList) {
        for (entry in reportDataList.entries) {
            if (entry == null) {
                continue
            }
            this.respondToDataTimed(entry)
        }
    }

    override fun start() {
        super.start()

        this.idempotencyExpirationMS = getIdempotencyExpirationMS(this.vertx.sharedData())
        this.maxQueryTerms = getMaxQueryTerms(this.vertx.sharedData())

        val eventBus = this.vertx.eventBus()
        val sharedData = this.vertx.sharedData()
        val queryAddress = addressForQueryServerQuery(sharedData, this.verticleOffset)
        this.queryHandler = eventBus.localConsumer(queryAddress) { message ->
            when (val body = message.body()) {
                is RegisterQueryRequest -> this.runCoroutineAndReply(message) { this.registerQuery(body) }
                is UnregisterQueryRequest -> this.runBlockingAndReply(message) { this.unregisterQuery(body) }
                else -> HttpProtocolErrorOrJson.ofError(
                    HttpProtocolError(
                        500, jsonObjectOf(
                            "code" to "invalid-request"
                        )
                    )
                )
            }
        }
        this.dataHandler = eventBus.localConsumer(addressForQueryServerData) { message ->
            this.runBlockingAndReply(message) { respondToDataList(it) }
        }
    }

    override fun stop() {
        this.dataHandler?.unregister()
        this.queryHandler?.unregister()

        val timerID = this.idempotencyCleanupTimerID
        if (timerID != null) {
            this.vertx.cancelTimer(timerID)
        }

        super.stop()
    }
}

class OnlyPathsWithReferencesFilterContext(
    private val referenceCounts: PersistentMap<String, KeyPathReferenceCount>
): KeyValueFilterContext {
    override fun includeKeyValue(key: String, value: Any?): Boolean {
        val refCountForKey = this.referenceCounts[key] ?: return false
        return if (value is JsonObject) {
            refCountForKey.subKeys.size > 0
        } else {
            refCountForKey.references > 0
        }
    }

    override fun contextForObject(key: String): KeyValueFilterContext {
        val refCountForKey = this.referenceCounts[key] ?: return AcceptNoneKeyValueFilterContext()
        val (_, subKeys) = refCountForKey
        return if (subKeys.size > 0) {
            OnlyPathsWithReferencesFilterContext(subKeys)
        } else {
            AcceptNoneKeyValueFilterContext()
        }
    }
}

val jsonResponseForEmptyRequest = jsonObjectOf("code" to "internal-failure-empty-request")
val baseJsonResponseForOverSizedChannelInfoIdempotencyKey = jsonObjectOf(
    "code" to "channel-idempotency-too-large"
)
val baseJsonResponseForStackOverflowData = jsonObjectOf(
    "code" to "exhausted-byte-budget"
)

// Translation doesn't implement any form of idempotent caching, and instead blindly translates
// every request sent to it. At a high enough update frequency, this is absolutely the right choice:
// we would be growing our working set of "cached data" up to millions of saved entries rather quickly,
// just to save on the "cost" of reprocessing a tiny handful.
class JsonToQueryableTranslatorVerticle(
    private val counters: GlobalCounterContext,
    metrics: MeterRegistry,
    verticleOffset: Int,
): ServerVerticle(verticleOffset) {
    private val unpackRequestTimer = Timer.builder(translationTimerName)
        .description(translationTimerDescription)
        .tag("translator", verticleOffset.toString())
        .register(metrics)
    private var maxJsonParsingRecursion = 16
    private var requestHandler: MessageConsumer<UnpackDataRequestList>? = null

    override fun resetForNewByteBudget() {
        // We aren't keeping any state that is affected by the byte budget.
    }

    private fun handleUnpackDataRequest(req: UnpackDataRequestWithIndex): HttpProtocolErrorOrReportDataWithIndex {
        val (index, message) = req
        if (message == null) {
            return HttpProtocolErrorOrReportDataWithIndex.ofError(
                HttpProtocolError(
                    400,
                    jsonResponseForEmptyRequest
                )
            )
        }
        val (channel, idempotencyKey, data) = message
        val channelBytes = convertStringToByteArray(channel)
        val byteBudget = this.byteBudget
        val maxJsonParsingRecursion = this.maxJsonParsingRecursion
        val filterContext = when (val referenceCounts = this.counters.getKeyPathReferenceCountsForChannel(channel)) {
            null -> AcceptNoneKeyValueFilterContext()
            else -> OnlyPathsWithReferencesFilterContext(referenceCounts.subKeys)
        }

        val getDataString = thunkForReportDataString {
            try {
                data.encode()
            } catch (e: Exception) {
                // Something has gone horribly wrong trying to convert this JSON to a string.
                // We can't actually do anything with this in terms of queries.
                "{}"
            }
        }
        try {
            val idempotencyKeyBytes = convertStringToByteArray(idempotencyKey)
            if (idempotencyKeyBytes.size + channelBytes.size > byteBudget) {
                return HttpProtocolErrorOrReportDataWithIndex.ofError(
                    HttpProtocolError(
                        413,
                        baseJsonResponseForOverSizedChannelInfoIdempotencyKey.copy().put(
                            "maxByteSize", byteBudget
                        ).put(
                            "eventID", idempotencyKey
                        )
                    )
                )
            }
            val (scalars, arrays) = encodeJsonToQueryableData(data, filterContext, byteBudget, maxJsonParsingRecursion)
            return HttpProtocolErrorOrReportDataWithIndex.ofSuccess(
                ReportDataWithIndex(
                    index,
                    ReportData(
                        channel,
                        idempotencyKey,
                        queryableScalarData = scalars,
                        queryableArrayData = arrays,
                        actualData = getDataString
                    )
                )
            )
        } catch (e: StackOverflowError) {
            this.handleStackOverflowWithNewByteBudget()
            return HttpProtocolErrorOrReportDataWithIndex.ofError(
                HttpProtocolError(
                    507,
                    baseJsonResponseForStackOverflowData.copy().put(
                        "maxByteSize", byteBudget
                    )
                )
            )
        }
    }

    private fun handleUnpackDataRequestTimed(req: UnpackDataRequestWithIndex): HttpProtocolErrorOrReportDataWithIndex {
        return this.unpackRequestTimer.record<HttpProtocolErrorOrReportDataWithIndex> {
            this.handleUnpackDataRequest(req)
        }!!
    }

    private fun handleUnpackDataRequestList(requests: UnpackDataRequestList): HttpProtocolErrorOrReportDataListWithIndexes {
        val results = mutableListOf<ReportDataWithIndex>()
        var error: HttpProtocolError? = null
        for (request in requests.requests) {
            if (request == null) {
                continue
            }
            this.handleUnpackDataRequestTimed(request)
                .whenError { error = it }
                .whenSuccess { results.add(it) }
            val lastError = error
            if (lastError != null) {
                return HttpProtocolErrorOrReportDataListWithIndexes.ofError(lastError)
            }
        }
        return HttpProtocolErrorOrReportDataListWithIndexes.ofSuccess(ReportDataListWithIndexes(results))
    }

    override fun start() {
        super.start()

        val vertx = this.vertx
        val sharedData = vertx.sharedData()
        val eventBus = vertx.eventBus()
        this.maxJsonParsingRecursion = getMaxJsonParsingRecursion(sharedData)

        this.requestHandler = eventBus.localConsumer(addressForTranslatorServer(sharedData, this.verticleOffset)) {
            message -> this.runBlockingAndReply(message) { this.handleUnpackDataRequestList(it) }
        }
    }

    override fun stop() {
        this.requestHandler?.unregister()
        super.stop()
    }
}

suspend fun registerQueryServer(
    vertx: Vertx,
    counters: GlobalCounterContext,
    metrics: MeterRegistry,
    queryVerticleOffset: Int,
    queryVerticles: Int,
    serverVerticleOffset: Int,
    serverVerticles: Int
): Set<String> {
    val opts = DeploymentOptions().setWorker(true)
    val futures = mutableListOf<Future<String>>()
    val lastQueryVerticleOffset = queryVerticleOffset + queryVerticles
    val idempotencyKeyCacheSize = getMaximumIdempotencyKeys(vertx.sharedData())
    for (i in queryVerticleOffset until lastQueryVerticleOffset) {
        futures.add(vertx.deployVerticle(QueryRouterVerticle(counters, metrics, i, idempotencyKeyCacheSize), opts))
    }
    val lastServerVerticleOffset = serverVerticleOffset + serverVerticles
    for (i in serverVerticleOffset until lastServerVerticleOffset) {
        futures.add(vertx.deployVerticle(JsonToQueryableTranslatorVerticle(counters, metrics, i), opts))
    }

    val deploymentIDs = mutableSetOf<String>()
    for (future in futures) {
        val deploymentID = future.await()
        deploymentIDs.add(deploymentID)
    }

    return deploymentIDs
}