// SPDX-FileCopyrightText: 2023 Dani Sweet <sidereal@djsweet.name>
//
// SPDX-License-Identifier: MIT

package name.djsweet.sidereal.servers

import io.micrometer.core.instrument.FunctionTimer
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.vertx.core.AbstractVerticle
import io.vertx.core.DeploymentOptions
import io.vertx.core.Future
import io.vertx.core.eventbus.Message
import io.vertx.core.Vertx
import io.vertx.core.eventbus.EventBus
import io.vertx.core.eventbus.MessageConsumer
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.json.jsonObjectOf
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.awaitEvent
import io.vertx.kotlin.coroutines.vertxFuture
import kotlinx.coroutines.delay
import name.djsweet.query.tree.QueryPath
import name.djsweet.query.tree.QuerySetTree
import name.djsweet.sidereal.*
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.collections.ArrayList
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine
import kotlin.math.ceil
import kotlin.time.toKotlinDuration

internal const val siderealMetaQueryRegistrationType = "name.djsweet.sidereal.meta.query.registration"
internal const val siderealMetaQueryRemovalType = "name.djsweet.sidereal.meta.query.removal"
private const val backoffBaseWaitTimeMS = 0.5

data class QueryResponderSpec(
    val queries: List<FullQuery>,
    val respondTo: String,
    val clientID: String,
    val queryID: String,
    val addedAt: Long,
    val arrayContainsCounts: List<Long>,
)

abstract class ServerVerticle(
    protected val config: GlobalConfig,
    protected val verticleOffset: Int
): AbstractVerticle() {
    private var byteBudgetResetConsumer: MessageConsumer<ResetByteBudget>? = null
    private var lastFuture = Future.succeededFuture<Unit>()
    protected var byteBudget = 0

    protected abstract fun resetForNewByteBudget()

    protected fun handleStackOverflowWithNewByteBudget() {
        val newByteBudget = this.config.reestablishByteBudget()
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

        this.byteBudget = this.config.byteBudget
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
    val queriesByClientThenQueryID = HashMap<String, HashMap<String, Pair<QueryResponderSpec, List<QueryPath>>>>()

    fun registerQuery(queries: List<FullQuery>, clientID: String, queryID: String, respondTo: String): List<QueryPath> {
        val queryTree = this.queryTree
        val queryIDsByClientID = this.queriesByClientThenQueryID
        val queriesByQueryID = queryIDsByClientID[clientID] ?: HashMap()
        val priorQuery = queriesByQueryID[clientID]

        val basisQueryTree = if (priorQuery == null) {
            queryTree
        } else {
            val (priorResponder, priorPaths) = priorQuery
            priorPaths.fold(queryTree) { qt, priorPath -> qt.removeElementByPath(priorPath, priorResponder) }
        }

        val arrayContainsCounts = queries.map { it.countArrayContainsConditions() }
        val responderSpec = QueryResponderSpec(
            queries,
            respondTo,
            clientID,
            queryID,
            monotonicNowMS(),
            arrayContainsCounts
        )
        val newPaths = mutableListOf<QueryPath>()
        var newQueryTree = basisQueryTree
        for (query in queries) {
            val (newPath, nextQueryTree) = newQueryTree.addElementByQuery(query.treeSpec, responderSpec)
            newPaths.add(newPath)
            newQueryTree = nextQueryTree
        }

        this.queryTree = newQueryTree
        queriesByQueryID[queryID] = responderSpec to newPaths
        queryIDsByClientID[clientID] = queriesByQueryID
        return newPaths
    }

    fun unregisterQuery(clientID: String, queryID: String): Pair<List<QueryPath>, Long>? {
        val queriesByClientID = this.queriesByClientThenQueryID
        val priorQueriesByQueryID = queriesByClientID[clientID] ?: return null
        val priorQuery = priorQueriesByQueryID[queryID] ?: return null
        val (responder, path) = priorQuery
        for (pathElem in path) {
            this.queryTree = this.queryTree.removeElementByPath(pathElem, responder)
        }
        priorQueriesByQueryID.remove(queryID)
        if (priorQueriesByQueryID.isEmpty()) {
            queriesByClientID.remove(clientID)
        }
        return Pair(path, responder.queries.size.toLong())
    }

    fun isEmpty(): Boolean {
        return this.queriesByClientThenQueryID.isEmpty()
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
const val outstandingDecrementDelayMS = 8L

class QueryRouterVerticle(
    config: GlobalConfig,
    private val counters: GlobalCounterContext,
    private val metrics: MeterRegistry,
    verticleOffset: Int,
    private val idempotencyKeyCacheSize: Int
): ServerVerticle(config, verticleOffset) {
    private val log = LoggerFactory.getLogger(QueryRouterVerticle::class.java)

    private val respondToDataCount = AtomicLong()
    private val respondToDataTotalTime = AtomicLong()

    private val channels = HashMap<String, ChannelInfo>()
    private val idempotencyKeys = HashMap<String, HashSet<String>>(
        ceil(idempotencyKeyCacheSize / idempotencyKeyLoadFactor).toInt(),
        idempotencyKeyLoadFactor.toFloat()
    )
    private val idempotencyKeyRemovalSchedule = LongKeyedBinaryMinHeap<Pair<String, String>>(idempotencyKeyCacheSize)
    private val responders = HashSet<QueryResponderSpec>()
    private val responsesByReplyAddress = HashMap<String, ReportDataWithClientAndQueryIDs>()

    private var idempotencyCleanupTimerID: Long? = null
    private var idempotencyExpirationMS = 0
    private var maxQueryTerms = 0
    private var maxOutstandingDecrements = 1024L
    private var queryHandler: MessageConsumer<Any>? = null
    private var dataHandler: MessageConsumer<ReportDataList>? = null

    private var cacheKeyGauge: Gauge? = null
    private var queryCountGauge: Gauge? = null
    private var functionTimer: FunctionTimer? = null

    override fun resetForNewByteBudget() {
        val eventBus = this.vertx.eventBus()
        val counters = this.counters
        for ((channel, channelInfo) in this.channels) {
            val removePathIncrements = ArrayList<Pair<List<String>, Int>>()
            for ((_, queryIDsToQueries) in channelInfo.queriesByClientThenQueryID) {
                for ((_, responderSpecToPath) in queryIDsToQueries) {
                    val (responderSpec, queryPaths) = responderSpecToPath
                    val (_, respondTo, clientID, queryID) = responderSpec
                    eventBus.publish(respondTo, UnregisterQueryRequest(channel, clientID, queryID))
                    for (queryPath in queryPaths) {
                        extractKeyPathsFromQueryPath(queryPath, -1) {
                            removePathIncrements.add(it)
                        }
                    }
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

    private fun unpackDataRequestForMetaChannel(
        clientID: String,
        queryID: String,
        eventType: String,
        idPrefix: String,
        data: JsonObject
    ): UnpackDataRequest {
        val config = this.config
        val globalID = "${config.instanceID} $idPrefix$clientID $queryID"
        val sourceName = config.sourceName
        return UnpackDataRequest(
            channel = metaChannelName,
            data = jsonObjectOf(
                "source" to sourceName,
                "id" to globalID,
                "specversion" to "1.0",
                "type" to eventType,
                "time" to wallNowAsString(),
                "data" to data
            ),
            idempotencyKey = "$sourceName $globalID"
        )
    }

    private suspend fun registerQuery(req: RegisterQueryRequest): HttpProtocolErrorOrJson {
        // This is a suspend function because of the weird whenError/whenSuccess callback mechanisms.
        // We're luckily suspending anyway, but... whoops.
        return suspendCoroutine { cont ->
            val (channel, clientID, queryID, queryString, queryParams, returnAddress) = req

            stackOverflowRetry@ while (true) {
                val fullQueries = mutableListOf<FullQuery>()
                val keyIncrements = mutableListOf<Pair<List<String>, Int>>()

                for (conjunction in queryParams) {
                    val possiblyFullQuery: HttpProtocolErrorOr<FullQueryAndAffectedKeyIncrements>
                    try {
                        possiblyFullQuery = convertQueryStringToFullQuery(
                            conjunction,
                            this.maxQueryTerms,
                            this.byteBudget
                        )
                    } catch (x: StackOverflowError) {
                        // This is an intentional early trapping of StackOverflowError, even though
                        // it's being handled transparently in runAndReply. Doing this here allows us
                        // to re-attempt without a crash, which should result in .whenError getting
                        // an appropriate HTTP status code.
                        this.handleStackOverflowWithNewByteBudget()
                        continue@stackOverflowRetry
                    }
                    var hadError = false
                    possiblyFullQuery
                        .whenError {
                            cont.resumeWith(Result.success(HttpProtocolErrorOrJson.ofError(it)))
                            hadError = true
                        }.whenSuccess { (query, increments) ->
                            fullQueries.add(query)
                            keyIncrements.addAll(increments)
                        }
                    if (hadError) {
                        break@stackOverflowRetry
                    }
                }

                val channels = this.channels
                val config = this.config
                val counters = this.counters
                val channelInfo = channels[channel] ?: ChannelInfo()

                channelInfo.registerQuery(fullQueries, clientID, queryID, returnAddress)
                channels[channel] = channelInfo
                counters.alterQueryCountForThread(
                    this.verticleOffset,
                    fullQueries.size.toLong()
                )
                counters.updateKeyPathReferenceCountsForChannel(channel, keyIncrements)
                sendUnpackDataRequests(
                    this.vertx,
                    config,
                    counters,
                    listOf(
                        this.unpackDataRequestForMetaChannel(
                            clientID = clientID,
                            queryID = queryID,
                            eventType = siderealMetaQueryRegistrationType,
                            idPrefix = "+",
                            data = jsonObjectOf(
                                "instanceID" to config.instanceID,
                                "channel" to channel,
                                "clientID" to clientID,
                                "queryID" to queryID,
                                "query" to queryString
                            )
                        )
                    ),
                    respectLimit = false
                ).onComplete {
                    // We'll have to wait for the meta channel send before we report to the registering
                    // client that we've connected. Query registrants may depend on the meta channel send
                    // strictly happening before the query was registered, possibly as a registration
                    // verification measure.
                    if (it.failed()) {
                        // Note that this will also attempt to send over the meta channel, too, but will
                        // report a channel/clientID pair that possibly nobody saw. Consumers should be aware
                        // of this possible failure mode.
                        val self = this
                        vertxFuture {
                            self.unregisterQuery(UnregisterQueryRequest(channel, clientID, queryID))
                            cont.resumeWithException(it.cause())
                        }
                    } else {
                        cont.resumeWith(
                            Result.success(
                                HttpProtocolErrorOrJson.ofSuccess(
                                    jsonObjectOf(
                                        "channel" to channel,
                                        "clientID" to clientID,
                                        "queryID" to queryID
                                    )
                                )
                            )
                        )
                    }
                }

                break@stackOverflowRetry
            }
        }
    }

    private suspend fun unregisterQuery(
        channel: String,
        clientID: String,
        queryID: String,
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

        val involvedPathsAndQuerySize = channelInfo.unregisterQuery(clientID, queryID)
        return if (involvedPathsAndQuerySize == null) {
            HttpProtocolErrorOrJson.ofError(
                HttpProtocolError(
                    404, jsonObjectOf(
                        "code" to "missing-client-id",
                        "channel" to channel,
                        "clientID" to clientID,
                        "queryID" to queryID
                    )
                )
            )
        } else {
            val (involvedPaths, querySize) = involvedPathsAndQuerySize

            for (involvedPath in involvedPaths) {
                withRemovedPath(involvedPath)
            }
            val counters = this.counters
            val config = this.config
            val vertx = this.vertx
            if (channelInfo.isEmpty()) {
                this.channels.remove(channel)
            }
            val unpackRequest = listOf(this.unpackDataRequestForMetaChannel(
                clientID = clientID,
                queryID = queryID,
                eventType = siderealMetaQueryRemovalType,
                idPrefix = "-",
                data = jsonObjectOf(
                    "instanceID" to config.instanceID,
                    "channel" to channel,
                    "clientID" to clientID,
                    "queryID" to queryID,
                )
            ))
            val backoffSession = BackoffSession(backoffBaseWaitTimeMS)
            while (true) {
                val sendResult = awaitEvent {
                    sendUnpackDataRequests(vertx, config, counters, unpackRequest, respectLimit = true).onComplete(it)
                }
                if (sendResult.succeeded()) {
                    val status = sendResult.result()
                    if (status.withinLimit && status.error == null) {
                        break
                    }
                } else {
                    this.log.atWarn()
                        .setMessage("Could not report query removal")
                        .addKeyValue("channel", channel)
                        .addKeyValue("clientID", clientID)
                        .addKeyValue("queryID", queryID)
                        .log()
                }
                val sleepTimeMS = backoffSession.sleepTimeMS()
                delay(Duration.ofNanos((sleepTimeMS * 1_000_000).toLong()).toKotlinDuration())
            }
            counters.alterQueryCountForThread(
                this.verticleOffset,
                -querySize
            )
            HttpProtocolErrorOrJson.ofSuccess(
                jsonObjectOf(
                    "channel" to channel,
                    "clientID" to clientID,
                    "queryID" to queryID
                )
            )
        }
    }

    private suspend fun unregisterQuery(req: UnregisterQueryRequest): HttpProtocolErrorOrJson {
        return this.unregisterQuery(req.channel, req.clientID, req.queryID) { queryPath ->
            val decrements = mutableListOf<Pair<List<String>, Int>>()
            extractKeyPathsFromQueryPath(queryPath, -1) {
                decrements.add(it)
            }
            this.counters.updateKeyPathReferenceCountsForChannel(req.channel, decrements)
        }
    }

    private fun trySendDataToResponder(
        eventBus: EventBus,
        respondTo: String,
        data: ReportDataWithClientAndQueryIDs,
        prior: Future<Message<Any>>,
        removedPathIncrements: MutableList<Pair<List<String>, Int>>
    ): Future<Message<Any>> {
        val (reportData, clientID, queryIDs) = data
        val current = eventBus.request<Any>(respondTo, data, localRequestOptions).onFailure {
            val self = this
            vertxFuture {
                for (queryID in queryIDs) {
                    self.unregisterQuery(reportData.channel, clientID, queryID) { queryPath ->
                        extractKeyPathsFromQueryPath(queryPath, -1) {
                            removedPathIncrements.add(it)
                        }
                    }
                    eventBus.publish(respondTo, UnregisterQueryRequest(reportData.channel, clientID, queryID))
                }
            }
        }
        return prior.eventually { current }
    }

    private var outstandingDecrements = 0L
    private var reportDecrementTimerID: Long? = null

    private fun reportOutstandingDecrements() {
        val toReport = this.outstandingDecrements
        if (toReport > 0) {
            this.outstandingDecrements = 0
            this.counters.decrementOutstandingEventCount(toReport)
        }
        this.reportDecrementTimerID = null
    }

    private fun deferOutstandingDecrement() {
        this.outstandingDecrements += 1
        if (this.outstandingDecrements >= this.maxOutstandingDecrements) {
            val timerID = this.reportDecrementTimerID
            if (timerID != null) {
                this.vertx.cancelTimer(timerID)
            }
            this.reportOutstandingDecrements()
        } else if (this.reportDecrementTimerID == null) {
            this.reportDecrementTimerID = this.vertx.setTimer(outstandingDecrementDelayMS) {
                this.reportOutstandingDecrements()
            }
        }
    }

    private fun respondToData(reportData: ReportData) {
        val responders = this.responders
        val responsesByReturnAddress = this.responsesByReplyAddress
        val idempotencyKeys = this.idempotencyKeys
        val idempotencyKeyCacheSize = this.idempotencyKeyCacheSize

        val removedPathIncrements = ArrayList<Pair<List<String>, Int>>()
        val (channel, idempotencyKey, queryableScalarDataShared, queryableArrayDataShared) = reportData
        val queryableScalarData = queryableScalarDataShared.trie
        val queryableArrayData = queryableArrayDataShared.trie
        var respondFutures = Future.succeededFuture<Message<Any>>()

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

            queryTree.visitByData(queryableScalarData) { _, responder ->
                responders.add(responder)
            }

            responderLoop@ for (responder in responders) {
                val (queries, respondTo, clientID, queryID, _, arrayContainsCounts) = responder
                var matched = false
                queryLoop@ for ((index, query) in queries.withIndex()) {
                    if (!query.startsWithMatchesAtOffsets(queryableScalarData)) {
                        continue@queryLoop
                    }
                    if (!query.notEqualsMatchesData(queryableScalarData)) {
                        continue@queryLoop
                    }

                    // If we have a relative comparison (one of <, <=, >=, >), we need to ensure that the
                    // type of the data is compatible with the type of the bounds.
                    val relativeComparisonBounds = query.treeSpec.relativeComparisonValuesUnsafeShared()
                    val relativeComparisonKey = relativeComparisonBounds?.key
                    val lowerBound = relativeComparisonBounds?.lowerBound
                    val upperBound = relativeComparisonBounds?.upperBound

                    if (relativeComparisonKey != null && (lowerBound != null || upperBound != null)) {
                        val fromData = queryableScalarData.get(relativeComparisonKey) ?: continue@queryLoop
                        var typeMatched = false
                        if (lowerBound != null) {
                            typeMatched = Radix64JsonEncoder.encodedValuesHaveSameType(lowerBound, fromData)
                        }
                        if (!typeMatched && upperBound != null) {
                            typeMatched = Radix64JsonEncoder.encodedValuesHaveSameType(upperBound, fromData)
                        }
                        if (!typeMatched) {
                            continue@queryLoop
                        }
                    }
                    // If we don't have any arrayContains to inspect for _this_ query, we can short-circuit out
                    // and call it good.
                    var arrayReferenceCount = arrayContainsCounts[index]
                    if (arrayReferenceCount <= 0L) {
                        // We don't actually have to perform the array checks here! This responder will already
                        // be triggered without having to go through the array checks.
                        matched = true
                        break@queryLoop
                    }

                    // We want to iterate as little as possible here, so that means greedily choosing to iterate
                    // over the smallest of each collection.
                    val arrayContains = query.arrayContains
                    if (arrayContains.size < queryableArrayData.size) {
                        arrayContains.visitUnsafeSharedKey keysVisit@ { (key, wantedValues) ->
                            val dataValuesForKey = queryableArrayData.get(key) ?: return@keysVisit
                            valuesVisit@ for (value in dataValuesForKey) {
                                wantedValues.get(value) ?: continue@valuesVisit
                                arrayReferenceCount--
                                if (arrayReferenceCount == 0L) {
                                    matched = true
                                    return@keysVisit
                                }
                            }
                        }
                    } else {
                        queryableArrayData.visitUnsafeSharedKey keysVisit@ { (key, dataValuesForKey) ->
                            val wantedValues = arrayContains.get(key) ?: return@keysVisit
                            valuesVisit@ for (value in dataValuesForKey) {
                                wantedValues.get(value) ?: continue@valuesVisit
                                arrayReferenceCount--
                                if (arrayReferenceCount == 0L) {
                                    matched = true
                                    return@keysVisit
                                }
                            }
                        }
                    }
                    // If we've already matched the data, we don't have to inspect any more queries.
                    if (matched) {
                        break@queryLoop
                    }
                }

                if (matched) {
                    val response = responsesByReturnAddress[respondTo]
                        ?: ReportDataWithClientAndQueryIDs(reportData, clientID)
                    response.queryIDs.add(queryID)
                    responsesByReturnAddress[respondTo] = response
                }
            }

            for ((respondTo, response) in responsesByReturnAddress) {
                respondFutures = this.trySendDataToResponder(
                    eventBus,
                    respondTo,
                    response,
                    respondFutures,
                    removedPathIncrements
                )
            }
        } finally {
            // Backpressure: we don't return until all clients either accept the message, or we time out.
            // If we time out, we remove the query and attempt to notify the client that we have done so.
            // However, this won't block us from processing any other messages in this worker.
            respondFutures.onComplete { this.deferOutstandingDecrement() }
            // This is logically redundant, but profiling indicates that calling into
            // updateKeyPathReferenceCountsForChannel is surprisingly expensive!
            if (removedPathIncrements.size > 0) {
                respondFutures.onComplete {
                    this.counters.updateKeyPathReferenceCountsForChannel(channel, removedPathIncrements)
                }
            }

            responders.clear()
            responsesByReturnAddress.clear()
        }
    }

    private fun respondToDataTimed(reportData: ReportData) {
        val startTime = System.nanoTime()
        try {
            return this.respondToData(reportData)
        } finally {
            this.respondToDataCount.addAndGet(1)
            this.respondToDataTotalTime.addAndGet(System.nanoTime() - startTime)
        }
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

        val verticleOffsetString = this.verticleOffset.toString()

        this.cacheKeyGauge = Gauge
            .builder(idempotencyKeyCacheSizeName) { this.idempotencyKeys.size }
            .description(idempotencyKeyCacheSizeDescription)
            .tag("router", verticleOffsetString)
            .register(this.metrics)
        this.queryCountGauge = Gauge
            .builder(queryCountName) { this.counters.getQueryCountForThread(this.verticleOffset) }
            .description(queryCountDescription)
            .tag("router", verticleOffsetString)
            .register(this.metrics)
        this.functionTimer = FunctionTimer
            .builder(
                routerTimerName,
                this,
                { this.respondToDataCount.get() },
                { this.respondToDataTotalTime.get().toDouble() },
                TimeUnit.NANOSECONDS,
            )
            .description(routerTimerDescription)
            .tag("router", verticleOffsetString)
            .register(this.metrics)

        this.idempotencyExpirationMS = this.config.idempotencyExpirationMS
        this.maxQueryTerms = this.config.maxQueryTerms
        this.maxOutstandingDecrements = this.config.maxOutstandingEventsPerRouterThread.toLong()

        val eventBus = this.vertx.eventBus()
        val queryAddress = addressForRouterServer(this.config, this.verticleOffset)
        this.queryHandler = eventBus.localConsumer(queryAddress) { message ->
            when (val body = message.body()) {
                is RegisterQueryRequest -> this.runCoroutineAndReply(message) { this.registerQuery(body) }
                is UnregisterQueryRequest -> this.runCoroutineAndReply(message) { this.unregisterQuery(body) }
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

        val idempotencyCleanupTimerID = this.idempotencyCleanupTimerID
        if (idempotencyCleanupTimerID != null) {
            this.vertx.cancelTimer(idempotencyCleanupTimerID)
        }

        val reportDecrementTimerID = this.reportDecrementTimerID
        if (reportDecrementTimerID != null) {
            this.vertx.cancelTimer(reportDecrementTimerID)
        }

        this.reportOutstandingDecrements()

        val functionTimer = this.functionTimer
        if (functionTimer != null) {
            this.metrics.remove(functionTimer)
        }

        val queryCountGauge = this.queryCountGauge
        if (queryCountGauge != null) {
            this.metrics.remove(queryCountGauge)
        }

        val cacheKeyGauge = this.cacheKeyGauge
        if (cacheKeyGauge != null) {
            this.metrics.remove(cacheKeyGauge)
        }

        super.stop()
    }
}

class OnlyPathsWithReferencesFilterContext(
    private val referenceCounts: KeyPathReferenceCount
): KeyValueFilterContext {
    override fun keysForObject(obj: JsonObject): Iterable<String> {
        val subKeys = this.referenceCounts.subKeys
        return if (subKeys.size < obj.size()) {
            subKeys.keys
        } else {
            obj.fieldNames()
        }
    }

    override fun contextForObject(key: String): KeyValueFilterContext {
        val subKeys = this.referenceCounts.subKeys
        val refCountForKey = subKeys[key] ?: return AcceptNoneKeyValueFilterContext()
        return if (subKeys.size > 0) {
            OnlyPathsWithReferencesFilterContext(refCountForKey)
        } else {
            AcceptNoneKeyValueFilterContext()
        }
    }

    override fun offsetsForArray(arr: JsonArray): Iterable<Int> {
        val intSubKeys = this.referenceCounts.intSubKeys
        val arrSize = arr.size()
        return if (arrSize < intSubKeys.size) {
            (0 until arrSize).filter { intSubKeys.containsKey(it) }
        } else {
            intSubKeys.keys
        }
    }

    override fun contextForArray(offset: Int): KeyValueFilterContext {
        val stringForOffset = this.referenceCounts.intSubKeys[offset] ?: return AcceptNoneKeyValueFilterContext()
        return this.contextForObject(stringForOffset)
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
    config: GlobalConfig,
    private val counters: GlobalCounterContext,
    private val metrics: MeterRegistry,
    verticleOffset: Int,
): ServerVerticle(config, verticleOffset) {
    private val handleUnpackDataRequestCount = AtomicLong()
    private val handleUnpackDataRequestTotalTime = AtomicLong()

    private var maxJsonParsingRecursion = 16
    private var requestHandler: MessageConsumer<UnpackDataRequestList>? = null
    private var functionTimer: FunctionTimer? = null

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
            else -> OnlyPathsWithReferencesFilterContext(referenceCounts)
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
        val startTime = System.nanoTime()
        try {
            return this.handleUnpackDataRequest(req)
        } finally {
            this.handleUnpackDataRequestCount.addAndGet(1)
            this.handleUnpackDataRequestTotalTime.addAndGet(System.nanoTime() - startTime)
        }
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

        this.functionTimer = FunctionTimer
            .builder(
                translationTimerName,
                this,
                { this.handleUnpackDataRequestCount.get() },
                { this.handleUnpackDataRequestTotalTime.get().toDouble() },
                TimeUnit.NANOSECONDS,
            )
            .description(translationTimerDescription)
            .tag("translator", this.verticleOffset.toString())
            .register(this.metrics)

        val eventBus = this.vertx.eventBus()
        this.maxJsonParsingRecursion = this.config.maxJsonParsingRecursion

        this.requestHandler = eventBus.localConsumer(addressForTranslatorServer(this.config, this.verticleOffset)) {
            message -> this.runBlockingAndReply(message) { this.handleUnpackDataRequestList(it) }
        }
    }

    override fun stop() {
        this.requestHandler?.unregister()

        val functionTimer = this.functionTimer
        if (functionTimer != null) {
            this.metrics.remove(functionTimer)
        }

        super.stop()
    }
}

suspend fun registerQueryServer(
    vertx: Vertx,
    config: GlobalConfig,
    counters: GlobalCounterContext,
    metrics: MeterRegistry,
): Set<String> {
    val opts = DeploymentOptions().setWorker(true)
    val futures = mutableListOf<Future<String>>()
    val queryThreads = config.routerThreads
    val idempotencyKeyCacheSize = config.maxIdempotencyKeys
    for (i in 0 until queryThreads) {
        futures.add(
            vertx.deployVerticle(QueryRouterVerticle(config, counters, metrics, i, idempotencyKeyCacheSize), opts)
        )
    }
    val translatorThreads = config.translatorThreads
    for (i in 0 until translatorThreads) {
        futures.add(vertx.deployVerticle(JsonToQueryableTranslatorVerticle(config, counters, metrics, i), opts))
    }

    val deploymentIDs = mutableSetOf<String>()
    for (future in futures) {
        val deploymentID = future.await()
        deploymentIDs.add(deploymentID)
    }

    return deploymentIDs
}