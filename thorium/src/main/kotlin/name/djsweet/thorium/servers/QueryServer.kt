package name.djsweet.thorium.servers

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
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.json.jsonObjectOf
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.awaitEvent
import io.vertx.kotlin.coroutines.vertxFuture
import kotlinx.collections.immutable.PersistentMap
import kotlinx.coroutines.delay
import name.djsweet.query.tree.IdentitySet
import name.djsweet.query.tree.QPTrie
import name.djsweet.query.tree.QueryPath
import name.djsweet.query.tree.QuerySetTree
import name.djsweet.thorium.*
import name.djsweet.thorium.convertStringToByteArray
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

internal const val thoriumMetaQueryRegistrationType = "name.djsweet.thorium.meta.query.registration"
internal const val thoriumMetaQueryRemovalType = "name.djsweet.thorium.meta.query.removal"
private const val backoffBaseWaitTimeMS = 0.5

data class QueryResponderSpec(
    val query: FullQuery,
    val respondTo: String,
    val clientID: String,
    val addedAt: Long,
    val arrayContainsCounter: Long,
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
    private val responders = ArrayList<QueryResponderSpec>()
    private val arrayResponderReferenceCounts = mutableMapOf<QueryResponderSpec, Long>()
    private val arrayResponderInsertionPairs = ArrayList<Pair<ByteArray, ByteArray>>()

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

    private fun unpackDataRequestForMetaChannel(
        clientID: String,
        eventType: String,
        idPrefix: String,
        data: JsonObject
    ): UnpackDataRequest {
        val config = this.config
        val globalID = "${config.instanceID} $idPrefix$clientID"
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
                        val config = this.config
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
                        sendUnpackDataRequests(
                            this.vertx,
                            config,
                            counters,
                            listOf(this.unpackDataRequestForMetaChannel(
                                clientID = req.clientID,
                                eventType = thoriumMetaQueryRegistrationType,
                                idPrefix = "+",
                                data = jsonObjectOf(
                                    "instanceID" to config.instanceID,
                                    "channel" to req.channel,
                                    "clientID" to req.clientID,
                                    "query" to req.queryString
                                )
                            )),
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
                                    self.unregisterQuery(UnregisterQueryRequest(req.channel, req.clientID))
                                    cont.resumeWithException(it.cause())
                                }
                            } else {
                                cont.resumeWith(Result.success(
                                    HttpProtocolErrorOrJson.ofSuccess(jsonObjectOf(
                                        "channel" to req.channel,
                                        "clientID" to req.clientID
                                    ))
                                ))
                            }
                        }
                    }
                break
            }
        }
    }

    private suspend fun unregisterQuery(
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
            val counters = this.counters
            val config = this.config
            val vertx = this.vertx
            if (channelInfo.isEmpty()) {
                this.channels.remove(channel)
            }
            val unpackRequest = listOf(this.unpackDataRequestForMetaChannel(
                clientID = clientID,
                eventType = thoriumMetaQueryRemovalType,
                idPrefix = "-",
                data = jsonObjectOf(
                    "instanceID" to config.instanceID,
                    "channel" to channel,
                    "clientID" to clientID
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
                        .log()
                }
                val sleepTimeMS = backoffSession.sleepTimeMS()
                delay(Duration.ofNanos((sleepTimeMS * 1_000_000).toLong()).toKotlinDuration())
            }
            counters.alterQueryCountForThread(
                this.verticleOffset,
                channelInfo.queryTree.size - basisQueryTreeSize
            )
            HttpProtocolErrorOrJson.ofSuccess(
                jsonObjectOf(
                    "channel" to channel,
                    "clientID" to clientID
                )
            )
        }
    }

    private suspend fun unregisterQuery(req: UnregisterQueryRequest): HttpProtocolErrorOrJson {
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
            val self = this
            vertxFuture {
                self.unregisterQuery(data.channel, responder.clientID) { queryPath ->
                    extractKeyPathsFromQueryPath(queryPath, -1) {
                        removedPathIncrements.add(it)
                    }
                }
                eventBus.publish(respondTo, UnregisterQueryRequest(data.channel, clientID))
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
                val dataTrie = queryableScalarData.trie
                if (!query.startsWithMatchesAtOffsets(dataTrie)) {
                    continue
                }
                if (!query.notEqualsMatchesData(dataTrie)) {
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
            respondFutures.onComplete { this.deferOutstandingDecrement() }
            // This is logically redundant, but profiling indicates that calling into
            // updateKeyPathReferenceCountsForChannel is surprisingly expensive!
            if (removedPathIncrements.size > 0) {
                respondFutures.onComplete {
                    this.counters.updateKeyPathReferenceCountsForChannel(channel, removedPathIncrements)
                }
            }

            responders.clear()
            arrayResponderInsertionPairs.clear()
            arrayResponderReferenceCounts.clear()
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
    private val referenceCounts: PersistentMap<String, KeyPathReferenceCount>
): KeyValueFilterContext {
    override fun keysForObject(obj: JsonObject): Iterable<String> {
        val referenceCounts = this.referenceCounts
        return if (referenceCounts.size < obj.size()) {
            referenceCounts.keys
        } else {
            obj.fieldNames()
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