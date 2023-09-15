package name.djsweet.thorium.servers

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
import name.djsweet.thorium.convertByteArrayToLong
import name.djsweet.thorium.convertLongToByteArray
import name.djsweet.thorium.convertStringToByteArray
import name.djsweet.thorium.reestablishByteBudget
import java.util.*
import kotlin.collections.ArrayList
import kotlin.coroutines.suspendCoroutine

data class QueryResponderSpec(
    val query: FullQuery,
    val respondTo: String,
    val clientID: String,
    val addedAt: Long,
    val arrayContainsCounter: Long,
)

interface ChannelIdempotencyMapping {
    fun minIdempotencyKey(): ByteArray?
}

abstract class IdempotencyManager<TValues, TSelf>: ChannelIdempotencyMapping {
    abstract val idempotencyKeys: QPTrie<TValues>
    abstract val idempotencyKeysByExpiration: QPTrie<IdentitySet<ByteArray>>

    protected abstract fun self(): TSelf

    protected abstract fun withNewIdempotencyKeys(
        idempotencyKeys: QPTrie<TValues>,
        idempotencyKeysByExpiration: QPTrie<IdentitySet<ByteArray>>
    ): TSelf

    override fun minIdempotencyKey(): ByteArray? {
        return this.idempotencyKeysByExpiration.minKeyValueUnsafeSharedKey()?.key
    }

    fun hasIdempotencyKeys(): Boolean {
        return this.idempotencyKeys.size > 0L
    }

    fun removeMinimumIdempotencyKeys(): TSelf {
        var newIdempotencyKeys = this.idempotencyKeys
        val idempotencyKeysByExpiration = this.idempotencyKeysByExpiration
        val minExpirationKey = idempotencyKeysByExpiration.minKeyValueUnsafeSharedKey()?.key ?: return this.self()
        val minKeys = idempotencyKeysByExpiration.get(minExpirationKey) ?: return this.self()
        minKeys.visitAll {
            newIdempotencyKeys = newIdempotencyKeys.remove(it)
        }
        return if (newIdempotencyKeys === this.idempotencyKeys) {
            this.self()
        } else {
            this.withNewIdempotencyKeys(
                 newIdempotencyKeys,
                 idempotencyKeysByExpiration.remove(minExpirationKey)
            )
        }
    }

    fun removeIdempotencyKeysBelowOrAt(removeTarget: ByteArray): TSelf {
        var newIdempotencyKeys = this.idempotencyKeys
        var newIdempotencyKeysByExpiration = this.idempotencyKeysByExpiration
        while (true) {
            val minExpirationKey = newIdempotencyKeysByExpiration.minKeyValueUnsafeSharedKey()?.key ?: break
            if (Arrays.compareUnsigned(minExpirationKey, removeTarget) > 0) {
                break
            }
            val minKeys = newIdempotencyKeysByExpiration.get(minExpirationKey) ?: break
            minKeys.visitAll {
                newIdempotencyKeys = newIdempotencyKeys.remove(it)
            }
            newIdempotencyKeysByExpiration = newIdempotencyKeysByExpiration.remove(minExpirationKey)

        }
        return if (newIdempotencyKeys === this.idempotencyKeys) {
            this.self()
        } else {
            this.withNewIdempotencyKeys(
                newIdempotencyKeys,
                newIdempotencyKeysByExpiration
            )
        }
    }
}

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

private val heapPeek = { heap: ByteArrayKeyedBinaryMinHeap<ByteArray> -> heap.peek() }
private val nowAsByteArray = { convertLongToByteArray(monotonicNowMS()) }

private typealias UpdateChannelFunc = (targetChannel: ByteArray) -> Pair<ChannelIdempotencyMapping, Long>?

abstract class ServerVerticleWithIdempotency(verticleOffset: Int): ServerVerticle(verticleOffset) {
    protected var currentIdempotencyKeys = 0

    protected val idempotencyKeyRemovalSchedule = ByteArrayKeyedBinaryMinHeap<ByteArray>()
    private var idempotencyCleanupTimerID: Long? = null

    protected var idempotencyExpirationMS = 0
    private var maximumIdempotencyKeys = 0

    private fun handleIdempotencyCleanup(
        current: (ByteArrayKeyedBinaryMinHeap<ByteArray>) -> Pair<ByteArray, ByteArray>?,
        updateChannelMappingReturningNext: UpdateChannelFunc
    ) {
        val removalSchedule = this.idempotencyKeyRemovalSchedule
        while (removalSchedule.size > 0) {
            val cur = current(removalSchedule) ?: break
            val (_, targetChannel) = cur
            val updateResult = updateChannelMappingReturningNext(targetChannel)
            if (updateResult == null) {
                removalSchedule.pop()
                continue
            }

            val (updatedChannelMapping, keyDelta) = updateResult
            this.currentIdempotencyKeys += keyDelta.toInt()
            val minKeyForUpdate = updatedChannelMapping.minIdempotencyKey()
            if (minKeyForUpdate == null) {
                removalSchedule.pop()
                continue
            }
            removalSchedule.popPush(minKeyForUpdate to targetChannel)
        }
    }

    protected fun handleIdempotencyCleanupForMaximum(
        updateChannelMappingReturningNext: UpdateChannelFunc
    ) {
        while (this.currentIdempotencyKeys >= this.maximumIdempotencyKeys) {
            this.handleIdempotencyCleanup(heapPeek, updateChannelMappingReturningNext)
        }
    }

    protected abstract fun updateForTimerCleanup(
        targetChannel: ByteArray,
        until: ByteArray
    ): Pair<ChannelIdempotencyMapping, Long>?

    private fun handleIdempotencyCleanupOnTimer() {
        this.handleIdempotencyCleanup(
            {
                val bestRemovalItem = it.peek()
                val rightNow = nowAsByteArray()
                if (bestRemovalItem == null || Arrays.compareUnsigned(bestRemovalItem.first, rightNow) > 0) {
                    null
                } else {
                    bestRemovalItem
                }
            },
            {
                this.updateForTimerCleanup(it, nowAsByteArray())
            }
        )
        this.setupIdempotencyCleanupTimer()
    }

    protected fun setupIdempotencyCleanupTimer() {
        val minItem = this.idempotencyKeyRemovalSchedule.peek() ?: return
        val cleanupAt = convertByteArrayToLong(minItem.first)
        val waitFor = cleanupAt - monotonicNowMS()
        val currentTimerID = this.idempotencyCleanupTimerID
        if (currentTimerID != null) {
            this.vertx.cancelTimer(currentTimerID)
        }
        // We're going to sleep at least 1 millisecond between attempts, as a guard against CPU exhaustion
        // in the event that this removal process is buggy.
        this.idempotencyCleanupTimerID = this.vertx.setTimer(waitFor.coerceAtLeast(1)) {
            this.handleIdempotencyCleanupOnTimer()
        }
    }

    override fun start() {
        super.start()
        this.idempotencyExpirationMS = getIdempotencyExpirationMS(this.vertx.sharedData())
        this.maximumIdempotencyKeys = getMaximumIdempotencyKeys(this.vertx.sharedData())
    }

    override fun stop() {
        val timerID = this.idempotencyCleanupTimerID
        if (timerID != null) {
            this.vertx.cancelTimer(timerID)
        }
        super.stop()
    }
}

data class ChannelInfoPathChangeResult(
    val newChannelInfo: ChannelInfo,
    val involvedPath: QueryPath?
)

data class ChannelInfo(
    val queryTree: QuerySetTree<QueryResponderSpec>,
    val queriesByClientID: QPTrie<Pair<QueryResponderSpec, QueryPath>>,
    override val idempotencyKeys: QPTrie<ByteArray>,
    override val idempotencyKeysByExpiration: QPTrie<IdentitySet<ByteArray>>,
    val channel: String,
): IdempotencyManager<ByteArray, ChannelInfo>() {
    constructor(channel: String): this(QuerySetTree(), QPTrie(), QPTrie(), QPTrie(), channel)

    override fun self(): ChannelInfo {
        return this
    }

    override fun withNewIdempotencyKeys(
        idempotencyKeys: QPTrie<ByteArray>,
        idempotencyKeysByExpiration: QPTrie<IdentitySet<ByteArray>>
    ): ChannelInfo {
        return this.copy(
            idempotencyKeys=idempotencyKeys,
            idempotencyKeysByExpiration=idempotencyKeysByExpiration
        )
    }

    fun registerQuery(query: FullQuery, clientID: String, respondTo: String): ChannelInfoPathChangeResult {
        val clientIDBytes = convertStringToByteArray(clientID)
        val (queryTree, queriesByClientID) = this
        val priorQuery = queriesByClientID.get(clientIDBytes)
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
        return ChannelInfoPathChangeResult(
            newChannelInfo = this.copy(
                queryTree=newQueryTree,
                queriesByClientID=queriesByClientID.put(clientIDBytes, responderSpec to newPath),
            ),
            involvedPath = newPath
        )
    }

    fun unregisterQuery(clientID: String): ChannelInfoPathChangeResult {
        val (queryTree, queriesByClientID) = this
        val clientIDBytes = convertStringToByteArray(clientID)
        val priorQuery = queriesByClientID.get(clientIDBytes) ?: return ChannelInfoPathChangeResult(
            newChannelInfo = this,
            involvedPath = null
        )
        val (responder, path) = priorQuery
        return ChannelInfoPathChangeResult(
            newChannelInfo = this.copy(
                queryTree=queryTree.removeElementByPath(path, responder),
                queriesByClientID=queriesByClientID.remove(clientIDBytes)
            ),
            involvedPath = path
        )
    }

    fun addIdempotencyKey(idempotencyKey: ByteArray, expiresAt: ByteArray): ChannelInfo {
        return this.withNewIdempotencyKeys(
            idempotencyKeys = this.idempotencyKeys.put(idempotencyKey, expiresAt),
            idempotencyKeysByExpiration = this.idempotencyKeysByExpiration.update(expiresAt) {
                (it ?: IdentitySet()).add(idempotencyKey)
            }
        )
    }
}

private val ciRemoveMin = { ci: ChannelInfo -> ci.removeMinimumIdempotencyKeys() }

class QueryRouterVerticle(
    private val counters: GlobalCounterContext,
    metrics: MeterRegistry,
    verticleOffset: Int,
): ServerVerticleWithIdempotency(verticleOffset) {
    private val routerTimer = Timer.builder(routerMetricName)
        .description(routerMetricDescription)
        .tag("worker", verticleOffset.toString())
        .register(metrics)

    private var channels = QPTrie<ChannelInfo>()
    private val responders = ArrayList<QueryResponderSpec>()
    private val arrayResponderReferenceCounts = mutableMapOf<QueryResponderSpec, Long>()
    private val arrayResponderInsertionPairs = ArrayList<Pair<ByteArray, ByteArray>>()

    private var maxQueryTerms = 0
    private var queryHandler: MessageConsumer<Any>? = null
    private var dataHandler: MessageConsumer<ReportDataList>? = null

    override fun resetForNewByteBudget() {
        val eventBus = this.vertx.eventBus()
        for ((_, channelInfo) in this.channels) {
            val channel = channelInfo.channel
            for ((_, responderSpecToPath) in channelInfo.queriesByClientID) {
                val (responderSpec) = responderSpecToPath
                val (_, respondTo, clientID) = responderSpec
                eventBus.publish(respondTo, UnregisterQueryRequest(channel, clientID))
            }
        }
        this.channels = QPTrie()
        this.idempotencyKeyRemovalSchedule.clear()
        this.counters.resetQueryCountForThread(this.verticleOffset)
    }

    private fun updateChannelsWithRemoval(
        targetChannel: ByteArray,
        remove: (ChannelInfo) -> ChannelInfo
    ): Pair<ChannelIdempotencyMapping, Long>? {
        var keyDelta = 0L
        var updatedChannelInfo: ChannelInfo? = null
        this.channels = this.channels.update(targetChannel) {
            if (it == null) {
                null
            } else {
                val priorKeySize = it.idempotencyKeys.size
                val nextChannelInfo = remove(it)
                updatedChannelInfo = nextChannelInfo
                keyDelta = nextChannelInfo.idempotencyKeys.size - priorKeySize
                nextChannelInfo
            }
        }
        return if (updatedChannelInfo == null) {
            null
        } else {
            Pair(updatedChannelInfo!!, keyDelta)
        }
    }

    override fun updateForTimerCleanup(
        targetChannel: ByteArray,
        until: ByteArray
    ): Pair<ChannelIdempotencyMapping, Long>? {
        return this.updateChannelsWithRemoval(targetChannel) {
            it.removeIdempotencyKeysBelowOrAt(until)
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
                        val channelBytes = convertStringToByteArray(channel)
                        this.channels = this.channels.update(channelBytes) {
                            val basis = (it ?: ChannelInfo(channel))
                            val (newChannelInfo) = basis.registerQuery(query, req.clientID, req.returnAddress)
                            this.counters.alterQueryCountForThread(
                                this.verticleOffset,
                                newChannelInfo.queryTree.size - basis.queryTree.size
                            )
                            newChannelInfo
                        }
                        this.counters.updateKeyPathReferenceCountsForChannel(channel, increments)
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
        channelString: String,
        clientID: String,
        withRemovedPath: (QueryPath) -> Unit
    ): HttpProtocolErrorOrJson {
        val channelBytes = convertStringToByteArray(channelString)
        val channel = this.channels.get(channelBytes) ?: return HttpProtocolErrorOrJson.ofError(
            HttpProtocolError(
                404, jsonObjectOf(
                    "code" to "missing-channel",
                    "channel" to channelString
                )
            )
        )

        val (updatedChannel, involvedPath) = channel.unregisterQuery(clientID)
        if (involvedPath != null) {
            withRemovedPath(involvedPath)
        }
        return if (updatedChannel === channel) {
            HttpProtocolErrorOrJson.ofError(
                HttpProtocolError(
                    404, jsonObjectOf(
                        "code" to "missing-client-id",
                        "channel" to channelString,
                        "clientID" to clientID
                    )
                )
            )
        } else {
            this.channels = this.channels.put(channelBytes, updatedChannel)
            this.counters.alterQueryCountForThread(
                this.verticleOffset,
                updatedChannel.queryTree.size - channel.queryTree.size
            )
            HttpProtocolErrorOrJson.ofSuccess(
                jsonObjectOf(
                    "channel" to channelString,
                    "clientID" to clientID
                )
            )
        }
    }

    private fun unregisterQuery(req: UnregisterQueryRequest): HttpProtocolErrorOrJson {
        return this.unregisterQuery(req.channel, req.clientID) { queryPath ->
            val decrements = mutableListOf<Pair<List<String>, Int>>()
            for (pathComponent in queryPath.keys()) {
                val decoder = Radix64LowLevelDecoder(pathComponent)
                val sublist = mutableListOf<String>()
                while (decoder.withString { sublist.add(it) }) {
                    // This seems silly, but withString already causes a break on `false`.
                }
                decrements.add(Pair(sublist, -1))
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
                for (pathComponent in queryPath.keys()) {
                    val decoder = Radix64LowLevelDecoder(pathComponent)
                    val sublist = mutableListOf<String>()
                    while (decoder.withString { sublist.add(it) }) {
                        // This seems silly, but withString already causes a break on `false`.
                    }
                    removedPathIncrements.add(Pair(sublist, -1))
                }
            }
            eventBus.publish(respondTo, UnregisterQueryRequest(data.channel, clientID))
        }
        return prior.eventually { current }
    }

    private fun respondToData(response: ReportData) {
        val responders = this.responders
        val arrayResponderReferenceCounts = this.arrayResponderReferenceCounts
        val arrayResponderInsertionPairs = this.arrayResponderInsertionPairs
        var respondFutures = Future.succeededFuture<Message<Any>>()
        val removedPathIncrements = ArrayList<Pair<List<String>, Int>>()
        val (channel, idempotencyKey, queryableScalarData, queryableArrayData) = response

        try {
            val eventBus = this.vertx.eventBus()
            val channelBytes = convertStringToByteArray(channel)
            val respondingChannel = this.channels.get(channelBytes) ?: return
            val (queryTree, _, idempotencyKeys) = respondingChannel
            val idempotencyKeyBytes = convertStringToByteArray(idempotencyKey)
            if (idempotencyKeys.get(idempotencyKeyBytes) != null) {
                return
            }

            this.handleIdempotencyCleanupForMaximum {
                this.updateChannelsWithRemoval(it, ciRemoveMin)
            }

            val idempotencyKeyExpiration = monotonicNowMS() + this.idempotencyExpirationMS
            val idempotencyKeyExpirationBytes = convertLongToByteArray(idempotencyKeyExpiration)
            var newChannel = false
            var updatedChannelInfo: ChannelInfo? = null
            val priorRemovalScheduleSize = this.idempotencyKeyRemovalSchedule.size
            this.channels = this.channels.update(channelBytes) {
                val updateTarget = (it ?: ChannelInfo(channel))
                newChannel = !updateTarget.hasIdempotencyKeys()
                updatedChannelInfo = updateTarget.addIdempotencyKey(
                    idempotencyKeyBytes,
                    idempotencyKeyExpirationBytes
                )
                updatedChannelInfo
            }
            if (updatedChannelInfo != null && newChannel) {
                val minExpiration = updatedChannelInfo!!.minIdempotencyKey()
                if (minExpiration != null) {
                    this.currentIdempotencyKeys += 1
                    this.idempotencyKeyRemovalSchedule.push(minExpiration to channelBytes)
                }
            }
            val afterRemovalScheduleSize = this.idempotencyKeyRemovalSchedule.size
            if (priorRemovalScheduleSize == 0 && afterRemovalScheduleSize > 0) {
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
    private val unpackRequestTimer = Timer.builder(translationMetricName)
        .description(translationMetricDescription)
        .tag("worker", verticleOffset.toString())
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
    for (i in queryVerticleOffset until lastQueryVerticleOffset) {
        futures.add(vertx.deployVerticle(QueryRouterVerticle(counters, metrics, i), opts))
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