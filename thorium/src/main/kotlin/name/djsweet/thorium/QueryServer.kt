package name.djsweet.thorium

import io.vertx.core.AbstractVerticle
import io.vertx.core.DeploymentOptions
import io.vertx.core.Future
import io.vertx.core.eventbus.Message
import io.vertx.core.Vertx
import io.vertx.core.eventbus.MessageConsumer
import io.vertx.kotlin.core.json.jsonObjectOf
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.vertxFuture
import name.djsweet.query.tree.IdentitySet
import name.djsweet.query.tree.QPTrie
import name.djsweet.query.tree.QueryPath
import name.djsweet.query.tree.QuerySetTree
import java.util.*
import kotlin.collections.ArrayList
import kotlin.coroutines.suspendCoroutine

fun nowMS(): Long {
    return System.nanoTime() / 1_000_000
}

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
    protected fun<T, U> runAndReply(message: Message<T>, handler: suspend (T) -> U) {
        val response = vertxFuture { handler(message.body()) }
        response.onComplete {
            if (it.succeeded()) {
                message.reply(it.result())
            } else {
                message.fail(500, it.cause().message)
            }
        }
    }
}

private val heapPeek = { heap: ByteArrayKeyedBinaryMinHeap<ByteArray> -> heap.peek() }
private val nowAsByteArray = { convertLongToByteArray(nowMS()) }

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
        val waitFor = cleanupAt - nowMS()
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

data class ChannelInfo(
    val queryTree: QuerySetTree<QueryResponderSpec>,
    val queriesByClientID: QPTrie<Pair<QueryResponderSpec, QueryPath>>,
    override val idempotencyKeys: QPTrie<ByteArray>,
    override val idempotencyKeysByExpiration: QPTrie<IdentitySet<ByteArray>>,
): IdempotencyManager<ByteArray, ChannelInfo>() {
    constructor(): this(QuerySetTree(), QPTrie(), QPTrie(), QPTrie())

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

    fun registerQuery(query: FullQuery, clientID: String, respondTo: String): ChannelInfo {
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
        return this.copy(
            queryTree=newQueryTree,
            queriesByClientID=queriesByClientID.put(clientIDBytes, responderSpec to newPath),
        )
    }

    fun unregisterQuery(clientID: String): ChannelInfo {
        val (queryTree, queriesByClientID) = this
        val clientIDBytes = convertStringToByteArray(clientID)
        val priorQuery = queriesByClientID.get(clientIDBytes) ?: return this
        val (responder, path) = priorQuery
        return this.copy(
            queryTree=queryTree.removeElementByPath(path, responder),
            queriesByClientID=queriesByClientID.remove(clientIDBytes)
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

class QueryResponderVerticle(verticleOffset: Int): ServerVerticleWithIdempotency(verticleOffset) {
    private var channels = QPTrie<ChannelInfo>()
    private val responders = ArrayList<QueryResponderSpec>()
    private val respondFutures = ArrayList<Triple<Future<Message<Any>>, String, String>>()
    private val arrayResponderReferenceCounts = mutableMapOf<QueryResponderSpec, Long>()
    private val arrayResponderInsertionPairs = ArrayList<Pair<ByteArray, ByteArray>>()

    private var byteBudget = 0
    private var maxQueryTerms = 0
    private var queryCount = 0
    private var queryHandler: MessageConsumer<Any>? = null
    private var dataHandler: MessageConsumer<ReportData>? = null

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
                val possiblyFullQuery: HttpProtocolErrorOr<FullQuery>
                try {
                    possiblyFullQuery = convertQueryStringToFullQuery(
                        req.queryParams,
                        this.maxQueryTerms,
                        this.byteBudget
                    )
                } catch (x: StackOverflowError) {
                    // FIXME: We should tell all the other query servers that they need to dump their queries!
                    this.byteBudget = reestablishByteBudget(this.vertx.sharedData())
                    continue
                }
                possiblyFullQuery
                    .whenError { cont.resumeWith(Result.success(HttpProtocolErrorOrJson.ofError(it))) }
                    .whenSuccess { query ->
                        val channelBytes = convertStringToByteArray(req.channel)
                        // FIXME: StackOverflowErrors can come from here too
                        this.channels = this.channels.update(channelBytes) {
                            val basis = (it ?: ChannelInfo())
                            val result = basis.registerQuery(query, req.clientID, req.returnAddress)
                            this.queryCount -= basis.queryTree.size.toInt()
                            this.queryCount += basis.queryTree.size.toInt()
                            setCurrentQueryCount(this.vertx.sharedData(), this.verticleOffset, this.queryCount)
                            result
                        }
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

    private fun unregisterQuery(req: UnregisterQueryRequest): HttpProtocolErrorOrJson {
        // FIXME: This might result in a StackOverflowError
        val channelBytes = convertStringToByteArray(req.channel)
        val channel = this.channels.get(channelBytes) ?: return HttpProtocolErrorOrJson.ofError(
            HttpProtocolError(404, jsonObjectOf(
                "code" to "missing-channel",
                "channel" to req.channel
            ))
        )

        val updatedChannel = channel.unregisterQuery(req.clientID)
        return if (updatedChannel === channel) {
            HttpProtocolErrorOrJson.ofError(
                HttpProtocolError(404, jsonObjectOf(
                    "code" to "missing-client-id",
                    "channel" to req.channel,
                    "client-id" to req.clientID
                ))
            )
        } else {
            this.channels = this.channels.put(channelBytes, updatedChannel)
            this.queryCount -= (channel.queryTree.size - updatedChannel.queryTree.size).toInt()
            setCurrentQueryCount(this.vertx.sharedData(), this.verticleOffset, this.queryCount)
            HttpProtocolErrorOrJson.ofSuccess(jsonObjectOf(
                "channel" to req.channel,
                "client-id" to req.clientID
            ))
        }
    }

    private suspend fun respondToData(response: ReportData) {
        val eventBus = this.vertx.eventBus()
        val (channel, idempotencyKey, queryableScalarData, queryableArrayData) = response
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

        val idempotencyKeyExpiration = nowMS() + this.idempotencyExpirationMS
        val idempotencyKeyExpirationBytes = convertLongToByteArray(idempotencyKeyExpiration)
        var newChannel = false
        var updatedChannelInfo: ChannelInfo? = null
        val priorRemovalScheduleSize = this.idempotencyKeyRemovalSchedule.size
        // FIXME: This operation might cause a StackOverflow itself
        this.channels = this.channels.update(channelBytes) {
            val updateTarget = (it ?: ChannelInfo())
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

        val responders = this.responders
        val arrayResponderReferenceCounts = this.arrayResponderReferenceCounts
        val arrayResponderInsertionPairs = this.arrayResponderInsertionPairs
        val respondFutures = this.respondFutures
        queryTree.visitByData(queryableScalarData.trie) { _, responder ->
            responders.add(responder)
        }
        var arrayResponderQueries = QPTrie<QPTrie<IdentitySet<QueryResponderSpec>>>()
        for (responder in responders) {
            val (query, respondTo, clientID, _, arrayContainsCount) = responder
            // FIXME: notEqualsMatchesData could result in a StackOverflowError; trap and handle this
            if (!query.notEqualsMatchesData(queryableScalarData.trie)) {
                continue
            }
            // If we have any arrayContains we have to inspect, save this for another phase
            if (arrayContainsCount > 0) {
                arrayResponderReferenceCounts[responder] = arrayContainsCount
                // FIXME: This could result in a StackOverflowError; trap and handle this
                try {
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
                } finally {
                    arrayResponderInsertionPairs.clear()
                }
                continue
            }
            respondFutures.add(Triple(eventBus.request(respondTo, response, localRequestOptions), respondTo, clientID))
        }
        responders.clear()

        // FIXME: This could be a source of StackOverflowError; trap and handle this
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
                            val (_, respondTo, clientID) = responder
                            respondFutures.add(
                                Triple(eventBus.request(respondTo, response, localRequestOptions), respondTo, clientID)
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
        arrayResponderReferenceCounts.clear()
        // Backpressure: we don't return until all clients accept the message.
        try {
            for ((respondFuture, respondTo, clientID) in respondFutures) {
                try {
                    respondFuture.await()
                } catch (e: Exception) {
                    eventBus.publish(respondTo, UnregisterQueryRequest(channel, clientID))
                }
            }
        } finally {
            respondFutures.clear()
            decrementOutstandingQueryCountAsync(this.vertx.sharedData(), 1)
        }
    }

    override fun start() {
        super.start()

        this.byteBudget = getByteBudget(this.vertx.sharedData())
        this.maxQueryTerms = getMaxQueryTerms(this.vertx.sharedData())

        val eventBus = this.vertx.eventBus()
        val sharedData = this.vertx.sharedData()
        val queryAddress = addressForQueryServerQuery(sharedData, this.verticleOffset)
        val dataAddress = addressForQueryServerData(sharedData, this.verticleOffset)
        this.queryHandler = eventBus.localConsumer(queryAddress) { message -> this.runAndReply(message) {
            when (it) {
                is RegisterQueryRequest -> this.registerQuery(it)
                is UnregisterQueryRequest -> this.unregisterQuery(it)
                else -> HttpProtocolErrorOrJson.ofError(HttpProtocolError(500, jsonObjectOf(
                    "code" to "invalid-request"
                )))
            }
        } }
        this.dataHandler = eventBus.localConsumer(dataAddress) { message ->
            this.runAndReply(message) { this.respondToData(it) }
        }
    }

    override fun stop() {
        this.dataHandler?.unregister()
        this.queryHandler?.unregister()
        super.stop()
    }
}


val baseJsonResponseForBadJsonString = jsonObjectOf("code" to "failed-json-stringify")
val baseJsonResponseForOversizedChannelInfoIdempotencyKey = jsonObjectOf(
    "code" to "channel-idempotency-too-large"
)
val baseJsonResponseForStackOverflowData = jsonObjectOf(
    "code" to "exhausted-byte-budget"
)

// Translation doesn't implement any form of idempotent caching, and instead blindly translates
// every request sent to it. At a high enough update frequency, this is absolutely the right choice:
// we would be growing our working set of "cached data" up to millions of saved entries rather quickly,
// just to save on the "cost" of reprocessing a tiny handful.
class JsonToQueryableTranslatorVerticle(verticleOffset: Int): ServerVerticle(verticleOffset) {
    private var byteBudget = 0
    private var maxJsonParsingRecursion = 16
    private var requestHandler: MessageConsumer<UnpackDataRequest>? = null

    private fun handleUnpackDataRequest(req: UnpackDataRequest): HttpProtocolErrorOrReportData {
        val (channel, idempotencyKey, data) = req
        val channelBytes = convertStringToByteArray(channel)
        val byteBudget = this.byteBudget
        val maxJsonParsingRecursion = this.maxJsonParsingRecursion

        val jsonString: String
        try {
            jsonString = data.toString()
        } catch (e: Exception) {
            // Something has gone horribly wrong trying to convert this JSON to a string.
            // We can't actually do anything with this in terms of queries.
            return HttpProtocolErrorOrReportData.ofError(
                HttpProtocolError(
                    400,
                    baseJsonResponseForBadJsonString.copy().put("eventID", idempotencyKey)
                )
            )
        }
        try {
            val idempotencyKeyBytes = convertStringToByteArray(idempotencyKey)
            if (idempotencyKeyBytes.size + channelBytes.size > byteBudget) {
                return HttpProtocolErrorOrReportData.ofError(HttpProtocolError(
                    413,
                    baseJsonResponseForOversizedChannelInfoIdempotencyKey.copy().put(
                        "maxByteSize", byteBudget
                    ).put(
                        "eventID", idempotencyKey
                    )
                ))
            }
            val (scalars, arrays) = encodeJsonToQueryableData(data, byteBudget, maxJsonParsingRecursion)
            return HttpProtocolErrorOrReportData.ofSuccess(ReportData(
                channel=channel,
                idempotencyKey=idempotencyKey,
                queryableScalarData=scalars,
                queryableArrayData=arrays,
                actualData=jsonString
            ))
        } catch (e: StackOverflowError) {
            // FIXME: Signal all consumers of byteBudget that this happened!
            this.byteBudget = reestablishByteBudget(this.vertx.sharedData())
            return HttpProtocolErrorOrReportData.ofError(HttpProtocolError(
                507,
                baseJsonResponseForStackOverflowData.copy().put(
                    "maxByteSize", byteBudget
                )
            ))
        }
    }

    override fun start() {
        super.start()

        val vertx = this.vertx
        val sharedData = vertx.sharedData()
        val eventBus = vertx.eventBus()
        this.byteBudget = getByteBudget(sharedData)
        this.maxJsonParsingRecursion = getMaxJsonParsingRecursion(sharedData)

        this.requestHandler = eventBus.localConsumer(addressForTranslatorServer(sharedData, this.verticleOffset)) {
            message -> this.runAndReply(message) { this.handleUnpackDataRequest(it) }
        }
    }

    override fun stop() {
        this.requestHandler?.unregister()
        super.stop()
    }
}

suspend fun registerQueryServer(
    vertx: Vertx,
    queryVerticleOffset: Int,
    queryVerticles: Int,
    serverVerticleOffset: Int,
    serverVerticles: Int
): Set<String> {
    val opts = DeploymentOptions().setWorker(true)
    val futures = mutableListOf<Future<String>>()
    val lastQueryVerticleOffset = queryVerticleOffset + queryVerticles
    for (i in queryVerticleOffset until lastQueryVerticleOffset) {
        futures.add(vertx.deployVerticle(QueryResponderVerticle(i), opts))
    }
    val lastServerVerticleOffset = serverVerticleOffset + serverVerticles
    for (i in serverVerticleOffset until lastServerVerticleOffset) {
        futures.add(vertx.deployVerticle(JsonToQueryableTranslatorVerticle(i), opts))
    }

    val deploymentIDs = mutableSetOf<String>()
    for (future in futures) {
        val deploymentID = future.await()
        deploymentIDs.add(deploymentID)
    }

    return deploymentIDs
}