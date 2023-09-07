package name.djsweet.thorium

import io.netty.handler.codec.http.QueryStringDecoder
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.Vertx
import io.vertx.core.eventbus.MessageConsumer
import io.vertx.core.http.HttpMethod
import io.vertx.core.http.HttpServer
import io.vertx.core.http.HttpServerRequest
import io.vertx.core.http.HttpServerResponse
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.json.jsonObjectOf
import io.vertx.kotlin.coroutines.await
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import kotlin.math.absoluteValue

fun jsonStatusCodeResponse(req: HttpServerRequest, code: Int): HttpServerResponse {
    return req.response()
        .setStatusCode(code)
        .putHeader("Content-Type", "application/json; charset=utf-8")
        .putHeader("Access-Control-Allow-Origin", "*")
}

const val serverSentCommentTimeout = 30_000.toLong()

val clientSerial: ThreadLocal<Long> = ThreadLocal.withInitial { 0.toLong() }
fun getClientIDFromSerial(): String {
    val current = clientSerial.get()
    clientSerial.set(current + 1)
    return "${Thread.currentThread().id}-$current"
}

fun nowAsString(): String {
    return LocalDateTime.now().atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_ZONED_DATE_TIME)
}

class QueryClientSSEVerticle(
    private val resp: HttpServerResponse,
    private val clientAddress: String,
    private val clientID: String,
    private val channel: String,
    private val serverAddress: String,
): AbstractVerticle() {
    private var timerID: Long? = null
    private var commentFuture: Future<Void>? = null
    private var messageHandler: MessageConsumer<Any>? = null

    private fun writeHeadersIfNecessary() {
        if (this.resp.headWritten()) {
            return
        }
        this.resp
            .setChunked(true)
            .setStatusCode(200)
            .putHeader("Content-Type", "text/event-stream; charset=utf-8")
            .putHeader("Cache-Control", "no-store")
            .putHeader("Connection", "keep-alive")
            .putHeader("Access-Control-Allow-Origin", "*")
            .write(": connected at ${nowAsString()} with client ID $clientID\n\n")
    }

    private fun writeComment(): Future<Void> {
        return this.resp.write(": ping at ${nowAsString()}\n\n")
    }

    private fun setupCommentTimer() {
        val currentTimerID = this.timerID
        val vertx = this.vertx
        if (currentTimerID != null) {
            vertx.cancelTimer(currentTimerID)
            this.timerID = null
        }
        val commentFuture = this.commentFuture
        if (commentFuture != null) {
            commentFuture.onComplete {
                this.timerID = vertx.setTimer(serverSentCommentTimeout) {
                    this.commentFuture = writeComment().onComplete {
                        this.setupCommentTimer()
                    }
                }
            }
        } else {
            this.timerID = vertx.setTimer(serverSentCommentTimeout) {
                this.commentFuture = writeComment().onComplete {
                    this.setupCommentTimer()
                }
            }
        }
    }

    override fun start() {
        val vertx = this.vertx
        val resp = this.resp
        val eventBus = vertx.eventBus()
        this.resp.endHandler {
            eventBus.request<Any>(
                this.serverAddress,
                UnregisterQueryRequest(this.channel, this.clientID),
                localRequestOptions
            ).onComplete {
                vertx.undeploy(this.deploymentID()).onSuccess {
                    decrementGlobalQueryCountReturning(this.vertx.sharedData(), 1L)
                }
            }
        }
        this.messageHandler = eventBus.consumer(this.clientAddress) { message ->
            val messageBody = message.body()
            if (resp.ended()) {
                if (messageBody is ReportData) {
                    message.reply("ended")
                }
                return@consumer
            }
            this.writeHeadersIfNecessary()
            this.setupCommentTimer()
            if (messageBody is ReportData) {
                val dataPayload = messageBody.actualData.replace("\n", "\ndata: ")
                resp.write(
                    ": sent at ${nowAsString()}\nevent: data\nid: ${messageBody.idempotencyKey}\ndata: $dataPayload\n\n"
                ).onComplete {
                    message.reply("handled")
                }
            }
            if (messageBody is UnregisterQueryRequest) {
                resp.end()
            }
        }
    }

    override fun stop() {
        val timerID = this.timerID
        if (timerID != null) {
            this.vertx.cancelTimer(timerID)
        }
        this.messageHandler?.unregister()
        super.stop()
    }
}

const val channelsPrefix = "/channels/"
val baseInvalidMethodJson = jsonObjectOf("code" to "invalid-method")
val baseInvalidChannelJson = jsonObjectOf("code" to "invalid-channel")
val internalFalureJson = jsonObjectOf("code" to "internal-failure")

fun failRequest(req: HttpServerRequest): Future<Void> {
    return jsonStatusCodeResponse(req, 500).end(internalFalureJson.toString())
}

fun handleQuery(vertx: Vertx, channel: String, req: HttpServerRequest) {
    val sharedData = vertx.sharedData()
    incrementGlobalQueryCountReturning(sharedData, 1L).onComplete increment@{ queryCountResult ->
        if (queryCountResult.failed()) {
            failRequest(req)
            return@increment
        }
        val queryMap = QueryStringDecoder(req.query() ?: "").parameters()
        val clientID = getClientIDFromSerial()
        val returnAddress = addressForQueryClientAtOffset(clientID)
        var bestOffset = 0
        var bestQueryCount = Int.MAX_VALUE
        for (i in 0 until getQueryThreads(sharedData)) {
            val queryCount = getCurrentQueryCount(sharedData, i)
            if (queryCount < bestQueryCount) {
                bestOffset = i
                bestQueryCount = queryCount
            }
        }
        val serverAddress = addressForQueryServerQuery(sharedData, bestOffset)
        val response = req.response()
        val sseClient = QueryClientSSEVerticle(response, returnAddress, clientID, channel, serverAddress)
        val registerRequest = RegisterQueryRequest(
            channel,
            clientID,
            queryMap,
            returnAddress
        )
        val eventBus = vertx.eventBus()
        vertx.deployVerticle(sseClient).onComplete { deploymentIDResult ->
            if (deploymentIDResult.failed()) {
                failRequest(req)
                return@onComplete
            }
            val deploymentID = deploymentIDResult.result()
            response.endHandler {
                eventBus.request<Any>(
                    serverAddress,
                    UnregisterQueryRequest(channel, clientID),
                    localRequestOptions
                ).onComplete {
                    vertx.undeploy(deploymentID).onSuccess {
                        decrementGlobalQueryCountReturning(sharedData, 1L)
                    }
                }
            }
            eventBus.request<HttpProtocolErrorOrJson>(
                serverAddress,
                registerRequest,
                localRequestOptions
            ) {
                if (it.failed()) {
                    failRequest(req)
                } else {
                    it.result().body().whenError { err ->
                        jsonStatusCodeResponse(req, err.statusCode).end(err.contents.toString())
                    }.whenSuccess {
                        // At this point, the entire request lifecycle is governed by the verticle we just registered.
                        // But we still need to make sure the headers get written, so we'll send an arbitrary string
                        // to trigger .writeHeadersIfNecessary.
                        eventBus.publish(returnAddress, "send-headers")
                    }
                }
            }
        }
    }
}

val missingContentTypeJson = jsonObjectOf("code" to "missing-content-type")
val baseUnsupportedContentTypeJson = jsonObjectOf("code" to "invalid-content-type")
val missingEventSourceJson = jsonObjectOf("code" to "missing-event-source")
val missingEventIDJson = jsonObjectOf("code" to "missing-event-id")
val invalidJsonBodyJson = jsonObjectOf("code" to "invalid-json-body")
val baseExceededDataLimitJson = jsonObjectOf("code" to "exceeded-outstanding-data-limit")
val acceptedJson = jsonObjectOf("code" to "accepted")

fun handleData(vertx: Vertx, channel: String, req: HttpServerRequest) {
    val withParams = req.headers().get("Content-Type")
    if (withParams == null) {
        jsonStatusCodeResponse(req, 400).end(missingContentTypeJson.toString())
        return
    }
    val paramOffset = withParams.indexOf(";")
    val eventBus = vertx.eventBus()
    val sharedData = vertx.sharedData()
    when (if (paramOffset < 0) { withParams } else { withParams.substring(0, paramOffset) }) {
        "application/json" -> {
            val eventSource = req.headers().get("ce-source")
            if (eventSource == null) {
                jsonStatusCodeResponse(req, 400).end(missingEventSourceJson.toString())
                return
            }
            val eventID = req.headers().get("ce-id")
            if (eventID == null) {
                jsonStatusCodeResponse(req, 400).end(missingEventIDJson.toString())
                return
            }
            val idempotencyKey = "$eventSource $eventID"
            val serverThreads = getTranslatorThreads(vertx.sharedData())
            val targetTranslatorOffset = idempotencyKey.hashCode().absoluteValue % serverThreads
            val sendAddress = addressForTranslatorServer(vertx.sharedData(), targetTranslatorOffset)
            req.bodyHandler { bodyBytes ->
                val json: JsonObject
                try {
                    json = JsonObject(bodyBytes)
                } catch (e: Exception) {
                    jsonStatusCodeResponse(req,400).end(invalidJsonBodyJson.toString())
                    return@bodyHandler
                }
                val unpackRequest = UnpackDataRequest(
                    channel,
                    listOf(idempotencyKey to json),
                )
                incrementOutstandingDataCountReturning(sharedData, serverThreads.toLong()).onComplete { dataCountResult ->
                    if (dataCountResult.failed()) {
                        failRequest(req)
                        return@onComplete
                    }
                    val queryCount = dataCountResult.result()
                    val limit = getMaxOutstandingData(sharedData)
                    if (queryCount > limit) {
                        decrementOutstandingDataCountReturning(sharedData, serverThreads.toLong()).onComplete {
                            jsonStatusCodeResponse(req, 429).end(
                                baseExceededDataLimitJson.put("count", queryCount).put("limit", limit).toString()
                            )
                        }
                        return@onComplete
                    }
                    eventBus.request<HttpProtocolErrorOrReportDataList>(
                        sendAddress,
                        unpackRequest,
                        localRequestOptions
                    ) { busResponse ->
                        if (busResponse.failed()) {
                            decrementOutstandingDataCountReturning(sharedData, serverThreads.toLong()).onComplete {
                                failRequest(req)
                            }
                            return@request
                        }
                        val errorOrReports = busResponse.result()
                        errorOrReports.body().whenError { err ->
                            decrementOutstandingDataCountReturning(sharedData, serverThreads.toLong()).onComplete {
                                jsonStatusCodeResponse(req, err.statusCode).end(err.contents.toString())
                            }
                        }.whenSuccess {
                            for (report in it.entries) {
                                for (i in 0 until serverThreads) {
                                    eventBus.publish(addressForQueryServerData(sharedData, i), report)
                                }
                            }
                            jsonStatusCodeResponse(req, 202).end(acceptedJson.toString())
                        }
                    }
                }
            }
        }
        // FIXME: Handle Structured Content and Batched Content modes
        else -> {
            jsonStatusCodeResponse(req, 400).end(
                baseUnsupportedContentTypeJson.put("content-type", withParams).toString()
            )
        }
    }
}

val notFoundJson = jsonObjectOf("code" to "not-found")

class WebServerVerticle: AbstractVerticle() {
    private var server: HttpServer? = null

    override fun start(promise: Promise<Void>) {
        super.start()
        val server = this.vertx.createHttpServer()
        this.server = server
        server.requestHandler { req ->
            req.exceptionHandler {
                val resp = req.response()
                if (resp.ended()) return@exceptionHandler
                if (!resp.headWritten()) {
                    failRequest(req)
                }
                resp.end()
            }
            try {
                if (req.path().startsWith(channelsPrefix)) {
                    val possiblyChannel = req.path().substring(channelsPrefix.length)
                    val slashIndex = possiblyChannel.indexOf("/")
                    if (slashIndex > 0 && slashIndex != possiblyChannel.length - 1) {
                        jsonStatusCodeResponse(req, 400).end(
                            baseInvalidChannelJson.copy().put("channel", possiblyChannel).toString()
                        )
                        return@requestHandler
                    }
                    val channel = if (slashIndex < 0) {
                        possiblyChannel
                    } else {
                        possiblyChannel.substring(0, slashIndex)
                    }

                    if (req.method() == HttpMethod.POST || req.method() == HttpMethod.PUT) {
                        handleData(vertx, channel, req)
                    } else if (req.method() == HttpMethod.GET) {
                        handleQuery(vertx, channel, req)
                    } else {
                        jsonStatusCodeResponse(req, 400)
                            .end(baseInvalidMethodJson.copy().put("method", req.method()).toString())
                    }
                } else {
                    jsonStatusCodeResponse(req, 404).end(notFoundJson.toString())
                }
            } catch (e: Exception) {
                val resp = req.response()
                if (resp.ended()) return@requestHandler
                if (!resp.headWritten()) {
                    failRequest(req)
                }
                resp.end()
            }
        }
        server.listen(getServerPort(this.vertx.sharedData())) {
            if (it.failed()) {
                promise.fail(it.cause())
            } else {
                promise.complete()
            }
        }
    }

    override fun stop() {
        this.server?.close()
        super.stop()
    }
}

suspend fun registerWebServer(vertx: Vertx, webServerVerticles: Int): Set<String> {
    val deploymentIDs = mutableSetOf<String>()
    for (i in 0 until webServerVerticles) {
        val deploymentID = vertx.deployVerticle(WebServerVerticle()).await()
        deploymentIDs.add(deploymentID)
    }
    return deploymentIDs
}