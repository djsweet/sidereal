package name.djsweet.thorium.servers

import io.micrometer.prometheus.PrometheusMeterRegistry
import io.netty.handler.codec.http.QueryStringDecoder
import io.vertx.core.*
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpMethod
import io.vertx.core.http.HttpServer
import io.vertx.core.http.HttpServerOptions
import io.vertx.core.http.HttpServerRequest
import io.vertx.core.http.HttpServerResponse
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.json.jsonObjectOf
import io.vertx.kotlin.coroutines.await
import kotlinx.collections.immutable.PersistentMap
import name.djsweet.thorium.*
import java.lang.NumberFormatException
import java.net.URLDecoder
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import kotlin.math.absoluteValue

fun writeCommonHeaders(resp: HttpServerResponse): HttpServerResponse {
    return resp
        .putHeader("Cache-Control", "no-store")
        .putHeader("Connection", "keep-alive")
        .putHeader("Access-Control-Allow-Origin", "*")
}

const val jsonMimeType = "application/json; charset=utf-8"
fun jsonStatusCodeResponse(config: GlobalConfig, req: HttpServerRequest, code: Int): HttpServerResponse {
    return writeCommonHeaders(req.response())
        .putHeader("Content-Type", jsonMimeType)
        .putHeader("ce-specversion", "1.0")
        .putHeader("ce-datacontenttype", jsonMimeType)
        .putHeader("ce-time", wallNowAsString())
        .putHeader("ce-type", "name.djsweet.thorium.channel.send.response")
        .putHeader("ce-source", config.sourceName)
        // We don't need a cryptographically random source for event IDs, we just need to make sure
        // we effectively never have an ID collision, while at the same time avoiding blocking as much
        // as we possibly can.
        //
        // java.util.ThreadLocalRandom uses the same linear congruential generator as java.util.Random,
        // both with a period of around 2^48 results. Even when considering that we expect duplicates
        // after 2^24 attempts, these duplicates are extremely unlikely to generate a true collision,
        // considering we use 120 bits to generate the event ID.
        .putHeader("ce-id", generateOutboundEventID(ThreadLocalRandom.current()))
        .setStatusCode(code)
}

fun requestHasBody(req: HttpServerRequest): Boolean {
    val headers = req.headers()
    // See IETF RFC 7230 section 3.3 -- any request can contain a body, even if this is semantically not expected.
    // The presence of either of these headers is enough to signal to the server that a body will be following.
    return headers["content-length"] != null || headers["transfer-encoding"] != null
}

fun jsonStatusResponseBodyCloseIfRequestBody(
    config: GlobalConfig,
    req: HttpServerRequest,
    code: Int,
    body: JsonObject
): Future<Void> {
    return jsonStatusCodeResponse(config, req, code).end(body.encode()).onComplete {
        if (requestHasBody(req)) {
            req.connection().close()
        }
    }
}

val clientSerial: ThreadLocal<Long> = ThreadLocal.withInitial { 0.toLong() }
fun getClientIDFromSerial(): String {
    val current = clientSerial.get()
    clientSerial.set(current + 1)
    return "${Thread.currentThread().id}-$current"
}

const val channelsPrefix = "/channels/"
const val metricsPrefix = "/metrics"
const val keyReferenceCountPrefix = "/metrics/keycounts/"
val baseInvalidMethodJson = jsonObjectOf("code" to "invalid-method")
val baseInvalidChannelJson = jsonObjectOf("code" to "invalid-channel")
val internalFailureJson = jsonObjectOf("code" to "internal-failure")
val unexpectedBodyJson = jsonObjectOf("code" to "unexpected-body")

fun failRequest(config: GlobalConfig, req: HttpServerRequest): Future<Void> {
    return jsonStatusResponseBodyCloseIfRequestBody(config, req, 500, internalFailureJson)
}

fun handleQuery(
    vertx: Vertx,
    config: GlobalConfig,
    counters: GlobalCounterContext,
    channel: String,
    req: HttpServerRequest
) {
    if (requestHasBody(req)) {
        jsonStatusResponseBodyCloseIfRequestBody(config, req, 400, unexpectedBodyJson)
        return
    }

    counters.incrementGlobalQueryCountByAndGet(1)
    val queryMap = QueryStringDecoder(req.query() ?: "", false).parameters()
    val clientID = getClientIDFromSerial()
    val returnAddress = addressForQueryClientAtOffset(clientID)

    // The initial proposed query server is randomized, to ensure that too many concurrent connections
    // don't overwhelm a single query server.
    val queryThreads = config.queryThreads
    val initialOffset = ThreadLocalRandom.current().nextInt().absoluteValue % queryThreads
    var bestOffset = 0
    var bestQueryCount = Int.MAX_VALUE

    for (i in 0 until queryThreads) {
        val inspect = (i + initialOffset) % queryThreads
        val queryCount = counters.getQueryCountForThread(inspect).toInt()
        if (queryCount < bestQueryCount) {
            bestOffset = inspect
            bestQueryCount = queryCount
        }
    }
    val serverAddress = addressForQueryServerQuery(config, bestOffset)
    val response = req.response()
    val sseClient = QueryClientSSEServer(response, returnAddress, clientID, channel, serverAddress)
    val registerRequest = RegisterQueryRequest(
        channel,
        clientID,
        queryMap,
        returnAddress
    )
    val eventBus = vertx.eventBus()
    vertx.deployVerticle(sseClient).onComplete { deploymentIDResult ->
        if (deploymentIDResult.failed()) {
            failRequest(config, req)
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
                    counters.decrementGlobalQueryCount(1)
                }
            }
        }
        eventBus.request<HttpProtocolErrorOrJson>(
            serverAddress,
            registerRequest,
            localRequestOptions
        ) {
            if (it.failed()) {
                failRequest(config, req)
            } else {
                it.result().body().whenError { err ->
                    jsonStatusResponseBodyCloseIfRequestBody(config, req, err.statusCode, err.contents)
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

val missingContentTypeJson = jsonObjectOf("code" to "missing-content-type")
val baseUnsupportedContentTypeJson = jsonObjectOf("code" to "invalid-content-type")
val missingEventSourceJson = jsonObjectOf("code" to "missing-event-source")
val missingEventIDJson = jsonObjectOf("code" to "missing-event-id")
val invalidJsonBodyJson = jsonObjectOf("code" to "invalid-json-body")
val missingContentLengthJson = jsonObjectOf("code" to "missing-content-length")
val invalidContentLengthJson = jsonObjectOf("code" to "invalid-content-length")
val partialContentJson = jsonObjectOf("code" to "partial-content")
val bodyTooLargeJson = jsonObjectOf("code" to "body-too-large")
val bodyTimeoutJson = jsonObjectOf("code" to "body-timeout")
val baseExceededEventLimitJson = jsonObjectOf("code" to "exceeded-outstanding-event-limit")
val invalidDataFieldJson = jsonObjectOf("code" to "invalid-data-field")
val conflictingDataTypeHeadersJson = jsonObjectOf("code" to "conflicting-data-type-headers")
val acceptedJson = jsonObjectOf("code" to "accepted")
// The X- prefix was deprecated in IETF RFC 6648, dated June 2012, so it's something of a free-for-all now.
const val bodyReadTimeHeader = "Thorium-Body-Read-Time"
const val jsonParseTimeHeader = "Thorium-JSON-Parse-Time"
const val eventEncodeTimeHeader = "Thorium-Encode-Time"

fun handleDataWithUnpackRequest(
    vertx: Vertx,
    config: GlobalConfig,
    counters: GlobalCounterContext,
    unpackReqs: List<UnpackDataRequest>,
    httpReq: HttpServerRequest
) {
    val translatorSendStartTime = monotonicNowMS()
    sendUnpackDataRequests(vertx, config, counters, unpackReqs, respectLimit = true).onFailure {
        failRequest(config, httpReq)
    }.onSuccess { (withinLimit, eventCountWithReqs, error) ->
        if (!withinLimit) {
            // We have already read the entire body here; there's no need to close the underlying connection
            jsonStatusCodeResponse(config, httpReq, 429).end(
                baseExceededEventLimitJson.copy()
                    .put("count", eventCountWithReqs)
                    .put("limit", config.maxOutstandingEvents)
                    .encode()
            )
            return@onSuccess
        }

        if (error != null) {
            // We have already read the entire body here; there's no need to close the underlying connection
            jsonStatusCodeResponse(config, httpReq, error.statusCode).end(error.contents.encode())
            return@onSuccess
        }

        // This is a success condition so there's no need to close any connections.
        jsonStatusCodeResponse(config, httpReq, 202)
            .putHeader(eventEncodeTimeHeader, "${monotonicNowMS() - translatorSendStartTime} ms")
            .end(acceptedJson.encode())
    }
}

fun readFullBodyBytes(
    vertx: Vertx,
    config: GlobalConfig,
    req: HttpServerRequest,
    handle: (resp: HttpServerResponse, bs: Buffer) -> Unit
) {
    val resp = req.response()
    val contentLengthString = req.headers()["content-length"]
    // Note that the cases below need to close the underlying HTTP connection instead of just returning normally.
    // In an HTTP/1.1 pipelined request model, we'll be waiting around to read the whole body before recovering,
    // and incurring ingress bandwidth as a result, _unless_ we close the underlying connection.
    if (contentLengthString == null) {
        jsonStatusResponseBodyCloseIfRequestBody(config, req, 400, missingContentLengthJson)
        return
    }
    var contentLength = 0
    try {
        contentLength = Integer.parseInt(contentLengthString)
    } catch (e: NumberFormatException) {
        // Do nothing, we're going to handle this in the check below
    }
    if (contentLength <= 0) {
        jsonStatusResponseBodyCloseIfRequestBody(config, req, 400, invalidContentLengthJson)
        return
    }
    if (contentLength > config.maxBodySizeBytes) {
        jsonStatusResponseBodyCloseIfRequestBody(
            config,
            req,
            413,
            bodyTooLargeJson.copy().put("maxBytes", config.maxBodySizeBytes)
        )
        return
    }
    var bytesToGo = contentLength
    val buffer = Buffer.buffer(contentLength)
    val bodyReadStartTime = monotonicNowMS()
    val timeoutHandle = vertx.setTimer(config.bodyTimeoutMS.toLong()) {
        if (bytesToGo > 0) {
            jsonStatusResponseBodyCloseIfRequestBody(config, req, 408, bodyTimeoutJson)
        }
    }
    req.handler { chunk ->
        if (chunk.length() > bytesToGo) {
            req.connection().close()
        }
        buffer.appendBuffer(chunk)
        bytesToGo -= chunk.length()
    }
    req.endHandler {
        vertx.cancelTimer(timeoutHandle)
        if (resp.closed() || resp.ended()) {
            return@endHandler
        }
        resp.putHeader(bodyReadTimeHeader, "${monotonicNowMS() - bodyReadStartTime} ms")
        if (buffer.length() != contentLength) {
            // We somehow under-read the buffer. This shouldn't have happened, the connection isn't
            // going to work now.
            jsonStatusResponseBodyCloseIfRequestBody(config, req, 400, partialContentJson)
            return@endHandler
        }
        handle(resp, buffer)
    }
}

fun readBodyTryParseJsonObject(
    vertx: Vertx,
    config: GlobalConfig,
    req: HttpServerRequest,
    handle: (obj: JsonObject) -> Unit
) {
    readFullBodyBytes(vertx, config, req) { resp, bodyBytes ->
        val jsonParseStartTime = monotonicNowMS()

        // Clients are expected to send very large bodies. We've measured 440 kB payloads taking up to 5 ms,
        // which is enough to become very uncomfortable on the event loop, so all JSON parsing happens in
        // the worker pool.
        vertx.executeBlocking { ->
            JsonObject(bodyBytes)
        }.onFailure {
            // We've read the full body here, so there's no need to close the underlying connection.
            jsonStatusCodeResponse(config, req, 400)
                .putHeader(jsonParseTimeHeader, "${monotonicNowMS() - jsonParseStartTime} ms")
                .end(invalidJsonBodyJson.encode())
        }.onSuccess {
            resp.putHeader(jsonParseTimeHeader, "${monotonicNowMS() - jsonParseStartTime} ms")
            handle(it)
        }
    }
}

fun readBodyTryParseJsonArray(
    vertx: Vertx,
    config: GlobalConfig,
    req: HttpServerRequest,
    handle: (arr: JsonArray, jsonParseStartTime: Long) -> Unit
) {
    readFullBodyBytes(vertx, config, req) { resp, bodyBytes ->
        val jsonParseStartTime = monotonicNowMS()

        // Clients are expected to send very large bodies. We've measured 440 kB payloads taking up to 5 ms,
        // which is enough to become very uncomfortable on the event loop, so all JSON parsing happens in
        // the worker pool.
        vertx.executeBlocking { ->
            JsonArray(bodyBytes)
        }.onFailure {
            // We've read the full body here, so there's no need to close the underlying connection.
            jsonStatusCodeResponse(config, req,400)
                .putHeader(jsonParseTimeHeader, "${monotonicNowMS() - jsonParseStartTime} ms")
                .end(invalidJsonBodyJson.encode())
        }.onSuccess {
            resp.putHeader(jsonParseTimeHeader, "${monotonicNowMS() - jsonParseStartTime} ms")
            handle(it, jsonParseStartTime)
        }
    }
}

fun encodeEventSourceIDAsIdempotencyKey(source: String, id: String): String {
    return "$source $id"
}

fun handleData(
    vertx: Vertx,
    config: GlobalConfig,
    counters: GlobalCounterContext,
    channel: String,
    req: HttpServerRequest
) {
    val withParams = req.headers().get("Content-Type")
    if (withParams == null) {
        jsonStatusResponseBodyCloseIfRequestBody(config, req, 400, missingContentTypeJson)
        return
    }
    val paramOffset = withParams.indexOf(";")
    when (if (paramOffset < 0) { withParams } else { withParams.substring(0, paramOffset) }) {
        "application/json" -> {
            val headers = req.headers()

            val eventSource = headers.get("ce-source")
            if (eventSource == null) {
                jsonStatusResponseBodyCloseIfRequestBody(config, req, 400, missingEventSourceJson)
                return
            }

            val eventID = headers.get("ce-id")
            if (eventID == null) {
                jsonStatusResponseBodyCloseIfRequestBody(config, req, 400, missingEventIDJson)
                return
            }

            val dataContentTypeHeader = headers.get("ce-datacontenttype")
            if (dataContentTypeHeader != null && dataContentTypeHeader != withParams) {
                // The HTTP Protocol Binding for CloudEvents - Version 1.0.3-wip, section 3.1.1 states
                // > Note that a ce-datacontenttype HTTP header MUST NOT be present in the message
                // (https://github.com/cloudevents/spec/blob/a64cb14/cloudevents/bindings/http-protocol-binding.md#311-http-content-type)
                // But we are more relaxed in our restrictions. If it is set, it just must be equal to
                // Content-Type.
                jsonStatusResponseBodyCloseIfRequestBody(config, req, 400, conflictingDataTypeHeadersJson)
                return
            }

            val idempotencyKey = encodeEventSourceIDAsIdempotencyKey(eventSource, eventID)
            readBodyTryParseJsonObject(vertx, config, req) { data ->
                val unpackRequest = UnpackDataRequest(
                    channel,
                    idempotencyKey,
                    // Jackson renders JSON to strings in the order of insertion; having the "data" section
                    // come last is pretty convenient for visual inspection, so we'll only add the full data
                    // after all the attributes are set.
                    fillDataWithCloudEventsHeaders(req, jsonObjectOf()).put("data", data)
                )
                handleDataWithUnpackRequest(
                    vertx,
                    config,
                    counters,
                    listOf(unpackRequest),
                    req
                )
            }
        }
        "application/cloudevents+json" -> {
            readBodyTryParseJsonObject(vertx, config, req) { json ->
                val eventSource = json.getValue("source")
                if (eventSource !is String) {
                    // We've already read the entire body, so there's no need to close the connection.
                    jsonStatusCodeResponse(config, req, 400).end(missingEventSourceJson.encode())
                    return@readBodyTryParseJsonObject
                }

                val eventID = json.getValue("id")
                if (eventID !is String) {
                    // We've already read the entire body, so there's no need to close the connection.
                    jsonStatusCodeResponse(config, req, 400).end(missingEventIDJson.encode())
                    return@readBodyTryParseJsonObject
                }

                // This is, at this point, a sanity check. We indirectly refer to "data"
                // in many queries if they aren't explicitly anchored at the metadata root.
                val data = json.getValue("data")
                if (data !is JsonObject) {
                    // We've already read the entire body, so there's no need to close the connection.
                    jsonStatusCodeResponse(config, req, 400).end(invalidDataFieldJson.encode())
                    return@readBodyTryParseJsonObject
                }

                val idempotencyKey = encodeEventSourceIDAsIdempotencyKey(eventSource, eventID)
                val unpackRequest = UnpackDataRequest(
                    channel,
                    idempotencyKey,
                    json
                )
                handleDataWithUnpackRequest(
                    vertx,
                    config,
                    counters,
                    listOf(unpackRequest),
                    req
                )
            }
        }
        "application/cloudevents-batch+json" -> {
            readBodyTryParseJsonArray(vertx, config, req) { json, jsonParseStartTime ->
                val unpackRequests = mutableListOf<UnpackDataRequest>()
                for (i in 0 until json.size()) {
                    val elem = json.getValue(i)
                    if (elem !is JsonObject) {
                        // We've already read the entire body, so there's no need to close the connection.
                        jsonStatusCodeResponse(config, req,400).end(
                            invalidJsonBodyJson.copy().put("offset", i).encode()
                        )
                        return@readBodyTryParseJsonArray
                    }

                    val eventSource = elem.getValue("source")
                    if (eventSource !is String) {
                        // We've already read the entire body, so there's no need to close the connection.
                        jsonStatusCodeResponse(config, req, 400).end(
                            missingEventSourceJson.copy().put("offset", i).encode()
                        )
                        return@readBodyTryParseJsonArray
                    }

                    val eventID = elem.getValue("id")
                    if (eventID !is String) {
                        // We've already read the entire body, so there's no need to close the connection.
                        jsonStatusCodeResponse(config, req, 400).end(
                            missingEventIDJson.copy().put("offset", i).encode()
                        )
                        return@readBodyTryParseJsonArray
                    }

                    // This is, at this point, a sanity check. We indirectly refer to "data"
                    // in many queries if they aren't explicitly anchored at the metadata root.
                    val data = elem.getValue("data")
                    if (data !is JsonObject) {
                        // We've already read the entire body, so there's no need to close the connection.
                        jsonStatusCodeResponse(config, req, 400).end(
                            invalidDataFieldJson.copy().put("offset", i).encode()
                        )
                        return@readBodyTryParseJsonArray
                    }

                    val idempotencyKey = encodeEventSourceIDAsIdempotencyKey(eventSource, eventID)
                    unpackRequests.add(UnpackDataRequest(
                        channel,
                        idempotencyKey,
                        elem
                    ))
                }

                req.response().putHeader(jsonParseTimeHeader, "${monotonicNowMS() - jsonParseStartTime} ms")
                handleDataWithUnpackRequest(
                    vertx,
                    config,
                    counters,
                    unpackRequests,
                    req
                )
            }
        }
        else -> {
            jsonStatusResponseBodyCloseIfRequestBody(
                config,
                req,
                400,
                baseUnsupportedContentTypeJson.copy().put("contentType", withParams)
            )
        }
    }
}

val notFoundJson = jsonObjectOf("code" to "not-found")

fun extractReferenceCountsToJSON(counts: PersistentMap<String, KeyPathReferenceCount>): JsonObject {
    val baseResult = jsonObjectOf()
    for ((key, value) in counts) {
        if (value.isEmpty()) {
            continue
        }
        if (value.subKeys.isEmpty()) {
            baseResult.put(key, value.references)
        } else {
            baseResult.put(key, extractReferenceCountsToJSON(value.subKeys))
        }
    }
    return baseResult
}

fun handleKeyReferenceCounts(config: GlobalConfig, counts: KeyPathReferenceCount, req: HttpServerRequest) {
    val result = extractReferenceCountsToJSON(counts.subKeys)
    jsonStatusResponseBodyCloseIfRequestBody(config, req, 200, result)
}

fun extractChannelFromRemainingPath(remainingPath: String): String? {
    val slashIndex = remainingPath.indexOf("/")
    if (slashIndex > 0 && slashIndex != remainingPath.length - 1) {
        return null
    }
    return URLDecoder.decode(if (slashIndex < 0) {
        remainingPath
    } else {
        remainingPath.substring(0, slashIndex)
    }, "UTF-8")
}

class WebServerVerticle(
    private val config: GlobalConfig,
    private val counters: GlobalCounterContext,
    private val meterRegistry: PrometheusMeterRegistry
): AbstractVerticle() {
    private var server: HttpServer? = null

    override fun start(promise: Promise<Void>) {
        super.start()

        val config = this.config
        val serverOptions = HttpServerOptions()
            .setPort(config.serverPort)
            .setCompressionSupported(false)
            .setDecompressionSupported(false)
            .setTcpKeepAlive(true)
            .setIdleTimeoutUnit(TimeUnit.MILLISECONDS)
            .setIdleTimeout(config.tcpIdleTimeoutMS)

        val server = this.vertx.createHttpServer(serverOptions)
        this.server = server

        server.requestHandler { req ->
            req.exceptionHandler {
                val resp = req.response()
                if (resp.ended()) return@exceptionHandler
                if (!resp.headWritten()) {
                    failRequest(config, req)
                } else {
                    resp.end()
                }
            }
            try {
                val path = req.path()
                if (path.startsWith(channelsPrefix)) {
                    val possiblyChannel = req.path().substring(channelsPrefix.length)
                    val channel = extractChannelFromRemainingPath(possiblyChannel)
                    if (channel == null) {
                        jsonStatusResponseBodyCloseIfRequestBody(
                            config,
                            req,
                            400,
                            baseInvalidChannelJson.copy().put("channel", possiblyChannel)
                        )
                        return@requestHandler
                    }

                    if (req.method() == HttpMethod.POST || req.method() == HttpMethod.PUT) {
                        handleData(vertx, this.config, this.counters, channel, req)
                    } else if (req.method() == HttpMethod.GET) {
                        handleQuery(vertx, this.config, this.counters, channel, req)
                    } else {
                        jsonStatusResponseBodyCloseIfRequestBody(
                            config,
                            req,
                            405,
                            baseInvalidMethodJson.copy().put("method", req.method())
                        )
                    }
                } else if (path.startsWith(metricsPrefix)) {
                    if (req.method() != HttpMethod.GET) {
                        jsonStatusResponseBodyCloseIfRequestBody(
                            config,
                            req,
                            405,
                            baseInvalidMethodJson.copy().put("method", req.method())
                        )
                        return@requestHandler
                    }
                    if (requestHasBody(req)) {
                        jsonStatusResponseBodyCloseIfRequestBody(config, req, 400, unexpectedBodyJson)
                        return@requestHandler
                    }
                    if (path.length == metricsPrefix.length
                            || (path.length == metricsPrefix.length + 1 && path[metricsPrefix.length] == '/')) {
                        writeCommonHeaders(req.response())
                            .putHeader("Content-Type", "text/plain; version=0.0.4")
                            .end(this.meterRegistry.scrape())
                    } else if (path.startsWith(keyReferenceCountPrefix)) {
                        val possiblyChannel = req.path().substring(keyReferenceCountPrefix.length)
                        val channel = extractChannelFromRemainingPath(possiblyChannel)
                        if (channel == null) {
                            jsonStatusResponseBodyCloseIfRequestBody(
                                config,
                                req,
                                400,
                                baseInvalidChannelJson.copy().put("channel", possiblyChannel)
                            )
                            return@requestHandler
                        }

                        val forChannel = counters.getKeyPathReferenceCountsForChannel(channel)
                        if (forChannel == null) {
                            jsonStatusResponseBodyCloseIfRequestBody(config, req, 404, notFoundJson)
                            return@requestHandler
                        }

                        handleKeyReferenceCounts(config, forChannel, req)
                    } else {
                        jsonStatusResponseBodyCloseIfRequestBody(config, req, 404, notFoundJson)
                    }
                } else {
                    jsonStatusResponseBodyCloseIfRequestBody(config, req, 404, notFoundJson)
                }
            } catch (e: Exception) {
                val resp = req.response()
                if (resp.ended()) return@requestHandler
                if (!resp.headWritten()) {
                    failRequest(config, req)
                } else {
                    resp.end()
                }
            }
        }

        server.listen {
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

suspend fun registerWebServer(
    vertx: Vertx,
    config: GlobalConfig,
    counters: GlobalCounterContext,
    prom: PrometheusMeterRegistry,
): Set<String> {
    val deploymentIDs = mutableSetOf<String>()
    val webServerThreads = config.webServerThreads
    for (i in 0 until webServerThreads) {
        val deploymentID = vertx.deployVerticle(WebServerVerticle(config, counters, prom)).await()
        deploymentIDs.add(deploymentID)
    }
    return deploymentIDs
}