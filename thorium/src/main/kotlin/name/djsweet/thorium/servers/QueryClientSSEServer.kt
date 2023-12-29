// SPDX-FileCopyrightText: 2023 Dani Sweet <thorium@djsweet.name>
//
// SPDX-License-Identifier: MIT

package name.djsweet.thorium.servers

import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.eventbus.MessageConsumer
import io.vertx.core.http.HttpServerResponse
import io.vertx.kotlin.core.json.jsonObjectOf
import name.djsweet.thorium.*

const val serverSentPingTimeout = 30_000.toLong()

class QueryClientSSEServer(
    private val resp: HttpServerResponse,
    private val clientAddress: String,
    private val clientID: String,
    private val channel: String,
    private val serverAddress: String,
): AbstractVerticle() {
    private var timerID: Long? = null
    private var pingFuture: Future<Void> = Future.succeededFuture()
    private var messageHandler: MessageConsumer<Any>? = null

    private fun writeHeadersIfNecessary() {
        if (this.resp.headWritten()) {
            return
        }
        val connectPayload = jsonObjectOf(
            "timestamp" to wallNowAsString(),
            "clientID" to this.clientID
        ).encode()
        writeCommonHeaders(this.resp)
            .setChunked(true)
            .setStatusCode(200)
            .putHeader("Content-Type", "text/event-stream; charset=utf-8")
            .write("event: connect\ndata: $connectPayload\n\n")
    }

    private fun writePing(): Future<Void> {
        return this.resp.write("event: ping\ndata: {\"timestamp\":\"${wallNowAsString()}\"}\n\n")
    }

    private fun setupPingTimer() {
        val vertx = this.vertx

        val currentTimerIDBeforeComment = this.timerID
        if (currentTimerIDBeforeComment != null) {
            vertx.cancelTimer(currentTimerIDBeforeComment)
            this.timerID = null
        }

        this.pingFuture = this.pingFuture.eventually {

            // If we're hammering on setupPingTimer, we're buffering up a ton
            // of .onComplete handlers on the same comment future. This permits
            // a weird world where we can establish multiple timers, and spam
            // the output with a ton of pings. Cancelling the outstanding timer
            // in here, again, prevents this weird world.
            val currentTimerIDAfterComment = this.timerID
            if (currentTimerIDAfterComment != null) {
                vertx.cancelTimer(currentTimerIDAfterComment)
            }

            this.timerID = vertx.setTimer(serverSentPingTimeout) {
                this.pingFuture = this.pingFuture.eventually {
                    writePing()
                }.onComplete {
                    this.setupPingTimer()
                }
            }

            Future.succeededFuture<Void>()
        }
    }

    override fun start() {
        super.start()
        val vertx = this.vertx
        val resp = this.resp
        val eventBus = vertx.eventBus()
        this.resp.endHandler {
            eventBus.request<Any>(
                this.serverAddress,
                UnregisterQueryRequest(this.channel, this.clientID, this.clientID),
                localRequestOptions
            ).onComplete {
                vertx.undeploy(this.deploymentID())
            }
        }
        this.messageHandler = eventBus.consumer(this.clientAddress) { message ->
            val messageBody = message.body()
            if (resp.ended()) {
                // It's important that we keep replying to the query server even when we're not
                // actually sending the data; if we dropped this message without replying to it,
                // we'd deadlock the query server until Vertx times out the request. This is
                // particularly important around the query un-registration process: we might receive
                // multiple data reports before the un-registration actually happens, even after
                // we've requested it.
                if (messageBody is ReportDataWithClientAndQueryIDs) {
                    message.reply("ended")
                }
                return@consumer
            }
            this.writeHeadersIfNecessary()
            this.setupPingTimer()
            if (messageBody is ReportDataWithClientAndQueryIDs) {
                val (data) = messageBody
                resp.write(data.serverSentEventPayload.value).onComplete {
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