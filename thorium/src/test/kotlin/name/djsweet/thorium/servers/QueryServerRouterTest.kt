// SPDX-FileCopyrightText: 2023 Dani Sweet <thorium@djsweet.name>
//
// SPDX-License-Identifier: MIT

package name.djsweet.thorium.servers

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.netty.handler.codec.http.QueryStringDecoder
import io.vertx.core.AbstractVerticle
import io.vertx.core.Vertx
import io.vertx.core.eventbus.MessageConsumer
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.json.get
import io.vertx.kotlin.core.json.jsonArrayOf
import io.vertx.kotlin.core.json.jsonObjectOf
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.delay
import name.djsweet.thorium.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock

class QueryServerRouterTest {
    private val byteBudget = 1024
    private val recurseDepth = 64

    class DataReceiverVerticle(private val results: MutableList<ReportDataWithClientAndQueryIDs>): AbstractVerticle() {
        val receiverAddress = "name.djsweet.thorium.servers.test.DataReceiverVerticle"

        // This seems like an awful lot of pomp and circumstance around just getting data into a List...
        // but we need to be super mindful of how the receiving code in this Verticle is running in a
        // completely different thread from the test harness. And, as a result, we have to be cognizant of:
        //
        // 1. Waiting until all messages have been received and placed in the list, and more importantly
        // 2. Ensuring that every other thread can see that every message is present in the list
        //
        // So we need both a mechanism for signaling that all updates have been received, and a mechanism for
        // enforcing a "memory barrier" so that all writes to the List are visible in every thread after the
        // signal has been sent.
        //
        // It'd be very surprising if accessing an AtomicInteger with getAcquire didn't enforce a complete
        // memory barrier for all variables, but to ensure that we have a complete memory barrier for all
        // variables, based on the documented behavior of the mechanism, we also use a Condition. This has the
        // triple benefit of both being comparatively time-efficient versus an explicitly timed delay, CPU-efficient
        // compared to a non-delaying spin lock, and enforcing a complete memory barrier on signal receipt.
        val outstandingMessageCount = AtomicInteger(0)
        private val outstandingMessageLock = ReentrantLock()
        private val outstandingMessageCountCV = outstandingMessageLock.newCondition()

        private var messageHandler: MessageConsumer<Any>? = null

        private fun wakeupWaitingThreads() {
            this.outstandingMessageLock.lock()
            try {
                this.outstandingMessageCountCV.signalAll()
            } finally {
                this.outstandingMessageLock.unlock()
            }
        }

        override fun start() {
            super.start()
            val vertx = this.vertx
            val eventBus = vertx.eventBus()
            this.messageHandler = eventBus.localConsumer(this.receiverAddress) { message ->
                val messageBody = message.body()
                if (messageBody is ReportDataWithClientAndQueryIDs) {
                    this.results.add(messageBody)
                    val newMessageCount = this.outstandingMessageCount.decrementAndGet()
                    if (newMessageCount == 0) {
                        this.wakeupWaitingThreads()
                    }
                    message.reply("handled")
                }
            }
        }

        override fun stop() {
            this.messageHandler?.unregister()
            super.stop()
        }

        fun waitForNoOutstandingMessages() {
            this.outstandingMessageLock.lock()
            try {
                while (this.outstandingMessageCount.getAcquire() > 0) {
                    this.outstandingMessageCountCV.await()
                }
            } finally {
                this.outstandingMessageLock.unlock()
            }
        }
    }

    private fun withRunningRouterServer(testBody: suspend (
        vertx: Vertx,
        config: GlobalConfig,
        counters: GlobalCounterContext,
        receiver: DataReceiverVerticle,
        dataList: List<ReportDataWithClientAndQueryIDs>
    ) -> Unit) {
        withVertxAsync { vertx ->
            val config = GlobalConfig(vertx.sharedData())
            config.routerThreads = 1
            val counters = GlobalCounterContext(config.routerThreads)

            registerQueryServer(
                vertx,
                config,
                counters,
                SimpleMeterRegistry()
            )
            val results = mutableListOf<ReportDataWithClientAndQueryIDs>()
            val receiver = DataReceiverVerticle(results)
            vertx.deployVerticle(receiver)
            testBody(vertx, config, counters, receiver, results)
        }
    }

    private fun queryStringToMaps(qs: String): List<Map<String, List<String>>> {
        return qs.split(";").map { QueryStringDecoder(it, false).parameters() }
    }

    private suspend fun registerQueries(
        vertx: Vertx,
        channel: String,
        clientID: String,
        queryServerAddress: String,
        receiverAddress: String,
        vararg queries: Pair<String, String>
    ) {
        val eventBus = vertx.eventBus()
        for ((queryID, queryString) in queries) {
            val maps = this.queryStringToMaps(queryString)
            val registerReply = eventBus.request<HttpProtocolErrorOrJson>(
                queryServerAddress,
                RegisterQueryRequest(
                    channel,
                    clientID,
                    queryID,
                    queryString,
                    maps,
                    receiverAddress
                ),
                localRequestOptions
            ).await()

            registerReply.body()
                .whenError {
                    fail("Failed to register query ${queryID}: ${it.contents.encode()}")
                }.whenSuccess {
                    assertEquals(channel, it["channel"])
                    assertEquals(clientID, it["clientID"])
                    assertEquals(queryID, it["queryID"])
                }
        }
    }

    private fun reportAllJsonData(
        vertx: Vertx,
        counters: GlobalCounterContext,
        receiver: DataReceiverVerticle,
        channel: String, vararg data: JsonObject
    ) {
        val reportList = mutableListOf<ReportData>()
        val encodeEverythingContext = AcceptAllKeyValueFilterContext()

        for ((counter, point) in data.withIndex()) {
            val encoded = encodeJsonToQueryableData(point, encodeEverythingContext, this.byteBudget, this.recurseDepth)
            reportList.add(ReportData(
                channel,
                counter.toString(),
                encoded.scalars,
                encoded.arrays,
                thunkForReportDataString { point.encode() }
            ))
        }

        counters.incrementOutstandingEventCountByAndGet(data.size.toLong())
        receiver.outstandingMessageCount.addAndGet(data.size)
        vertx.eventBus().send(addressForQueryServerData, ReportDataList(reportList), localRequestOptions)

        receiver.waitForNoOutstandingMessages()
    }

    private fun cloudEventObjectOf(id: String, vararg fields: Pair<String, Any?>): JsonObject {
        return jsonObjectOf(
            "source" to "//name.djsweet.thorium.tests",
            "id" to id,
            "datacontenttype" to "application/json",
            "data" to jsonObjectOf(*fields)
        )
    }

    private suspend fun waitForZeroEventCount(counters: GlobalCounterContext) {
        while (counters.getOutstandingEventCount() > 0L) {
            delay(5)
        }
    }

    @Test
    fun orQueriesOnlyReplyOncePerData() {
        val testData1 = this.cloudEventObjectOf(
            "testData1",
            "a" to 1,
            "b" to jsonObjectOf(
                "a" to "2",
                "c" to 3
            ),
            "c" to "3"
        )
        val testData2 = this.cloudEventObjectOf(
            "testData2",
            "a" to 1,
            "b" to 2,
            "c" to "3"
        )
        val testData3 = this.cloudEventObjectOf(
            "testData3",
            "a" to jsonObjectOf(
                "b" to "2",
                "c" to 3
            ),
            "b" to 2,
            "c" to 3
        )

        val matchQuery12String = "a=1&c=\"3\""
        val matchQuery23String = "b=2"
        val matchQuery123String = "a=1&c=\"3\";b=2&%2fa%2fb=\"2\";b=2"

        val channel = "test-channel-or-no-duplicates"
        val clientID = "test-client-or-no-duplicates"

        this.withRunningRouterServer { vertx, config, counters, receiver, dataList ->
            val routerServerAddress = addressForRouterServer(config, 0)
            this.registerQueries(
                vertx,
                channel,
                clientID,
                routerServerAddress,
                receiver.receiverAddress,
                "one" to matchQuery12String,
                "two" to matchQuery23String,
                "three" to matchQuery123String
            )

            // All conjunctions in matchQuery123String count as a separate query,
            // so that's 1 for matchQuery12String, 1 for matchQuery23String, and 3 for matchQuery123String
            assertEquals(5, counters.getQueryCountForThread(0))

            this.reportAllJsonData(vertx, counters, receiver, channel, testData1, testData2, testData3)

            assertEquals(3, dataList.size)

            assertEquals(clientID, dataList[0].clientID)
            assertEquals(setOf("one", "three"), dataList[0].queryIDs.toSet())
            assertEquals(testData1.encode(), dataList[0].reportData.actualData.value)

            assertEquals(clientID, dataList[1].clientID)
            assertEquals(setOf("one", "two", "three"), dataList[1].queryIDs.toSet())
            assertEquals(testData2.encode(), dataList[1].reportData.actualData.value)

            assertEquals(clientID, dataList[2].clientID)
            assertEquals(setOf("two", "three"), dataList[2].queryIDs.toSet())
            assertEquals(testData3.encode(), dataList[2].reportData.actualData.value)

            this.waitForZeroEventCount(counters)
        }
    }

    @Test
    fun orQueriesShortCircuitArrayContains() {
        val channel = "test-channel-short-circuit-array-contains"
        val clientID = "test-client-short-circuit-array-contains"

        val testData = this.cloudEventObjectOf(
            "testData1",
            "a" to 1,
            "b" to jsonArrayOf(1, 2, 3)
        )

        val testQuery = "b=[4;a=1"

        this.withRunningRouterServer { vertx, config, counters, receiver, dataList ->
            val routerServerAddress = addressForRouterServer(config, 0)
            this.registerQueries(
                vertx,
                channel,
                clientID,
                routerServerAddress,
                receiver.receiverAddress,
                "one" to testQuery
            )

            // The conjunctions of testQuery count as two separate queries.
            assertEquals(2, counters.getQueryCountForThread(0))

            this.reportAllJsonData(vertx, counters, receiver, channel, testData)

            assertEquals(1, dataList.size)

            assertEquals(clientID, dataList[0].clientID)
            assertEquals(setOf("one"), dataList[0].queryIDs.toSet())
            assertEquals(testData.encode(), dataList[0].reportData.actualData.value)

            this.waitForZeroEventCount(counters)
        }
    }

    @Test
    fun orQueriesRespectArrayContains() {
        val channel = "test-channel-or-respects-array-contains"
        val clientID = "test-client-or-respects-array-contains"

        val testData = this.cloudEventObjectOf(
            "testData1",
            "a" to 1,
            "b" to jsonArrayOf(1, 2, 3)
        )

        val testQueryMatch = "a=1&b=[4;a=1&b=[2"
        val testQueryNoMatch = "a=1&b=[5"

        this.withRunningRouterServer { vertx, config, counters, receiver, dataList ->
            val routerServerAddress = addressForRouterServer(config, 0)
            this.registerQueries(
                vertx,
                channel,
                clientID,
                routerServerAddress,
                receiver.receiverAddress,
                "one" to testQueryNoMatch,
                "two" to testQueryMatch
            )

            // The conjunctions of testQueryMatch count as two separate queries.
            assertEquals(3, counters.getQueryCountForThread(0))

            this.reportAllJsonData(vertx, counters, receiver, channel, testData)

            assertEquals(1, dataList.size)

            assertEquals(clientID, dataList[0].clientID)
            assertEquals(setOf("two"), dataList[0].queryIDs.toSet())
            assertEquals(testData.encode(), dataList[0].reportData.actualData.value)

            this.waitForZeroEventCount(counters)
        }
    }
}