// SPDX-FileCopyrightText: 2023 Dani Sweet <thorium@djsweet.name>
//
// SPDX-License-Identifier: MIT

package name.djsweet.sidereal

import io.vertx.core.shareddata.LocalMap
import io.vertx.core.shareddata.SharedData

internal fun availableProcessors(): Int = Runtime.getRuntime().availableProcessors()

internal const val metaChannelName = "meta"

class GlobalConfig(private val sharedData: SharedData) {
    private val timingLocalMap: LocalMap<String, Int> get() = this.sharedData.getLocalMap("thorium.timings")

    private val limitLocalMap: LocalMap<String, Int> get() = this.sharedData.getLocalMap("thorium.limits")

    private val intParamsLocalMap: LocalMap<String, Int> get() = this.sharedData.getLocalMap("thorium.intParams")
    private val stringParamsLocalMap: LocalMap<String, String> get() = this.sharedData.getLocalMap("thorium.stringParams")

    companion object {
        const val defaultServerPort = 8232
        const val defaultBodyTimeoutMS = 60_000
        const val defaultIdempotencyExpirationMS = 3 * 60_000
        const val defaultTcpIdleTimeoutMS = 3 * 60_000
        const val defaultMaxIdempotencyKeys = 1024 * 1024
        const val defaultMaxQueryTerms = 32
        const val defaultMaxJsonParsingRecursion = 64
        const val defaultMaxOutstandingEventsPerRouterThread = 128 * 1024
        const val defaultMaxBodySize = 10 * 1024 * 1024
        const val defaultCloudEventSource = "//name.djsweet.thorium"
        val defaultRouterThreads = availableProcessors()
        val defaultTranslatorThreads = availableProcessors()
        val defaultWebServerThreads = availableProcessors()
    }

    /**
     * Used to disambiguate between Thorium instances with the same source name
     */
    val instanceID = generateInstanceID()

    private val serverPortKey = "serverPort"
    var serverPort: Int
        get() = this.intParamsLocalMap.getOrDefault(this.serverPortKey, defaultServerPort)
        set(newPort) {
            this.intParamsLocalMap[this.serverPortKey] = newPort
                .coerceAtLeast(1)
                .coerceAtMost(65535)
        }

    private val sourceNameKey = "sourceName"
    var sourceName: String
        get() = this.stringParamsLocalMap.getOrDefault(this.sourceNameKey, defaultCloudEventSource)
        set(newSourceName) {
            this.stringParamsLocalMap[this.sourceNameKey] = newSourceName
        }

    private val bodyTimeoutKey = "bodyTimeoutMS"
    var bodyTimeoutMS: Int
        get() = this.limitLocalMap.getOrDefault(this.bodyTimeoutKey, defaultBodyTimeoutMS)
        set(newBodyTimeout) {
            this.limitLocalMap[this.bodyTimeoutKey] = newBodyTimeout.coerceAtLeast(100)
        }

    private val tcpIdleTimeoutKey = "tcpIdleTimeout"
    var tcpIdleTimeoutMS: Int
        get() = this.intParamsLocalMap.getOrDefault(this.tcpIdleTimeoutKey, defaultTcpIdleTimeoutMS)
        set(newTcpTimeout) {
            this.intParamsLocalMap[this.tcpIdleTimeoutKey] = newTcpTimeout.coerceAtLeast(200)
        }

    private val idempotencyExpirationKey = "idempotencyExpirationMS"
    var idempotencyExpirationMS: Int
        get() = this.timingLocalMap.getOrDefault(this.idempotencyExpirationKey, defaultIdempotencyExpirationMS)
        set(newExpiration) {
            this.timingLocalMap[this.idempotencyExpirationKey] = newExpiration.coerceAtLeast(0)
        }

    private val maxIdempotencyMapKey = "maxIdempotencyKeys"
    var maxIdempotencyKeys: Int
        get() = this.limitLocalMap.getOrDefault(this.maxIdempotencyMapKey, defaultMaxIdempotencyKeys)
        set(newKeySize) { this.limitLocalMap[this.maxIdempotencyMapKey] = newKeySize.coerceAtLeast(0) }

    private val byteBudgetKey = "byteBudget"
    val byteBudget: Int get() = this.limitLocalMap.computeIfAbsent(this.byteBudgetKey) { maxSafeKeyValueSizeSync() }

    fun establishByteBudget(newByteBudget: Int) {
        this.limitLocalMap.putIfAbsent(this.byteBudgetKey, newByteBudget)
    }

    fun reestablishByteBudget(): Int {
        return this.limitLocalMap.compute(this.byteBudgetKey) { _, prior ->
            maxSafeKeyValueSizeSync().coerceAtMost(prior ?: Int.MAX_VALUE)
        }!!
    }

    private val maxQueryTermsKey = "maxQueryTerms"
    var maxQueryTerms: Int
        get() = this.limitLocalMap.getOrDefault(this.maxQueryTermsKey, defaultMaxQueryTerms)
        set(newTermsCount) {
            this.limitLocalMap[this.maxQueryTermsKey] = newTermsCount.coerceAtLeast(1)
        }

    private val maxJsonParsingRecursionKey = "maxJsonParsingRecursion"
    var maxJsonParsingRecursion: Int
        get() = this.limitLocalMap.getOrDefault(this.maxJsonParsingRecursionKey, defaultMaxJsonParsingRecursion)
        set(newRecursionLimit) {
            this.limitLocalMap[this.maxJsonParsingRecursionKey] = newRecursionLimit.coerceAtLeast(0)
        }

    private val maxOutstandingEventsPerRouterThreadKey = "maxOutstandingEventsPerThread"
    var maxOutstandingEventsPerRouterThread: Int
        get() = this.limitLocalMap.getOrDefault(
            this.maxOutstandingEventsPerRouterThreadKey,
            defaultMaxOutstandingEventsPerRouterThread
        )
        set(newOutstandingEventsPerQuery) {
            this.limitLocalMap[this.maxOutstandingEventsPerRouterThreadKey] = newOutstandingEventsPerQuery
                .coerceAtLeast(1)
        }

    val maxOutstandingEvents: Int get() = this.maxOutstandingEventsPerRouterThread * this.routerThreads

    private val maxBodySizeBytesKey = "maxBodySizeBytes"
    var maxBodySizeBytes: Int
        get() = this.limitLocalMap.getOrDefault(this.maxBodySizeBytesKey, defaultMaxBodySize)
        set(newMaxBodySize) {
            this.limitLocalMap[this.maxBodySizeBytesKey] = newMaxBodySize.coerceAtLeast(128)
        }

    private val routerThreadsKey = "routerThreads"
    var routerThreads: Int
        get() = this.limitLocalMap.getOrDefault(this.routerThreadsKey, defaultRouterThreads)
        set(newQueryThreadCount) {
            this.limitLocalMap[this.routerThreadsKey] = newQueryThreadCount
                .coerceAtLeast(1)
                .coerceAtMost(availableProcessors())
        }

    private val translatorThreadsKey = "translatorThreads"
    var translatorThreads: Int
        get() = this.limitLocalMap.getOrDefault(this.translatorThreadsKey, defaultTranslatorThreads)
        set(newTranslatorThreadCount) {
            this.limitLocalMap[this.translatorThreadsKey] = newTranslatorThreadCount
                .coerceAtLeast(1)
                .coerceAtMost(availableProcessors())
        }

    private val webServerThreadsKey = "webServerThreads"
    var webServerThreads: Int
        get() = this.limitLocalMap.getOrDefault(this.webServerThreadsKey, defaultWebServerThreads)
        set(newWebServerThreadCount) {
            this.limitLocalMap[this.webServerThreadsKey] = newWebServerThreadCount
                .coerceAtLeast(1)
                .coerceAtMost(2 * availableProcessors())
        }

}
