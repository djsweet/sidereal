package name.djsweet.thorium

import io.vertx.core.shareddata.LocalMap
import io.vertx.core.shareddata.SharedData

internal fun availableProcessors(): Int = Runtime.getRuntime().availableProcessors()

class GlobalConfig(private val sharedData: SharedData) {
    private val timingLocalMap: LocalMap<String, Int> get() = this.sharedData.getLocalMap("thorium.timings")

    private val limitLocalMap: LocalMap<String, Int> get() = this.sharedData.getLocalMap("thorium.limits")

    private val intParamsLocalMap: LocalMap<String, Int> get() = this.sharedData.getLocalMap("thorium.intParams")

    companion object {
        const val defaultServerPort = 8232
        const val defaultBodyTimeoutMS = 60_000
        const val defaultIdempotencyExpirationMS = 3 * 60_000
        const val defaultTcpIdleTimeoutMS = 3 * 60_000
        const val defaultMaxIdempotencyKeys = 1024 * 1024
        const val defaultMaxQueryTerms = 32
        const val defaultMaxJsonParsingRecursion = 64
        const val defaultMaxOutstandingEventsPerQueryThread = 128 * 1024
        const val defaultMaxBodySize = 10 * 1024 * 1024
        val defaultQueryThreads = availableProcessors()
        val defaultTranslatorThreads = availableProcessors()
        val defaultWebServerThreads = availableProcessors()
    }

    private val serverPortKey = "serverPort"
    var serverPort: Int
        get() = this.intParamsLocalMap.getOrDefault(this.serverPortKey, defaultServerPort)
        set(newPort) {
            this.intParamsLocalMap[this.serverPortKey] = newPort
                .coerceAtLeast(1)
                .coerceAtMost(65535)
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

    private val maxOutstandingEventsPerQueryKey = "maxOutstandingEventsPerThread"
    var maxOutstandingEventsPerQueryThread: Int
        get() = this.limitLocalMap.getOrDefault(
            this.maxOutstandingEventsPerQueryKey,
            defaultMaxOutstandingEventsPerQueryThread
        )
        set(newOutstandingEventsPerQuery) {
            this.limitLocalMap[this.maxOutstandingEventsPerQueryKey] = newOutstandingEventsPerQuery
                .coerceAtLeast(1)
        }

    val maxOutstandingEvents: Int get() = this.maxOutstandingEventsPerQueryThread * this.queryThreads

    private val maxBodySizeBytesKey = "maxBodySizeBytes"
    var maxBodySizeBytes: Int
        get() = this.limitLocalMap.getOrDefault(this.maxBodySizeBytesKey, defaultMaxBodySize)
        set(newMaxBodySize) {
            this.limitLocalMap[this.maxBodySizeBytesKey] = newMaxBodySize.coerceAtLeast(128)
        }

    private val queryThreadsKey = "queryThreads"
    var queryThreads: Int
        get() = this.limitLocalMap.getOrDefault(this.queryThreadsKey, defaultQueryThreads)
        set(newQueryThreadCount) {
            this.limitLocalMap[this.queryThreadsKey] = newQueryThreadCount
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
