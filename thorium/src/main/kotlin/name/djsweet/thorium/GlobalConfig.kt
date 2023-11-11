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
        const val defaultIdempotencyExpirationMS = 3 * 60_000
        const val defaultMaximumIdempotencyKeys = 1024 * 1024
        const val defaultMaxQueryTerms = 32
        const val defaultMaxJsonParsingRecursion = 64
        val defaultQueryThreads = availableProcessors()
        val defaultTranslatorThreads = availableProcessors()
        val defaultWebServerThreads = availableProcessors()
        const val defaultMaxOutstandingEventsPerQueryThread = 128 * 1024
    }

    val serverPort: Int get() = this.intParamsLocalMap.getOrDefault("serverPort", defaultServerPort)

    val idempotencyExpirationMS: Int
        get() = this.timingLocalMap.getOrDefault("idempotencyExpirationMS", defaultIdempotencyExpirationMS)

    val maximumIdempotencyKeys: Int
        get() = this.limitLocalMap.getOrDefault("maxIdempotencyKeys", defaultMaximumIdempotencyKeys)

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

    val maxQueryTerms: Int get() = this.limitLocalMap.getOrDefault("maxQueryTerms", defaultMaxQueryTerms)

    val maxJsonParsingRecursion: Int
        get() = this.limitLocalMap.getOrDefault("maxJsonParsingRecursion", defaultMaxJsonParsingRecursion)

    val queryThreads: Int get() = this.limitLocalMap.getOrDefault("queryThreads", defaultQueryThreads)

    val translatorThreads: Int
        get() = this.limitLocalMap.getOrDefault("translatorThreads", defaultTranslatorThreads)

    val webServerThreads: Int
        get() = this.limitLocalMap.getOrDefault("webServerThreads", defaultWebServerThreads)

    val maxOutstandingEventsPerQueryThread: Int
        get() = this.limitLocalMap.getOrDefault(
            "maxOutstandingEventsPerThread",
            defaultMaxOutstandingEventsPerQueryThread
        )

    val maxOutstandingEvents: Int get() = this.maxOutstandingEventsPerQueryThread * this.queryThreads
}
