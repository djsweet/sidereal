package name.djsweet.thorium

import io.vertx.core.shareddata.LocalMap
import io.vertx.core.shareddata.SharedData

internal fun timingLocalMap(sharedData: SharedData): LocalMap<String, Int> {
    return sharedData.getLocalMap("thorium.timings")
}

fun getIdempotencyExpirationMS(sharedData: SharedData): Int {
    // That's 10 minutes by default.
    return timingLocalMap(sharedData).getOrDefault("idempotencyExpirationMS", 3 * 60_000)
}

internal fun limitLocalMap(sharedData: SharedData): LocalMap<String, Int> {
    return sharedData.getLocalMap("thorium.limits")
}

fun getMaximumIdempotencyKeys(sharedData: SharedData): Int {
    return limitLocalMap(sharedData).getOrDefault("maxIdempotencyKeys", 1024 * 1024)
}

private const val byteBudgetKey = "byteBudget"

fun getByteBudget(sharedData: SharedData): Int {
    return limitLocalMap(sharedData).computeIfAbsent(byteBudgetKey) { maxSafeKeyValueSizeSync() }
}

internal fun reestablishByteBudget(sharedData: SharedData): Int {
    limitLocalMap(sharedData).compute(byteBudgetKey) { _, prior ->
        maxSafeKeyValueSizeSync().coerceAtMost(prior ?: Int.MAX_VALUE)
    }
    return getByteBudget(sharedData)
}

internal fun establishByteBudget(sharedData: SharedData, byteBudget: Int) {
    limitLocalMap(sharedData).putIfAbsent(byteBudgetKey, byteBudget)
}

fun getMaxQueryTerms(sharedData: SharedData): Int {
    return limitLocalMap(sharedData).getOrDefault("maxQueryTerms", 32)
}

fun getMaxJsonParsingRecursion(sharedData: SharedData): Int {
    return limitLocalMap(sharedData).getOrDefault("maxJsonParsingRecursion", 64)
}

private fun getDefaultToAvailableProcessors(localMap: LocalMap<String, Int>, key: String): Int {
    return localMap.computeIfAbsent(key) { Runtime.getRuntime().availableProcessors() }
}

fun getQueryThreads(sharedData: SharedData): Int {
    return getDefaultToAvailableProcessors(limitLocalMap(sharedData), "queryThreads")
}

fun getTranslatorThreads(sharedData: SharedData): Int {
    return getDefaultToAvailableProcessors(limitLocalMap(sharedData),"translatorThreads")
}

fun getWebServerThreads(sharedData: SharedData): Int {
    return getDefaultToAvailableProcessors(limitLocalMap(sharedData), "webServerThreads")
}

fun getMaxOutstandingData(sharedData: SharedData): Long {
    return limitLocalMap(sharedData).computeIfAbsent("maxOutstandingData") {
        128 * 1024 * getQueryThreads(sharedData)
    }.toLong()
}

internal fun intParamsLocalMap(sharedData: SharedData): LocalMap<String, Int> {
    return sharedData.getLocalMap("thorium.intParams")
}

fun getServerPort(sharedData: SharedData): Int {
    return intParamsLocalMap(sharedData).getOrDefault("serverPort", 8232)
}