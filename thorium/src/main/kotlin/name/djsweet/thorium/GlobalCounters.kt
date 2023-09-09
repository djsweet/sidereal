package name.djsweet.thorium

import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.shareddata.LocalMap
import io.vertx.core.shareddata.SharedData
import io.vertx.kotlin.coroutines.await

internal fun countersLocalMap(sharedData: SharedData): LocalMap<String, Int> {
    return sharedData.getLocalMap("thorium.counters")
}

fun getCurrentQueryCount(sharedData: SharedData, thread: Int): Int {
    return countersLocalMap(sharedData).getOrDefault("queryCounts.${thread}", 0)
}

fun setCurrentQueryCount(sharedData: SharedData, thread: Int, queryCount: Int) {
    return countersLocalMap(sharedData).set("queryCounts.${thread}", queryCount)
}

private fun incrementCounterReturning(sharedData: SharedData, name: String, incrementBy: Long): Future<Long> {
    val promise = Promise.promise<Long>()
    sharedData.getLocalCounter(name).onComplete {
        if (it.failed()) {
            promise.fail(it.cause())
        } else {
            it.result().addAndGet(incrementBy).onComplete(promise)
        }
    }
    return promise.future()
}

const val globalQueryCountName = "thorium.counters.globalQueryCount"

fun incrementGlobalQueryCountReturning(sharedData: SharedData, incrementBy: Long): Future<Long> {
    return incrementCounterReturning(sharedData, globalQueryCountName, incrementBy)
}

fun decrementGlobalQueryCountReturning(sharedData: SharedData, decrementBy: Long): Future<Long> {
    return incrementGlobalQueryCountReturning(sharedData, -decrementBy)
}

const val outstandingDataCountName = "thorium.counters.outstandingDataCount"

fun incrementOutstandingDataCountReturning(sharedData: SharedData, incrementBy: Long): Future<Long> {
    return incrementCounterReturning(sharedData, outstandingDataCountName, incrementBy)
}

fun decrementOutstandingDataCountReturning(sharedData: SharedData, decrementBy: Long): Future<Long> {
    return incrementOutstandingDataCountReturning(sharedData, -decrementBy)
}
