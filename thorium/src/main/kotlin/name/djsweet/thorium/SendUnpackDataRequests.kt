package name.djsweet.thorium

import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.eventbus.Message
import kotlin.math.absoluteValue

const val reportBatchSize = 256

data class SendUnpackDataRequestsStatus(
    val withinLimit: Boolean,
    val eventCountWithSends: Long,
    val error: HttpProtocolError?
)

fun sendUnpackDataRequests(
    vertx: Vertx,
    config: GlobalConfig,
    counters: GlobalCounterContext,
    unpackReqs: List<UnpackDataRequest>,
    respectLimit: Boolean
): Future<SendUnpackDataRequestsStatus> {
    val eventBus = vertx.eventBus()

    val queryThreads = config.queryThreads
    val translatorThreads = config.translatorThreads

    val eventIncrement = (unpackReqs.size * queryThreads).toLong()
    val newOutstandingEventCount = counters.incrementOutstandingEventCountByAndGet(eventIncrement)

    if (respectLimit) {
        val priorEventCount = newOutstandingEventCount - eventIncrement
        if (priorEventCount >= config.maxOutstandingEvents) {
            counters.decrementOutstandingEventCount(eventIncrement)
            return Future.succeededFuture(
                SendUnpackDataRequestsStatus(
                    withinLimit = false,
                    eventCountWithSends = newOutstandingEventCount,
                    error = null
                )
            )
        }
    }

    val requestLists = Array<MutableList<UnpackDataRequestWithIndex>>(translatorThreads) { mutableListOf() }
    for (i in unpackReqs.indices) {
        val unpackReq = unpackReqs[i]
        val targetTranslatorOffset = unpackReq.idempotencyKey.hashCode().absoluteValue % translatorThreads
        requestLists[targetTranslatorOffset].add(UnpackDataRequestWithIndex(i, unpackReq))
    }
    val translatorSends = requestLists.mapIndexed { index, unpackDataRequestsWithIndices ->
        val sendAddress = addressForTranslatorServer(config, index)
        eventBus.request<HttpProtocolErrorOrReportDataListWithIndexes>(
            sendAddress,
            UnpackDataRequestList(unpackDataRequestsWithIndices),
            localRequestOptions
        )
    }

    return Future.join(translatorSends).onFailure {
        counters.decrementOutstandingEventCount(eventIncrement)
    }.map translatorResults@ { futures ->
        // These data reports are dispatched to all query servers in batches of size `batchSize`.
        // We're doing this so that there is some degree of fairness in receiving data from multiple
        // sources: if we just dumped a 16,000 entry item into each query server's queue, we'd starve out
        // the 100 entry requests trying to process it.
        val batchesSize = unpackReqs.size / reportBatchSize
        val batches = Array<ArrayList<ReportData?>>(batchesSize) { ArrayList(reportBatchSize) }

        val modBatchSize = unpackReqs.size % reportBatchSize
        val modBatch = ArrayList<ReportData?>(modBatchSize)
        val indexAtIntoModBatch = batchesSize * reportBatchSize

        val futuresSize = futures.size()
        for (i in 0 until futuresSize) {
            val responseList = futures.resultAt<Message<HttpProtocolErrorOrReportDataListWithIndexes>>(i).body()
            var responseError: HttpProtocolError? = null
            responseList.whenError { error ->
                counters.decrementOutstandingEventCount(eventIncrement)
                responseError = error
            }.whenSuccess { reports ->
                for (maybeResponse in reports.responses) {
                    if (maybeResponse == null) {
                        continue
                    }
                    val (index, report) = maybeResponse
                    val targetBatch = if (index >= indexAtIntoModBatch) {
                        modBatch
                    } else {
                        batches[index / reportBatchSize]
                    }
                    val indexInBatch = index % reportBatchSize
                    for (j in targetBatch.size..indexInBatch) {
                        targetBatch.add(null)
                    }
                    targetBatch[indexInBatch] = report
                }
            }
            if (responseError != null) {
                return@translatorResults SendUnpackDataRequestsStatus(
                    withinLimit = true,
                    eventCountWithSends = newOutstandingEventCount,
                    error = responseError
                )
            }
        }
        for (i in 0 until batchesSize) {
            val reportBatch = ReportDataList(batches[i])
            eventBus.publish(addressForQueryServerData, reportBatch, localRequestOptions)
        }
        if (modBatchSize > 0) {
            val reportBatch = ReportDataList(modBatch)
            eventBus.publish(addressForQueryServerData, reportBatch, localRequestOptions)
        }

        return@translatorResults SendUnpackDataRequestsStatus(
            withinLimit = true,
            eventCountWithSends = newOutstandingEventCount,
            error = null
        )
    }
}