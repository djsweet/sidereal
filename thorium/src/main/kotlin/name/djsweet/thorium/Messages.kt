package name.djsweet.thorium

import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.MessageCodec
import io.vertx.core.json.JsonObject
import io.vertx.core.shareddata.SharedData
import io.vertx.kotlin.core.json.jsonObjectOf

fun appendStringAsUnicode(s: String, b: Buffer): Buffer {
    val stringBytes = convertStringToByteArray(s)
    b.appendInt(stringBytes.size)
    return b.appendBytes(stringBytes)
}

abstract class LocalListCodec<T, U>(
    nameSuffix: String,
    private val elementCodec: MessageCodec<T, T>
): LocalPrimaryMessageCodec<U>(nameSuffix) {
    abstract fun getListEntries(container: U): List<T?>
    abstract fun withListEntries(entries: List<T?>): U

    override fun encodeToWireNonNullBuffer(buffer: Buffer, s: U) {
        val elementCodec = this.elementCodec
        val entries = this.getListEntries(s)
        buffer.appendInt(entries.size)
        // We unfortunately have to cheese the output a little bit, because the contract for MessageCodec doesn't
        // give decodeFromWireNonNullBuffer any ability to communicate back how many bytes it consumed. We'll
        // instead record this known offset as an Int at the beginning of each entry.
        for (entry in entries) {
            if (entry == null) {
                buffer.appendByte(0)
                continue
            }
            buffer.appendByte(1)
            // We don't know the offset... yet. But we will once we're done! And we can overwrite this value
            // at the end to get an accurate understanding of where this entry ends.
            val priorOffset = buffer.length()
            buffer.appendInt(0)
            elementCodec.encodeToWire(buffer, entry)
            buffer.setInt(priorOffset, buffer.length())
        }
    }

    override fun decodeFromWireNonNullBuffer(pos: Int, buffer: Buffer): U {
        val elementCodec = this.elementCodec
        val listLength = buffer.getInt(pos)
        var nextPos = pos + 4
        val entries = mutableListOf<T?>()
        for (i in 0 until listLength) {
            val lastPos = nextPos
            val nullTag = buffer.getByte(lastPos)
            if (nullTag == 0.toByte()) {
                nextPos += 1
                entries.add(null)
            } else {
                nextPos = buffer.getInt(lastPos + 1)
                entries.add(elementCodec.decodeFromWire(lastPos + 5, buffer))
            }
        }
        return this.withListEntries(entries)
    }
}

interface ListIndexToMessage<T> {
    val index: Int
    val message: T?
}

abstract class ListIndexToMessageCodec<T, U: ListIndexToMessage<T>>(
    nameSuffix: String,
    private val messageCodec: MessageCodec<T, T>
): LocalPrimaryMessageCodec<U>(
    nameSuffix
) {
    abstract fun withIndexMessage(index: Int, maybeMessage: T?): U

    override fun encodeToWireNonNullBuffer(buffer: Buffer, s: U) {
        buffer.appendInt(s.index)
        if (s.message == null) {
            buffer.appendByte(0)
        } else {
            buffer.appendByte(1)
            this.messageCodec.encodeToWire(buffer, s.message)
        }
    }

    override fun decodeFromWireNonNullBuffer(pos: Int, buffer: Buffer): U {
        val index = buffer.getInt(pos)
        val nullFlag = buffer.getByte(pos + 4)
        return if (nullFlag == 0.toByte()) {
            this.withIndexMessage(index, null)
        } else {
            this.withIndexMessage(index, this.messageCodec.decodeFromWire(pos + 5, buffer))
        }
    }
}

data class RegisterQueryRequest(
    val channel: String,
    val clientID: String,
    val queryParams: Map<String, List<String>>,
    val returnAddress: String,
) {
    constructor(): this("", "", mapOf(), "")
}

class RegisterQueryRequestCodec: LocalPrimaryMessageCodec<RegisterQueryRequest>("RegisterQueryRequest") {
    override fun emptyInstance(): RegisterQueryRequest {
        return RegisterQueryRequest()
    }

    override fun encodeToWireNonNullBuffer(buffer: Buffer, s: RegisterQueryRequest) {
        appendStringAsUnicode(s.channel, buffer)
        appendStringAsUnicode(s.clientID, buffer)
        appendStringAsUnicode(s.returnAddress, buffer)
        buffer.appendInt(s.queryParams.size)
        for ((queryParamKey, queryParamValues) in s.queryParams) {
            appendStringAsUnicode(queryParamKey, buffer)
            buffer.appendInt(queryParamValues.size)
            for (queryParamValue in queryParamValues) {
                appendStringAsUnicode(queryParamValue, buffer)
            }
        }
    }

    override fun decodeFromWireNonNullBuffer(pos: Int, buffer: Buffer): RegisterQueryRequest {
        val channelPos = pos + 4
        val channelLength = buffer.getInt(pos)
        val clientIDLengthPos = channelPos + channelLength
        val channel = buffer.getString(channelPos, clientIDLengthPos, "utf-8")

        val clientIDPos = clientIDLengthPos + 4
        val clientIDLength = buffer.getInt(clientIDLengthPos)
        val returnAddressLengthPos = clientIDPos + clientIDLength
        val clientID = buffer.getString(clientIDPos, returnAddressLengthPos, "utf-8")

        val returnAddressPos = returnAddressLengthPos + 4
        val returnAddressLength = buffer.getInt(returnAddressLengthPos)
        val queryParamsSizePos = returnAddressPos + returnAddressLength
        val returnAddress = buffer.getString(returnAddressPos, queryParamsSizePos, "utf-8")

        val queryParamsSize = buffer.getInt(queryParamsSizePos)
        val queryParams = mutableMapOf<String, List<String>>()
        var lastPos = queryParamsSizePos + 4
        for (i in 0 until queryParamsSize) {
            val keyLength = buffer.getInt(lastPos)
            val keyPos = lastPos + 4
            val listSizePos = keyPos + keyLength
            val key = buffer.getString(keyPos, listSizePos, "utf-8")

            val listSize = buffer.getInt(listSizePos)
            lastPos = listSizePos + 4
            val valuesList = mutableListOf<String>()
            for (j in 0 until listSize) {
                val stringLength = buffer.getInt(lastPos)
                val stringPos = lastPos + 4
                lastPos = stringPos + stringLength
                val listEntry = buffer.getString(stringPos, lastPos, "utf-8")
                valuesList.add(listEntry)
            }
            queryParams[key] = valuesList
        }

        return RegisterQueryRequest(
            channel,
            clientID,
            queryParams,
            returnAddress
        )
    }
}

data class UnregisterQueryRequest(
    val channel: String,
    val clientID: String,
) {
    constructor() : this("", "")
}

class UnregisterQueryRequestCodec: LocalPrimaryMessageCodec<UnregisterQueryRequest>("UnregisterQueryRequest") {
    override fun emptyInstance(): UnregisterQueryRequest {
        return UnregisterQueryRequest()
    }

    override fun encodeToWireNonNullBuffer(buffer: Buffer, s: UnregisterQueryRequest) {
        appendStringAsUnicode(s.channel, buffer)
        appendStringAsUnicode(s.clientID, buffer)
    }

    override fun decodeFromWireNonNullBuffer(pos: Int, buffer: Buffer): UnregisterQueryRequest {
        val channelPos = pos + 4
        val channelLength = buffer.getInt(pos)
        val clientIDLengthPos = channelPos + channelLength
        val channel = buffer.getString(channelPos, clientIDLengthPos, "utf-8")

        val clientIDLength = buffer.getInt(clientIDLengthPos)
        val clientIDPos = clientIDLengthPos + 4
        val endPos = clientIDPos + clientIDLength
        val clientID = buffer.getString(clientIDPos, endPos, "utf-8")

        return UnregisterQueryRequest(channel, clientID)
    }
}

data class UnpackDataRequest(
    val channel: String,
    val idempotencyKey: String,
    val data: JsonObject,
) {
    constructor() : this("", "", jsonObjectOf())
}

class UnpackDataRequestCodec: LocalPrimaryMessageCodec<UnpackDataRequest>("UnpackDataRequest") {
    override fun emptyInstance(): UnpackDataRequest {
        return UnpackDataRequest()
    }

    override fun encodeToWireNonNullBuffer(buffer: Buffer, s: UnpackDataRequest) {
        appendStringAsUnicode(s.channel, buffer)
        appendStringAsUnicode(s.idempotencyKey, buffer)
        s.data.writeToBuffer(buffer)
    }

    override fun decodeFromWireNonNullBuffer(pos: Int, buffer: Buffer): UnpackDataRequest {
        val channelPos = pos + 4
        val channelLength = buffer.getInt(pos)
        val idempotencyKeyLengthPos = channelPos + channelLength
        val channel = buffer.getString(channelPos, idempotencyKeyLengthPos, "utf-8")

        val idempotencyKeyLength = buffer.getInt(idempotencyKeyLengthPos)
        val idempotencyKeyPos = idempotencyKeyLengthPos + 4
        val jsonPos = idempotencyKeyPos + idempotencyKeyLength
        val idempotencyKey = buffer.getString(idempotencyKeyPos, jsonPos, "utf-8")

        val data = jsonObjectOf()
        data.readFromBuffer(jsonPos, buffer)

        return UnpackDataRequest(channel, idempotencyKey, data)
    }
}

data class UnpackDataRequestWithIndex(
    override val index: Int,
    override val message: UnpackDataRequest?
): ListIndexToMessage<UnpackDataRequest> {
    constructor(): this(0, null)
}

class UnpackDataRequestWithIndexCodec: ListIndexToMessageCodec<UnpackDataRequest, UnpackDataRequestWithIndex>(
    "UnpackDataRequestWithIndex",
    UnpackDataRequestCodec()
) {
    override fun emptyInstance(): UnpackDataRequestWithIndex {
        return UnpackDataRequestWithIndex()
    }

    override fun withIndexMessage(index: Int, maybeMessage: UnpackDataRequest?): UnpackDataRequestWithIndex {
        return UnpackDataRequestWithIndex(index, maybeMessage)
    }
}

data class UnpackDataRequestList(
    val requests: List<UnpackDataRequestWithIndex?>
) {
    constructor(): this(listOf())
}

class UnpackDataRequestListCodec: LocalListCodec<UnpackDataRequestWithIndex, UnpackDataRequestList>(
    "UnpackDataRequestList",
    UnpackDataRequestWithIndexCodec()
) {
    override fun emptyInstance(): UnpackDataRequestList {
        return UnpackDataRequestList()
    }

    override fun getListEntries(container: UnpackDataRequestList): List<UnpackDataRequestWithIndex?> {
        return container.requests
    }

    override fun withListEntries(entries: List<UnpackDataRequestWithIndex?>): UnpackDataRequestList {
        return UnpackDataRequestList(entries)
    }
}

data class ReportData(
    val channel: String,
    val idempotencyKey: String,
    val queryableScalarData: ShareableQPTrieOfByteArrays,
    val queryableArrayData: ShareableQPTrieOfByteArrayLists,
    val actualData: String,
) {
    constructor() : this(
        "",
        "",
        ShareableQPTrieOfByteArrays(),
        ShareableQPTrieOfByteArrayLists(),
        ""
    )
}

class ReportDataCodec: LocalPrimaryMessageCodec<ReportData>("ReportData") {
    override fun emptyInstance(): ReportData {
        return ReportData()
    }

    override fun encodeToWireNonNullBuffer(buffer: Buffer, s: ReportData) {
        appendStringAsUnicode(s.channel, buffer)
        appendStringAsUnicode(s.idempotencyKey, buffer)
        s.queryableScalarData.writeToBuffer(buffer)
        s.queryableArrayData.writeToBuffer(buffer)
        appendStringAsUnicode(s.actualData, buffer)
    }

    override fun decodeFromWireNonNullBuffer(pos: Int, buffer: Buffer): ReportData {
        val channelSize = buffer.getInt(pos)
        val channelPos = pos + 4
        val idempotencyKeySizePos = channelPos + channelSize
        val channel = buffer.getString(channelPos, idempotencyKeySizePos, "utf-8")

        val idempotencyKeyPos = idempotencyKeySizePos + 4
        val idempotencyKeySize = buffer.getInt(idempotencyKeySizePos)
        val queryableDataPos = idempotencyKeyPos + idempotencyKeySize
        val idempotencyKey = buffer.getString(idempotencyKeyPos, queryableDataPos, "utf-8")

        val queryableScalarData = ShareableQPTrieOfByteArrays()
        val queryableArrayDataPos = queryableScalarData.readFromBuffer(queryableDataPos, buffer)

        val queryableArrayData = ShareableQPTrieOfByteArrayLists()
        val actualDataLengthPos = queryableArrayData.readFromBuffer(queryableArrayDataPos, buffer)

        val actualDataPos = actualDataLengthPos + 4
        val actualDataLength = buffer.getInt(actualDataLengthPos)
        val finalPos = actualDataPos + actualDataLength
        val actualData = buffer.getString(actualDataPos, finalPos, "utf-8")

        return ReportData(
            channel,
            idempotencyKey,
            queryableScalarData,
            queryableArrayData,
            actualData
        )
    }
}

data class ReportDataList(
    val entries: List<ReportData?>
) {
    constructor(): this(listOf())
}

class ReportDataListCodec: LocalListCodec<ReportData, ReportDataList>("ReportDataList", ReportDataCodec()) {
    override fun emptyInstance(): ReportDataList {
        return ReportDataList()
    }

    override fun getListEntries(container: ReportDataList): List<ReportData?> {
        return container.entries
    }

    override fun withListEntries(entries: List<ReportData?>): ReportDataList {
        return ReportDataList(entries)
    }
}

data class ReportDataWithIndex(
    override val index: Int,
    override val message: ReportData?
): ListIndexToMessage<ReportData> {
    constructor(): this(0, null)
}

class ReportDataWithIndexCodec: ListIndexToMessageCodec<ReportData, ReportDataWithIndex>(
    "ReportDataWithIndex",
    ReportDataCodec()
) {
    override fun emptyInstance(): ReportDataWithIndex {
        return ReportDataWithIndex()
    }

    override fun withIndexMessage(index: Int, maybeMessage: ReportData?): ReportDataWithIndex {
        return ReportDataWithIndex(index, maybeMessage)
    }
}

data class ReportDataListWithIndexes(
    val responses: List<ReportDataWithIndex?>
) {
    constructor(): this(listOf())
}

class ReportDataListWithIndexesCodec: LocalListCodec<ReportDataWithIndex, ReportDataListWithIndexes>(
    "ReportDataListWithIndexes",
    ReportDataWithIndexCodec()
) {
    override fun emptyInstance(): ReportDataListWithIndexes {
        return ReportDataListWithIndexes()
    }

    override fun getListEntries(container: ReportDataListWithIndexes): List<ReportDataWithIndex?> {
        return container.responses
    }

    override fun withListEntries(entries: List<ReportDataWithIndex?>): ReportDataListWithIndexes {
        return ReportDataListWithIndexes(entries)
    }
}

data class ResetByteBudget(val byteBudget: Int) {
    constructor(): this(0)
}

class ResetByteBudgetCodec: LocalPrimaryMessageCodec<ResetByteBudget>("ResetByteBudget") {
    override fun emptyInstance(): ResetByteBudget {
        return ResetByteBudget()
    }

    override fun encodeToWireNonNullBuffer(buffer: Buffer, s: ResetByteBudget) {
        buffer.appendInt(s.byteBudget)
    }

    override fun decodeFromWireNonNullBuffer(pos: Int, buffer: Buffer): ResetByteBudget {
        val byteBudget = buffer.getInt(pos)
        return ResetByteBudget(byteBudget)
    }
}

abstract class HttpProtocolErrorOrCodec<T, U: HttpProtocolErrorOr<T>>(
    nameSuffix: String,
    private val codec: LocalPrimaryMessageCodec<T>,
): LocalPrimaryMessageCodec<U>(nameSuffix) {
    protected abstract fun emptyFailureInstance(): U
    protected abstract fun ofError(err: HttpProtocolError): U
    protected abstract fun ofSuccess(success: T): U

    override fun emptyInstance(): U {
        return this.emptyFailureInstance()
    }

    override fun encodeToWireNonNullBuffer(buffer: Buffer, s: U) {
        s.whenError {
            buffer.appendByte(0)
            it.writeToBuffer(buffer)
        }.whenSuccess {
            buffer.appendByte(1)
            this.codec.encodeToWire(buffer, it)
        }
    }

    override fun decodeFromWireNonNullBuffer(pos: Int, buffer: Buffer): U {
        val kind = buffer.getByte(pos)
        return if (kind == 0.toByte()) {
            val error = HttpProtocolError()
            error.readFromBuffer(pos + 1, buffer)
            this.ofError(error)
        } else {
            this.ofSuccess(this.codec.decodeFromWire(pos + 1, buffer))
        }
    }
}

class HttpProtocolErrorOrJson private constructor(
    error: HttpProtocolError?,
    success: JsonObject?,
): HttpProtocolErrorOr<JsonObject>(error, success) {
    companion object {
        fun ofError(err: HttpProtocolError): HttpProtocolErrorOrJson {
            return HttpProtocolErrorOrJson(err, null)
        }

        fun ofSuccess(success: JsonObject): HttpProtocolErrorOrJson {
            return HttpProtocolErrorOrJson(null, success)
        }
    }
    constructor(): this(HttpProtocolError(), null)
}

class HttpProtocolErrorOrJsonCodec: HttpProtocolErrorOrCodec<JsonObject, HttpProtocolErrorOrJson>(
    "HttpProtocolErrorOrJson",
    TrivialJsonObjectCodec()
) {
    override fun emptyFailureInstance(): HttpProtocolErrorOrJson {
        return HttpProtocolErrorOrJson()
    }

    override fun ofError(err: HttpProtocolError): HttpProtocolErrorOrJson {
        return HttpProtocolErrorOrJson.ofError(err)
    }

    override fun ofSuccess(success: JsonObject): HttpProtocolErrorOrJson {
        return HttpProtocolErrorOrJson.ofSuccess(success)
    }
}

class HttpProtocolErrorOrReportDataWithIndex(
    error: HttpProtocolError?,
    success: ReportDataWithIndex?
): HttpProtocolErrorOr<ReportDataWithIndex>(error, success) {
    companion object {
        fun ofError(error: HttpProtocolError): HttpProtocolErrorOrReportDataWithIndex {
            return HttpProtocolErrorOrReportDataWithIndex(error, null)
        }

        fun ofSuccess(success: ReportDataWithIndex): HttpProtocolErrorOrReportDataWithIndex {
            return HttpProtocolErrorOrReportDataWithIndex(null, success)
        }
    }

    constructor(): this(HttpProtocolError(), null)
}

class HttpProtocolErrorOrReportDataWithIndexCodec: HttpProtocolErrorOrCodec<
    ReportDataWithIndex, HttpProtocolErrorOrReportDataWithIndex
>("HttpProtocolErrorOrReportDataWithIndex", ReportDataWithIndexCodec()) {
    override fun emptyFailureInstance(): HttpProtocolErrorOrReportDataWithIndex {
        return HttpProtocolErrorOrReportDataWithIndex()
    }

    override fun ofError(err: HttpProtocolError): HttpProtocolErrorOrReportDataWithIndex {
        return HttpProtocolErrorOrReportDataWithIndex.ofError(err)
    }

    override fun ofSuccess(success: ReportDataWithIndex): HttpProtocolErrorOrReportDataWithIndex {
        return HttpProtocolErrorOrReportDataWithIndex.ofSuccess(success)
    }
}

class HttpProtocolErrorOrReportDataListWithIndexes private constructor(
    error: HttpProtocolError?,
    success: ReportDataListWithIndexes?
): HttpProtocolErrorOr<ReportDataListWithIndexes>(error, success) {
    companion object {
        fun ofError(error: HttpProtocolError): HttpProtocolErrorOrReportDataListWithIndexes {
            return HttpProtocolErrorOrReportDataListWithIndexes(error, null)
        }

        fun ofSuccess(success: ReportDataListWithIndexes): HttpProtocolErrorOrReportDataListWithIndexes {
            return HttpProtocolErrorOrReportDataListWithIndexes(null, success)
        }
    }

    constructor(): this(HttpProtocolError(), null)
}

class HttpProtocolErrorOrReportDataListWithIndexesCodec: HttpProtocolErrorOrCodec<
    ReportDataListWithIndexes, HttpProtocolErrorOrReportDataListWithIndexes
>(
    "HttpProtocolErrorOrReportDataListWithIndexes",
    ReportDataListWithIndexesCodec()
) {
    override fun emptyFailureInstance(): HttpProtocolErrorOrReportDataListWithIndexes {
        return HttpProtocolErrorOrReportDataListWithIndexes()
    }

    override fun ofError(err: HttpProtocolError): HttpProtocolErrorOrReportDataListWithIndexes {
        return HttpProtocolErrorOrReportDataListWithIndexes.ofError(err)
    }

    override fun ofSuccess(success: ReportDataListWithIndexes): HttpProtocolErrorOrReportDataListWithIndexes {
        return HttpProtocolErrorOrReportDataListWithIndexes.ofSuccess(success)
    }
}

fun registerMessageCodecs(vertx: Vertx) {
    val eventBus = vertx.eventBus()
    eventBus
        .registerDefaultCodec(RegisterQueryRequest::class.java, RegisterQueryRequestCodec())
        .registerDefaultCodec(UnregisterQueryRequest::class.java, UnregisterQueryRequestCodec())
        .registerDefaultCodec(UnpackDataRequest::class.java, UnpackDataRequestCodec())
        .registerDefaultCodec(UnpackDataRequestWithIndex::class.java, UnpackDataRequestWithIndexCodec())
        .registerDefaultCodec(UnpackDataRequestList::class.java, UnpackDataRequestListCodec())
        .registerDefaultCodec(ReportData::class.java, ReportDataCodec())
        .registerDefaultCodec(ReportDataList::class.java, ReportDataListCodec())
        .registerDefaultCodec(ReportDataWithIndex::class.java, ReportDataWithIndexCodec())
        .registerDefaultCodec(ReportDataListWithIndexes::class.java, ReportDataListWithIndexesCodec())
        .registerDefaultCodec(ResetByteBudget::class.java, ResetByteBudgetCodec())
        .registerDefaultCodec(HttpProtocolErrorOrJson::class.java, HttpProtocolErrorOrJsonCodec())
        .registerDefaultCodec(HttpProtocolErrorOrReportDataWithIndex::class.java, HttpProtocolErrorOrReportDataWithIndexCodec())
        .registerDefaultCodec(HttpProtocolErrorOrReportDataListWithIndexes::class.java, HttpProtocolErrorOrReportDataListWithIndexesCodec())
}

private fun addressForQueryServerQueryAtOffset(verticleOffset: Int): String {
    return "thorium.query.server.$verticleOffset.query"
}

const val addressForQueryServerData = "thorium.query.server.all.data"

private fun addressForTranslatorServerAtOffset(verticleOffset: Int): String {
    return "thorium.data.translator.$verticleOffset"
}

fun addressForQueryClientAtOffset(clientID: String): String {
    return "thorium.query.clients.${clientID}"
}

private var queryServerQueryAddresses: Array<String>? = null
fun addressForQueryServerQuery(sharedData: SharedData, verticleOffset: Int): String {
    var currentQueryServerQueryAddresses = queryServerQueryAddresses
    if (currentQueryServerQueryAddresses == null) {
        currentQueryServerQueryAddresses = Array(getQueryThreads(sharedData)) { addressForQueryServerQueryAtOffset(it) }
        queryServerQueryAddresses = currentQueryServerQueryAddresses
    }
    return currentQueryServerQueryAddresses[verticleOffset]
}

private var translatorServerAddresses: Array<String>? = null
fun addressForTranslatorServer(sharedData: SharedData, verticleOffset: Int): String {
    var currentTranslatorServerAddresses = translatorServerAddresses
    if (currentTranslatorServerAddresses == null) {
        currentTranslatorServerAddresses = Array(getTranslatorThreads(sharedData)) { addressForTranslatorServerAtOffset(it) }
        translatorServerAddresses = currentTranslatorServerAddresses
    }
    return currentTranslatorServerAddresses[verticleOffset]
}

const val addressForByteBudgetReset = "thorium.byteBudgetReset"