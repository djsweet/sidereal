package name.djsweet.thorium

import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonObject
import io.vertx.core.shareddata.SharedData

fun appendStringAsUnicode(s: String, b: Buffer): Buffer {
    val stringBytes = convertStringToByteArray(s)
    b.appendInt(stringBytes.size)
    return b.appendBytes(stringBytes)
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
    val entries: List<Pair<String, JsonObject>>,
) {
    constructor() : this("", listOf())
}

class UnpackDataRequestCodec: LocalPrimaryMessageCodec<UnpackDataRequest>("UnpackDataRequest") {
    override fun emptyInstance(): UnpackDataRequest {
        return UnpackDataRequest()
    }

    override fun encodeToWireNonNullBuffer(buffer: Buffer, s: UnpackDataRequest) {
        appendStringAsUnicode(s.channel, buffer)
        buffer.appendInt(s.entries.size)
        for ((idempotencyKey, data) in s.entries) {
            appendStringAsUnicode(idempotencyKey, buffer)
            data.writeToBuffer(buffer)
        }
    }

    override fun decodeFromWireNonNullBuffer(pos: Int, buffer: Buffer): UnpackDataRequest {
        val channelPos = pos + 4
        val channelLength = buffer.getInt(pos)
        val entriesLengthPos = channelPos + channelLength
        val channel = buffer.getString(channelPos, entriesLengthPos, "utf-8")
        val entriesLength = buffer.getInt(entriesLengthPos)
        var lastPos = entriesLengthPos + 4
        val entries = mutableListOf<Pair<String, JsonObject>>()

        for (i in 0 until entriesLength) {
            val idempotencyPos = lastPos + 4
            val idempotencyLength = buffer.getInt(lastPos)
            val dataPos = idempotencyPos + idempotencyLength
            val idempotencyKey = buffer.getString(idempotencyPos, dataPos, "utf-8")
            val data = JsonObject()
            lastPos = data.readFromBuffer(dataPos, buffer)
            entries.add(idempotencyKey to data)
        }

        return UnpackDataRequest(channel, entries)
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
    val entries: List<ReportData>
) {
    constructor() : this(listOf())
}

class ReportDataListCodec(): LocalPrimaryMessageCodec<ReportDataList>("ReportDataList") {
    private val reportDataCodec = ReportDataCodec()

    override fun emptyInstance(): ReportDataList {
        return ReportDataList()
    }

    override fun encodeToWireNonNullBuffer(buffer: Buffer, s: ReportDataList) {
        val entries = s.entries
        buffer.appendInt(entries.size)
        for (entry in entries) {
            // We need to know how long the encoded reportData is, so we can update the position offset
            // on each entry. Sadly, the buffer API wasn't prepared to let us do this, so we're going
            // to cheese it a little. Note that the entry length includes itself, so you won't have to +4
            // when updating the next position.
            val lastSize = buffer.length()
            buffer.appendInt(0)
            this.reportDataCodec.encodeToWire(buffer, entry)
            buffer.setInt(lastSize, buffer.length() - lastSize)
        }
    }

    override fun decodeFromWireNonNullBuffer(pos: Int, buffer: Buffer): ReportDataList {
        val entriesPos = pos + 4
        val entriesCount = buffer.getInt(pos)
        val entries = mutableListOf<ReportData>()
        var endPos = entriesPos
        for (i in 0 until entriesCount) {
            val entryLength = buffer.getInt(endPos)
            val nextEntry = this.reportDataCodec.decodeFromWire(endPos + 4, buffer)
            entries.add(nextEntry)
            endPos += entryLength
        }
        return ReportDataList(entries)
    }
}

abstract class HttpProtocolErrorOrCodec<T, U: HttpProtocolErrorOr<T>>(
    nameSuffix: String,
    private val codec: LocalPrimaryMessageCodec<T>,
): LocalPrimaryMessageCodec<U>(nameSuffix) {
    protected abstract fun emptyFailureInstance(): U
    protected abstract fun ofError(err: HttpProtocolError): U
    protected abstract fun ofSuccess(succ: T): U

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

    override fun ofSuccess(succ: JsonObject): HttpProtocolErrorOrJson {
        return HttpProtocolErrorOrJson.ofSuccess(succ)
    }
}

class HttpProtocolErrorOrReportDataList private constructor(
    error: HttpProtocolError?,
    success: ReportDataList?
): HttpProtocolErrorOr<ReportDataList>(error, success) {
    companion object {
        fun ofError(error: HttpProtocolError): HttpProtocolErrorOrReportDataList {
            return HttpProtocolErrorOrReportDataList(error, null)
        }

        fun ofSuccess(success: ReportDataList): HttpProtocolErrorOrReportDataList {
            return HttpProtocolErrorOrReportDataList(null, success)
        }
    }

    constructor(): this(HttpProtocolError(), null)
}

class HttpProtocolErrorOrReportDataListCodec: HttpProtocolErrorOrCodec<ReportDataList, HttpProtocolErrorOrReportDataList>(
    "HttpProtocolErrorOrReportDataList",
    ReportDataListCodec()
) {
    override fun emptyFailureInstance(): HttpProtocolErrorOrReportDataList {
        return HttpProtocolErrorOrReportDataList()
    }

    override fun ofError(err: HttpProtocolError): HttpProtocolErrorOrReportDataList {
        return HttpProtocolErrorOrReportDataList.ofError(err)
    }

    override fun ofSuccess(succ: ReportDataList): HttpProtocolErrorOrReportDataList {
        return HttpProtocolErrorOrReportDataList.ofSuccess(succ)
    }
}

fun registerMessageCodecs(vertx: Vertx) {
    val eventBus = vertx.eventBus()
    eventBus
        .registerDefaultCodec(RegisterQueryRequest::class.java, RegisterQueryRequestCodec())
        .registerDefaultCodec(UnregisterQueryRequest::class.java, UnregisterQueryRequestCodec())
        .registerDefaultCodec(UnpackDataRequest::class.java, UnpackDataRequestCodec())
        .registerDefaultCodec(ReportData::class.java, ReportDataCodec())
        .registerDefaultCodec(ReportDataList::class.java, ReportDataListCodec())
        .registerDefaultCodec(HttpProtocolErrorOrJson::class.java, HttpProtocolErrorOrJsonCodec())
        .registerDefaultCodec(HttpProtocolErrorOrReportDataList::class.java, HttpProtocolErrorOrReportDataListCodec())
}

private fun addressForQueryServerQueryAtOffset(verticleOffset: Int): String {
    return "thorium.query.server.$verticleOffset.query"
}

private fun addressForQueryServerDataAtOffset(verticleOffset: Int): String {
    return "thorium.query.server.$verticleOffset.data"
}

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

private var queryServerDataAddresses: Array<String>? = null
fun addressForQueryServerData(sharedData: SharedData, verticleOffset: Int): String {
    var currentQueryServerDataAddresses = queryServerDataAddresses
    if (currentQueryServerDataAddresses == null) {
        currentQueryServerDataAddresses = Array(getQueryThreads(sharedData)) { addressForQueryServerDataAtOffset(it) }
        queryServerDataAddresses = currentQueryServerDataAddresses
    }
    return currentQueryServerDataAddresses[verticleOffset]
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
