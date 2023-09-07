package name.djsweet.thorium

import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonObject
import io.vertx.core.shareddata.ClusterSerializable
import io.vertx.core.shareddata.Shareable
import io.vertx.core.shareddata.SharedData

fun appendStringAsUnicode(s: String, b: Buffer): Buffer {
    val stringBytes = convertStringToByteArray(s)
    b.appendInt(stringBytes.size)
    return b.appendBytes(stringBytes)
}

data class RegisterQueryRequest(
    var channel: String,
    var clientID: String,
    var queryParams: Map<String, List<String>>,
    var returnAddress: String,
): Shareable, ClusterSerializable {
    constructor(): this("", "", mapOf(), "")

    override fun writeToBuffer(buffer: Buffer?) {
        if (buffer == null) {
            return
        }
        appendStringAsUnicode(this.channel, buffer)
        appendStringAsUnicode(this.clientID, buffer)
        appendStringAsUnicode(this.returnAddress, buffer)
        buffer.appendInt(this.queryParams.size)
        for ((queryParamKey, queryParamValues) in this.queryParams) {
            appendStringAsUnicode(queryParamKey, buffer)
            buffer.appendInt(queryParamValues.size)
            for (queryParamValue in queryParamValues) {
                appendStringAsUnicode(queryParamValue, buffer)
            }
        }
    }

    override fun readFromBuffer(pos: Int, buffer: Buffer?): Int {
        if (buffer == null) {
            return pos
        }
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
        this.channel = channel
        this.clientID = clientID
        this.queryParams = queryParams
        this.returnAddress = returnAddress
        return lastPos
    }
}

data class UnregisterQueryRequest(
    var channel: String,
    var clientID: String,
): Shareable, ClusterSerializable {
    constructor(): this("", "")

    override fun writeToBuffer(buffer: Buffer?) {
        if (buffer == null) {
            return
        }
        appendStringAsUnicode(this.channel, buffer)
        appendStringAsUnicode(this.clientID, buffer)
    }

    override fun readFromBuffer(pos: Int, buffer: Buffer?): Int {
        if (buffer == null) {
            return pos
        }
        val channelPos = pos + 4
        val channelLength = buffer.getInt(pos)
        val clientIDLengthPos = channelPos + channelLength
        val channel = buffer.getString(channelPos, clientIDLengthPos, "utf-8")
        val clientIDLength = buffer.getInt(clientIDLengthPos)
        val clientIDPos = clientIDLengthPos + 4
        val endPos = clientIDPos + clientIDLength
        val clientID = buffer.getString(clientIDPos, endPos, "utf-8")
        this.channel = channel
        this.clientID = clientID
        return endPos
    }
}

data class UnpackDataRequest(
    var channel: String,
    var entries: List<Pair<String, JsonObject>>,
): Shareable, ClusterSerializable {
    constructor(): this("", listOf())

    override fun writeToBuffer(buffer: Buffer?) {
        if (buffer == null) {
            return
        }
        appendStringAsUnicode(this.channel, buffer)
        buffer.appendInt(this.entries.size)
        for ((idempotencyKey, data) in this.entries) {
            appendStringAsUnicode(idempotencyKey, buffer)
            data.writeToBuffer(buffer)
        }
    }

    override fun readFromBuffer(pos: Int, buffer: Buffer?): Int {
        if (buffer == null) {
            return pos
        }
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
        this.channel = channel
        this.entries = entries
        return lastPos
    }
}

data class ReportData(
    var channel: String,
    var idempotencyKey: String,
    var queryableScalarData: ShareableQPTrieOfByteArrays,
    var queryableArrayData: ShareableQPTrieOfByteArrayLists,
    var actualData: String,
): Shareable, ClusterSerializable {
    constructor(): this(
        "",
        "",
        ShareableQPTrieOfByteArrays(),
        ShareableQPTrieOfByteArrayLists(),
        ""
    )

    override fun writeToBuffer(buffer: Buffer?) {
        if (buffer == null) {
            return
        }
        appendStringAsUnicode(this.channel, buffer)
        appendStringAsUnicode(this.idempotencyKey, buffer)
        this.queryableScalarData.writeToBuffer(buffer)
        this.queryableArrayData.writeToBuffer(buffer)
        appendStringAsUnicode(this.actualData, buffer)
    }

    override fun readFromBuffer(pos: Int, buffer: Buffer?): Int {
        if (buffer == null) {
            return pos
        }
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
        this.channel = channel
        this.idempotencyKey = idempotencyKey
        this.queryableScalarData = queryableScalarData
        this.queryableArrayData = queryableArrayData
        this.actualData = actualData
        return finalPos
    }
}

data class ReportDataList(
    var entries: List<ReportData>
): Shareable, ClusterSerializable {
    constructor(): this(listOf())

    override fun writeToBuffer(buffer: Buffer?) {
        if (buffer == null) {
            return
        }
        val entries = this.entries
        buffer.appendInt(entries.size)
        for (entry in entries) {
            entry.writeToBuffer(buffer)
        }
    }

    override fun readFromBuffer(pos: Int, buffer: Buffer?): Int {
        if (buffer == null) {
            return pos
        }
        val entriesPos = pos + 4
        val entriesCount = buffer.getInt(pos)
        val result = mutableListOf<ReportData>()
        var endPos = entriesPos
        for (i in 0 until entriesCount) {
            val nextEntry = ReportData()
            endPos = nextEntry.readFromBuffer(endPos, buffer)
            result.add(nextEntry)
        }
        this.entries = result
        return endPos
    }
}

class HttpProtocolErrorOrReportDataList private constructor(
    error: HttpProtocolError?,
    success: ReportDataList?
): HttpProtocolErrorOrVertxShareable<ReportDataList, HttpProtocolErrorOrReportDataList>(error, success) {
    companion object {
        fun ofError(error: HttpProtocolError): HttpProtocolErrorOrReportDataList {
            return HttpProtocolErrorOrReportDataList(error, null)
        }

        fun ofSuccess(success: ReportDataList): HttpProtocolErrorOrReportDataList {
            return HttpProtocolErrorOrReportDataList(null, success)
        }
    }

    constructor(): this(HttpProtocolError(), null)

    override fun self(): HttpProtocolErrorOrReportDataList {
        return this
    }

    override fun successInstance(): ReportDataList {
        return ReportDataList()
    }
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
