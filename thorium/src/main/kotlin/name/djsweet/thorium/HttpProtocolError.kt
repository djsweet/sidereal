package name.djsweet.thorium

import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonObject
import io.vertx.core.shareddata.ClusterSerializable
import io.vertx.core.shareddata.Shareable
import io.vertx.kotlin.core.json.*

const val emptyError = "empty"

data class HttpProtocolError(
    var statusCode: Int,
    var contents: JsonObject
): Shareable, ClusterSerializable {
    constructor() : this(400, json { obj("error" to emptyError) })

    override fun writeToBuffer(buffer: Buffer?) {
        if (buffer == null) {
            return
        }
        buffer.appendInt(this.statusCode)
        this.contents.writeToBuffer(buffer)
    }

    override fun readFromBuffer(pos: Int, buffer: Buffer?): Int {
        if (buffer == null) {
            return pos
        }
        val statusCode = buffer.getInt(pos)
        val contents = JsonObject()
        val nextPos = contents.readFromBuffer(pos + 4, buffer)
        this.statusCode = statusCode
        this.contents = contents
        return nextPos
    }
}

open class HttpProtocolErrorOr<T> protected constructor(
    private var error: HttpProtocolError?,
    private var success: T?
) {
    companion object {
        fun<T> ofError(err: HttpProtocolError): HttpProtocolErrorOr<T> {
            return HttpProtocolErrorOr(err, null)
        }

        fun<T> ofSuccess(success: T): HttpProtocolErrorOr<T> {
            return HttpProtocolErrorOr(null, success)
        }
    }

    fun whenError(cb: (HttpProtocolError) -> Unit): HttpProtocolErrorOr<T> {
        val error = this.error ?: return this
        cb(error)
        return this
    }

    fun whenSuccess(cb: (T) -> Unit): HttpProtocolErrorOr<T> {
        val success = this.success ?: return this
        cb(success)
        return this
    }
}
