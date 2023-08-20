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

class HttpProtocolErrorOr<T> private constructor(
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

abstract class HttpProtocolErrorOrVertxShareable<
        T, U : HttpProtocolErrorOrVertxShareable<T, U>
> protected constructor(
    private var error: HttpProtocolError?,
    private var success: T?
): Shareable, ClusterSerializable where T : Shareable, T : ClusterSerializable {
    protected abstract fun successInstance(): T
    protected abstract fun self(): U

    override fun writeToBuffer(buffer: Buffer?) {
        if (buffer == null) {
            return
        }
        buffer.appendByte(if (this.error == null) { 1 } else { 0 })
        if (this.error == null) {
            this.success!!.writeToBuffer(buffer)
        } else {
            this.error!!.writeToBuffer(buffer)
        }
    }

    override fun readFromBuffer(pos: Int, buffer: Buffer?): Int {
        if (buffer == null) {
            return pos
        }
        val kind = buffer.getByte(pos)
        return if (kind == 0.toByte()) {
            val error = HttpProtocolError()
            val nextPos = error.readFromBuffer(pos + 1, buffer)
            this.error = error
            this.success = null
            nextPos
        } else {
            val success = this.successInstance()
            val nextPos = success.readFromBuffer(pos + 1, buffer)
            this.error = null
            this.success = success
            nextPos
        }
    }

    fun whenError(cb: (HttpProtocolError) -> Unit): U {
        val error = this.error ?: return this.self()
        cb(error)
        return this.self()
    }

    fun whenSuccess(cb: (T) -> Unit): U {
        val success = this.success ?: return this.self()
        cb(success)
        return this.self()
    }
}

class HttpProtocolErrorOrJson private constructor(
    error: HttpProtocolError?,
    success: JsonObject?,
): HttpProtocolErrorOrVertxShareable<JsonObject, HttpProtocolErrorOrJson>(error, success) {
    companion object {
        fun ofError(err: HttpProtocolError): HttpProtocolErrorOrJson {
            return HttpProtocolErrorOrJson(err, null)
        }

        fun ofSuccess(success: JsonObject): HttpProtocolErrorOrJson {
            return HttpProtocolErrorOrJson(null, success)
        }
    }
    constructor(): this(HttpProtocolError(), null)

    override fun self(): HttpProtocolErrorOrJson {
        return this
    }

    override fun successInstance(): JsonObject {
        return JsonObject()
    }
}