// SPDX-FileCopyrightText: 2023 Dani Sweet <sidereal@djsweet.name>
//
// SPDX-License-Identifier: MIT

package name.djsweet.sidereal

import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.eventbus.MessageCodec
import io.vertx.core.json.JsonObject
import io.vertx.core.shareddata.ClusterSerializable
import io.vertx.core.tracing.TracingPolicy
import io.vertx.kotlin.core.json.jsonObjectOf

val localRequestOptions: DeliveryOptions = DeliveryOptions()
    .setLocalOnly(true)
    .setTracingPolicy(TracingPolicy.IGNORE)

abstract class LocalPrimaryMessageCodec<T>(
    nameSuffix: String
): MessageCodec<T, T> {
    private val fullName: String
    init {
        this.fullName = "thorium.message.codecs.${nameSuffix}"
    }

    protected abstract fun emptyInstance(): T

    override fun systemCodecID(): Byte {
        return -1
    }

    override fun name(): String {
        return this.fullName
    }

    override fun transform(s: T): T {
        return s ?: this.emptyInstance()
    }

    protected abstract fun encodeToWireNonNullBuffer(buffer: Buffer, s: T)
    protected abstract fun decodeFromWireNonNullBuffer(pos: Int, buffer: Buffer): T

    override fun encodeToWire(buffer: Buffer?, s: T) {
        if (buffer == null || s == null) {
            return
        }
        this.encodeToWireNonNullBuffer(buffer, s)
    }

    override fun decodeFromWire(pos: Int, buffer: Buffer?): T {
        if (buffer == null) {
            return this.emptyInstance()
        }
        return this.decodeFromWireNonNullBuffer(pos, buffer)
    }
}

abstract class TrivialClusterSerializableCodec<T: ClusterSerializable>(
    nameSuffix: String
): LocalPrimaryMessageCodec<T>("cluster.serializable.$nameSuffix") {
    override fun encodeToWireNonNullBuffer(buffer: Buffer, s: T) {
        s.writeToBuffer(buffer)
    }

    override fun decodeFromWireNonNullBuffer(pos: Int, buffer: Buffer): T {
        val result = this.emptyInstance()
        result.readFromBuffer(pos, buffer)
        return result
    }
}

class TrivialJsonObjectCodec: TrivialClusterSerializableCodec<JsonObject>("JsonObject") {
    override fun emptyInstance(): JsonObject {
        return jsonObjectOf()
    }
}