package name.djsweet.thorium.logging

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.encoder.EncoderBase
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.json.jsonArrayOf
import io.vertx.kotlin.core.json.jsonObjectOf
import name.djsweet.thorium.convertStringToByteArray
import name.djsweet.thorium.wallNowAsString
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.concurrent.atomic.AtomicReference

class JsonLogEncoder: EncoderBase<ILoggingEvent>() {
    private data class FormattedDate(
        val timestamp: Long
    ) {
        val string: String
        init {
            val instant = Instant.ofEpochMilli(this.timestamp)
            this.string = LocalDateTime
                .ofInstant(instant, ZoneId.systemDefault())
                .atOffset(ZoneOffset.UTC)
                .format(DateTimeFormatter.ISO_ZONED_DATE_TIME)
        }
    }

    private data class ThrowableInfo(val className: String, val message: String)

    companion object {
        private fun baseJsonEvent(timestamp: String, level: String, logName: String, message: String) = jsonObjectOf(
            "timestamp" to timestamp,
            "level" to level,
            "logName" to logName,
            "message" to message
        )

        fun baseJsonErrorEventForRightNow(logName: String, message: String) =
            baseJsonEvent(wallNowAsString(), Level.ERROR.toString(), logName, message)

        fun addPropertiesToJsonLogInPlace(log: JsonObject, propertyMap: JsonObject): Boolean {
            val hasProperties = propertyMap.size() > 0
            if (hasProperties) {
                log.put("properties", propertyMap)
            }
            return hasProperties
        }

        private fun baseJsonThrowable(info: ThrowableInfo, cause: ThrowableInfo?): JsonObject {
            val basis = jsonObjectOf(
                "name" to info.className,
                "message" to info.message
            )
            if (cause != null) {
                basis.put("cause", jsonObjectOf(
                    "name" to cause.className,
                    "message" to cause.message
                ))
            }
            return basis
        }

        private fun jsonForStackTraceElement(elem: StackTraceElement) = jsonObjectOf(
            "class" to elem.className,
            "method" to elem.methodName,
            "file" to elem.fileName,
            "line" to elem.lineNumber
        )

        fun jsonForException(logName: String, message: String, e: Exception): JsonObject {
            val exceptionCause = e.cause
            val throwableMap = baseJsonThrowable(
                ThrowableInfo(e.javaClass.name, e.message ?: "(No message)"),
                if (exceptionCause == null) {
                    null
                } else {
                    ThrowableInfo(exceptionCause.javaClass.name, exceptionCause.message ?: "(No message)")
                }
            )
            val stackFramesArray = jsonArrayOf()
            for (frame in e.stackTrace) {
                stackFramesArray.add(jsonForStackTraceElement(frame))
            }
            throwableMap.put("stacktrace", stackFramesArray)

            return baseJsonEvent(
                wallNowAsString(),
                Level.ERROR.toString(),
                logName,
                message
            ).put("throwable", throwableMap)
        }

        private val logName = JsonLogEncoder::class.java.name
    }

    private val lastFormattedDate = AtomicReference<FormattedDate?>()
    private val emptyBytes = byteArrayOf()

    override fun headerBytes(): ByteArray {
        return this.emptyBytes
    }

    override fun footerBytes(): ByteArray {
        return this.emptyBytes
    }


    private fun encodeNonNullImpl(event: ILoggingEvent): ByteArray {
        var lastTimestamp = this.lastFormattedDate.get()
        if (event.timeStamp != lastTimestamp?.timestamp) {
            val nextTimestamp = FormattedDate(event.timeStamp)
            this.lastFormattedDate.compareAndSet(lastTimestamp, nextTimestamp)
            lastTimestamp = nextTimestamp
        }

        val levelString = event.level?.toString() ?: "UNKNOWN"
        val jsonEvent = baseJsonEvent(lastTimestamp.string, levelString, event.loggerName, event.formattedMessage)

        val propertyMap = jsonObjectOf()
        if (event.mdcPropertyMap != null) {
            for ((key, value) in event.mdcPropertyMap) {
                propertyMap.put(key, value)
            }
        }
        if (event.keyValuePairs != null) {
            for (kvp in event.keyValuePairs) {
                propertyMap.put(kvp.key, kvp.value)
            }
        }

        val addedProperties = addPropertiesToJsonLogInPlace(jsonEvent, propertyMap)
        if (!addedProperties && event.argumentArray?.isNotEmpty() == true) {
            jsonEvent.put("arguments", event.argumentArray.map {
                when (it) {
                    null -> null
                    is Boolean -> it
                    is Number -> it
                    is String -> it
                    else -> it.toString()
                }
            })
        }
        val throwable = event.throwableProxy
        if (throwable != null) {
            val throwableCause = throwable.cause
            val throwableMap = baseJsonThrowable(
                ThrowableInfo(throwable.className, throwable.message),
                if (throwableCause == null) {
                    null
                } else {
                    ThrowableInfo(throwableCause.className, throwableCause.message)
                }
            )

            val stackTraceElements = throwable.stackTraceElementProxyArray
            if (stackTraceElements != null) {
                val stackFramesArray = jsonArrayOf()
                val stopAtFrame = (stackTraceElements.size - throwable.commonFrames).coerceAtLeast(0)
                for (i in 0 until stopAtFrame) {
                    stackFramesArray.add(
                        jsonForStackTraceElement(stackTraceElements[i].stackTraceElement)
                    )
                }

                if (stackFramesArray.size() > 0) {
                    throwableMap.put("stacktrace", stackFramesArray)
                }
            }

            jsonEvent.put("throwable", throwableMap)
        }

        return convertStringToByteArray(jsonEvent.encode() + "\n")
    }

    override fun encode(event: ILoggingEvent?): ByteArray {
        if (event == null) {
            return this.emptyBytes
        }
        return try {
            this.encodeNonNullImpl(event)
        } catch (e: Exception) {
            val logError = jsonForException(logName, "Could not render log event as JSON", e)
            convertStringToByteArray(logError.encode() + "\n")
        }
    }
}