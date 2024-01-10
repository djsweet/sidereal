// SPDX-FileCopyrightText: 2023 Dani Sweet <thorium@djsweet.name>
//
// SPDX-License-Identifier: MIT

package name.djsweet.sidereal

import ch.qos.logback.classic.Level
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.context
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.output.MordantHelpFormatter
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.choice
import com.github.ajalt.clikt.parameters.types.int
import io.micrometer.core.instrument.Gauge
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import io.vertx.core.*
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.json.jsonObjectOf
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import name.djsweet.sidereal.logging.JsonLogEncoder
import name.djsweet.sidereal.servers.registerQueryServer
import name.djsweet.sidereal.servers.registerWebServer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import kotlin.system.exitProcess

// FIXME: Set this elsewhere
private const val versionString = "0.8.0"
private const val envVarPrefix = "THORIUM_"

internal class KvpByteBudgetCommand: CliktCommand(
    help="Determines the maximum key/value pair byte budget to prevent tree operations from throwing StackOverflowError"
) {
    override fun run() {
        val vertx = Vertx.vertx()
        try {
            println(name.djsweet.sidereal.maxSafeKeyValueSizeSync(vertx))
        } finally {
            runBlocking { vertx.close().await() }
        }
        System.out.flush()
        exitProcess(0)
    }
}

internal class ServeCommand: CliktCommand(
    help =" Runs the Thorium Reactive Query Server"
) {
    companion object {
        private fun logLevelFromString(levelString: String): Level = when (levelString) {
            "trace" -> Level.TRACE
            "debug" -> Level.DEBUG
            "info" -> Level.INFO
            "warn" -> Level.WARN
            "error" -> Level.ERROR
            else -> Level.INFO
        }
    }

    private val logger = LoggerFactory.getLogger(name.djsweet.sidereal.ServeCommand::class.java)

    private val serverPort by option(
        help = "Listen to this TCP port",
        envvar = "${name.djsweet.sidereal.envVarPrefix}SERVER_PORT"
    ).int().default(name.djsweet.sidereal.GlobalConfig.Companion.defaultServerPort)

    private val sourceName by option(
        help = "Reports this string as the 'source' for all internally generated CloudEvents",
        envvar = "${name.djsweet.sidereal.envVarPrefix}SOURCE_NAME"
    ).default(name.djsweet.sidereal.GlobalConfig.Companion.defaultCloudEventSource)

    private val logLevel by option(
        help = "Sets the minimum logging severity",
        envvar = "${name.djsweet.sidereal.envVarPrefix}LOG_LEVEL"
    ).choice("trace", "debug", "info", "warn", "error").default("info")

    private val routerThreads by option(
        help = "Number of threads to use for routing events to queries. Expected to be between 1 and the number of logical processors available",
        envvar = "${name.djsweet.sidereal.envVarPrefix}ROUTER_THREADS"
    ).int().default(name.djsweet.sidereal.GlobalConfig.Companion.defaultRouterThreads)

    private val translatorThreads by option(
        help = "Number of threads to use for translating ingested data into its index representation. Expected to be between 1 and the number of logical processors available",
        envvar = "${name.djsweet.sidereal.envVarPrefix}TRANSLATOR_THREADS"
    ).int().default(name.djsweet.sidereal.GlobalConfig.Companion.defaultTranslatorThreads)

    private val webServerThreads by option(
        help = "Number of threads to use for the web server. Expected to be between 1 and twice the number of logical processors available",
        envvar = "${name.djsweet.sidereal.envVarPrefix}WEB_SERVER_THREADS"
    ).int().default(name.djsweet.sidereal.GlobalConfig.Companion.defaultWebServerThreads)

    private val maxBodySizeBytes by option(
        help = "Maximum size of all HTTP bodies. Any HTTP request with a body size greater than this value will result in an HTTP 413 result",
        envvar = "${name.djsweet.sidereal.envVarPrefix}MAX_BODY_SIZE_BYTES"
    ).int().default(name.djsweet.sidereal.GlobalConfig.Companion.defaultMaxBodySize)

    private val maxIdempotencyKeys by option(
        help = "Maximum number of idempotency keys to store before forgetting the oldest key",
        envvar = "${name.djsweet.sidereal.envVarPrefix}MAX_IDEMPOTENCY_KEYS"
    ).int().default(name.djsweet.sidereal.GlobalConfig.Companion.defaultMaxIdempotencyKeys)

    private val maxJsonParsingRecursion by option(
        help = "Maximum stack recursion used by JSON parsing. This is only a performance optimization, and does not prevent deeper JSON nesting than the configured value",
        envvar = "${name.djsweet.sidereal.envVarPrefix}MAX_JSON_PARSING_RECURSION"
    ).int().default(name.djsweet.sidereal.GlobalConfig.Companion.defaultMaxJsonParsingRecursion)

    private val maxOutstandingEventsPerRouterThread by option(
        help = "Number of outstanding events per query thread. The global maximum is calculated by multiplying this by the number of query threads",
        envvar = "${name.djsweet.sidereal.envVarPrefix}MAX_OUTSTANDING_EVENTS_PER_ROUTER_THREAD"
    ).int().default(name.djsweet.sidereal.GlobalConfig.Companion.defaultMaxOutstandingEventsPerRouterThread)

    private val maxQueryTerms by option(
        help = "Maximum number of terms in a query",
        envvar = "${name.djsweet.sidereal.envVarPrefix}MAX_QUERY_TERMS"
    ).int().default(name.djsweet.sidereal.GlobalConfig.Companion.defaultMaxQueryTerms)

    private val bodyTimeoutMS by option(
        help = "Maximum time (in milliseconds) to allow for HTTP body receipt",
        envvar = "${name.djsweet.sidereal.envVarPrefix}BODY_TIMEOUT_MS"
    ).int().default(name.djsweet.sidereal.GlobalConfig.Companion.defaultBodyTimeoutMS)

    private val idempotencyExpirationMS by option(
        help = "Lifetime (in milliseconds) of an idempotency key",
        envvar = "${name.djsweet.sidereal.envVarPrefix}IDEMPOTENCY_EXPIRATION_MS"
    ).int().default(name.djsweet.sidereal.GlobalConfig.Companion.defaultIdempotencyExpirationMS)

    private val tcpIdleTimeoutMS by option(
        help = "Maximum time (in milliseconds) to wait for TCP activity on an open connection",
        envvar = "${name.djsweet.sidereal.envVarPrefix}TCP_IDLE_TIMEOUT_MS"
    ).int().default(name.djsweet.sidereal.GlobalConfig.Companion.defaultTcpIdleTimeoutMS)

    private fun runWithVertx(vertx: Vertx) {
        val initialSafeKeyValueSize = name.djsweet.sidereal.maxSafeKeyValueSizeSync(vertx)

        val logger = this.logger
        logger.atInfo()
            .setMessage("Byte budget for key/value pairs is {}")
            .addArgument(initialSafeKeyValueSize)
            .addKeyValue("initialSafeKeyValueSize", initialSafeKeyValueSize)
            .addKeyValue("maxMemoryUsage", Runtime.getRuntime().maxMemory())
            .log()

        val sharedData = vertx.sharedData()
        val config = name.djsweet.sidereal.GlobalConfig(sharedData)
        config.establishByteBudget(initialSafeKeyValueSize)

        config.serverPort = this.serverPort
        config.sourceName = this.sourceName
        config.idempotencyExpirationMS = this.idempotencyExpirationMS
        config.bodyTimeoutMS = this.bodyTimeoutMS
        config.tcpIdleTimeoutMS = this.tcpIdleTimeoutMS

        config.maxIdempotencyKeys = this.maxIdempotencyKeys
        config.maxQueryTerms = this.maxQueryTerms
        config.maxJsonParsingRecursion = this.maxJsonParsingRecursion
        config.maxBodySizeBytes = this.maxBodySizeBytes
        config.maxOutstandingEventsPerRouterThread = this.maxOutstandingEventsPerRouterThread

        config.routerThreads = this.routerThreads
        config.translatorThreads = this.translatorThreads
        config.webServerThreads = this.webServerThreads

        name.djsweet.sidereal.registerMessageCodecs(vertx)

        val queryThreads = config.routerThreads
        val counters = name.djsweet.sidereal.GlobalCounterContext(queryThreads)
        val meterRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)

        Gauge.builder(name.djsweet.sidereal.outstandingEventsCountName) { counters.getOutstandingEventCount() }
            .description(name.djsweet.sidereal.outstandingEventsCountDescription)
            .register(meterRegistry)

        Gauge.builder(name.djsweet.sidereal.byteBudgetGaugeName) { config.byteBudget }
            .description(name.djsweet.sidereal.byteBudgetGaugeDescription)
            .register(meterRegistry)

        return runBlocking {
            val queryDeploymentIDs = registerQueryServer(
                vertx,
                config,
                counters,
                meterRegistry,
            )
            try {
                val webServerDeploymentIDs = registerWebServer(
                    vertx,
                    config,
                    counters,
                    meterRegistry,
                )
                val allDeploymentIDs = queryDeploymentIDs.union(webServerDeploymentIDs)

                logger.atInfo()
                    .setMessage("Listening")
                    .addKeyValue("serverPort", config.serverPort)
                    .log()

                while (vertx.deploymentIDs().containsAll(allDeploymentIDs)) {
                    delay(250)
                }
                for (deploymentID in webServerDeploymentIDs) {
                    vertx.undeploy(deploymentID)
                }
            } finally {
                for (deploymentID in queryDeploymentIDs) {
                    vertx.undeploy(deploymentID).await()
                }
            }
        }
    }

    override fun run() {
        val rootLogger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME) as ch.qos.logback.classic.Logger
        rootLogger.level = name.djsweet.sidereal.ServeCommand.Companion.logLevelFromString(this.logLevel)

        val workerPoolSize = (this.routerThreads + this.webServerThreads)
            .coerceAtMost(name.djsweet.sidereal.availableProcessors())
        val nonblockingPoolSize = webServerThreads.coerceAtMost(2 * name.djsweet.sidereal.availableProcessors())
        val opts = VertxOptions().setWorkerPoolSize(workerPoolSize).setEventLoopPoolSize(nonblockingPoolSize)

        val vertx = Vertx.vertx(opts)
        var exitCode = 0
        try {
            this.runWithVertx(vertx)
        } catch (e: Exception) {
            rootLogger.level = Level.OFF
            rootLogger.detachAndStopAllAppenders()
            // This is really a last-ditch log that we can't afford to lose. The logging system is otherwise
            // asynchronous, which means that if we used it, we'd lose this log. But we also can't have it continue
            // to run while we're trying to print this, either, which is why it's being shut off here.
            val logError: JsonObject
            if (e is java.net.BindException) {
                logError = JsonLogEncoder.baseJsonErrorEventForRightNow(
                    this.logger.name,
                    "Port already in use: ${this.serverPort}"
                )
                JsonLogEncoder.addPropertiesToJsonLogInPlace(
                    logError,
                    jsonObjectOf("serverPort" to this.serverPort)
                )
            } else {
                logError = JsonLogEncoder.jsonForException(
                    this.logger.name,
                    e.message ?: "Exception raised in main thread",
                    e
                )
            }
            println(logError.encode())
            exitCode = 1
        } finally {
            runBlocking { vertx.close().await() }
        }
        System.out.flush()
        exitProcess(exitCode)
    }
}

internal class ThoriumCommand: CliktCommand(
    help = "Reactive queries over CloudEvents"
) {
    private val version by option(help="Show the version and exit").flag()

    init {
        context {
            helpFormatter = { MordantHelpFormatter(it, showDefaultValues = true) }
        }
    }

    override fun run() {
        if (this.version) {
            println(name.djsweet.sidereal.versionString)
            System.out.flush()
            exitProcess(0)
        }
    }
}

fun main(args: Array<String>) = name.djsweet.sidereal.ThoriumCommand()
    .subcommands(name.djsweet.sidereal.KvpByteBudgetCommand(), name.djsweet.sidereal.ServeCommand()).main(args)