package name.djsweet.thorium

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
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import name.djsweet.thorium.servers.registerQueryServer
import name.djsweet.thorium.servers.registerWebServer
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
        println(maxSafeKeyValueSizeSync(vertx))
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

    private val logger = LoggerFactory.getLogger(ServeCommand::class.java)

    private val serverPort by option(
        help = "Listen to this TCP port",
        envvar = "${envVarPrefix}SERVER_PORT"
    ).int().default(GlobalConfig.defaultServerPort)

    private val sourceName by option(
        help = "Reports this string as the 'source' for all internally generated CloudEvents",
        envvar = "${envVarPrefix}SOURCE_NAME"
    ).default(GlobalConfig.defaultCloudEventSource)

    private val logLevel by option(
        help = "Sets the minimum logging severity",
        envvar = "${envVarPrefix}LOG_LEVEL"
    ).choice("trace", "debug", "info", "warn", "error").default("info")

    private val routerThreads by option(
        help = "Number of threads to use for routing events to queries. Expected to be between 1 and the number of logical processors available",
        envvar = "${envVarPrefix}ROUTER_THREADS"
    ).int().default(GlobalConfig.defaultRouterThreads)

    private val translatorThreads by option(
        help = "Number of threads to use for translating ingested data into its index representation. Expected to be between 1 and the number of logical processors available",
        envvar = "${envVarPrefix}TRANSLATOR_THREADS"
    ).int().default(GlobalConfig.defaultTranslatorThreads)

    private val webServerThreads by option(
        help = "Number of threads to use for the web server. Expected to be between 1 and twice the number of logical processors available",
        envvar = "${envVarPrefix}WEB_SERVER_THREADS"
    ).int().default(GlobalConfig.defaultWebServerThreads)

    private val maxBodySizeBytes by option(
        help = "Maximum size of all HTTP bodies. Any HTTP request with a body size greater than this value will result in an HTTP 413 result",
        envvar = "${envVarPrefix}MAX_BODY_SIZE_BYTES"
    ).int().default(GlobalConfig.defaultMaxBodySize)

    private val maxIdempotencyKeys by option(
        help = "Maximum number of idempotency keys to store before forgetting the oldest key",
        envvar = "${envVarPrefix}MAX_IDEMPOTENCY_KEYS"
    ).int().default(GlobalConfig.defaultMaxIdempotencyKeys)

    private val maxJsonParsingRecursion by option(
        help = "Maximum stack recursion used by JSON parsing. This is only a performance optimization, and does not prevent deeper JSON nesting than the configured value",
        envvar = "${envVarPrefix}MAX_JSON_PARSING_RECURSION"
    ).int().default(GlobalConfig.defaultMaxJsonParsingRecursion)

    private val maxOutstandingEventsPerRouterThread by option(
        help = "Number of outstanding events per query thread. The global maximum is calculated by multiplying this by the number of query threads",
        envvar = "${envVarPrefix}MAX_OUTSTANDING_EVENTS_PER_ROUTER_THREAD"
    ).int().default(GlobalConfig.defaultMaxOutstandingEventsPerRouterThread)

    private val maxQueryTerms by option(
        help = "Maximum number of terms in a query",
        envvar = "${envVarPrefix}MAX_QUERY_TERMS"
    ).int().default(GlobalConfig.defaultMaxQueryTerms)

    private val bodyTimeoutMS by option(
        help = "Maximum time (in milliseconds) to allow for HTTP body receipt",
        envvar = "${envVarPrefix}BODY_TIMEOUT_MS"
    ).int().default(GlobalConfig.defaultBodyTimeoutMS)

    private val idempotencyExpirationMS by option(
        help = "Lifetime (in milliseconds) of an idempotency key",
        envvar = "${envVarPrefix}IDEMPOTENCY_EXPIRATION_MS"
    ).int().default(GlobalConfig.defaultIdempotencyExpirationMS)

    private val tcpIdleTimeoutMS by option(
        help = "Maximum time (in milliseconds) to wait for TCP activity on an open connection",
        envvar = "${envVarPrefix}TCP_IDLE_TIMEOUT_MS"
    ).int().default(GlobalConfig.defaultTcpIdleTimeoutMS)

    override fun run() {
        (LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME) as ch.qos.logback.classic.Logger).level = logLevelFromString(
            this.logLevel
        )
        val meterRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)

        val workerPoolSize = (this.routerThreads + this.webServerThreads)
            .coerceAtMost(availableProcessors())
        val nonblockingPoolSize = webServerThreads.coerceAtMost(2 * availableProcessors())
        val opts = VertxOptions().setWorkerPoolSize(workerPoolSize).setEventLoopPoolSize(nonblockingPoolSize)

        val vertx = Vertx.vertx(opts)
        val initialSafeKeyValueSize = maxSafeKeyValueSizeSync(vertx)

        val logger = this.logger
        logger.atInfo()
            .setMessage("Byte budget for key/value pairs is {}")
            .addArgument(initialSafeKeyValueSize)
            .addKeyValue("initialSafeKeyValueSize", initialSafeKeyValueSize)
            .addKeyValue("maxMemoryUsage", Runtime.getRuntime().maxMemory())
            .log()

        val sharedData = vertx.sharedData()
        val config = GlobalConfig(sharedData)
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

        registerMessageCodecs(vertx)

        val queryThreads = config.routerThreads
        val counters = GlobalCounterContext(queryThreads)

        Gauge.builder(outstandingEventsCountName) { counters.getOutstandingEventCount() }
            .description(outstandingEventsCountDescription)
            .register(meterRegistry)

        Gauge.builder(byteBudgetGaugeName) { config.byteBudget }
            .description(byteBudgetGaugeDescription)
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
                    vertx.undeploy(deploymentID)
                }
            }
        }
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
            println(versionString)
            exitProcess(0)
        }
    }
}

fun main(args: Array<String>) {
    // This property is meant only to be set at build-time and not at runtime in a normal JVM context.
    // Clearing this property successfully prevents JVM execution contexts from ever seeing this, because
    // according to the Java 7 Language Specification, Section 12.4.1, "When Initialization Occurs",
    //
    // Since we aren't referencing KeyValueSizeLimits.kt here, well before we call into it, and none of the
    // above occurrences have happened yet, KeyValueSizeLimits.kt won't be initialized on the JVM; it'll only
    // be initialized at build-time due to its inclusion as a build-time class in native-image.properties.
    System.clearProperty(buildOnlyKvpSafetyFactorPropertyName)

    return ThoriumCommand().subcommands(KvpByteBudgetCommand(), ServeCommand()).main(args)
}
