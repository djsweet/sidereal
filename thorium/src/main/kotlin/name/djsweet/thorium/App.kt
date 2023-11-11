package name.djsweet.thorium

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import io.micrometer.core.instrument.Gauge
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import io.vertx.core.*
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import name.djsweet.thorium.servers.registerQueryServer
import name.djsweet.thorium.servers.registerWebServer
import kotlin.system.exitProcess

// FIXME: Set this elsewhere
private const val versionString = "0.8.0"

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
    help =" Runs the server"
) {
    override fun run() {
        val meterRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        val vertx = Vertx.vertx()
        val initialSafeKeyValueSize = maxSafeKeyValueSizeSync(vertx)
        println("Byte budget for key/value pairs is $initialSafeKeyValueSize")

        val sharedData = vertx.sharedData()
        val config = GlobalConfig(sharedData)
        config.establishByteBudget(initialSafeKeyValueSize)
        registerMessageCodecs(vertx)

        val queryThreads = config.queryThreads
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
                println("Listening on :${config.serverPort}")
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
    help = "Reactive queries over CloudEvents",
) {
    private val version by option(help="Show the version and exit").flag()

    override fun run() {
        if (this.version) {
            println(versionString)
            exitProcess(0)
        }
    }
}

fun main(args: Array<String>) = ThoriumCommand().subcommands(KvpByteBudgetCommand(), ServeCommand()).main(args)
