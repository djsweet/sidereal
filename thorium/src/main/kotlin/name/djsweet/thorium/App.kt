package name.djsweet.thorium

import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import io.vertx.core.*
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import name.djsweet.thorium.servers.registerQueryServer
import name.djsweet.thorium.servers.registerWebServer
import picocli.CommandLine
import picocli.CommandLine.Command
import kotlin.system.exitProcess

@Command(
    name = "thorium",
    version = ["0.8"],
    description = ["A reactive query server"],
    mixinStandardHelpOptions = true
)
internal class ThoriumCommand {
    @Command(
        name="kvp-byte-budget",
        description = ["Determines the maximum key/value pair byte budget to prevent tree operations from throwing StackOverflowError"]
    )
    fun determineMaxSafeKeyValueSize(): Int {
        val vertx = Vertx.vertx()
        println(maxSafeKeyValueSizeSync(vertx))
        return 0
    }

    @Command(
        name="print-system-properties",
        description = ["Prints all known System properties."]
    )
    fun printSystemProperties(): Int {
        val properties = System.getProperties()
        for (property in properties) {
            println("${property.key}=${property.value}")
        }
        return 0
    }

    @Command(
        name="serve",
        description = ["Runs the server"]
    )
    fun serve(): Int {
        val meterRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        val vertx = Vertx.vertx()
        val initialSafeKeyValueSize = maxSafeKeyValueSizeSync(vertx)
        println("Byte budget for key/value pairs is $initialSafeKeyValueSize")
        establishByteBudget(vertx.sharedData(), initialSafeKeyValueSize)
        registerMessageCodecs(vertx)
        val queryThreads = getQueryThreads(vertx.sharedData())
        val counters = GlobalCounterContext(queryThreads)
        return runBlocking {
            val queryDeploymentIDs = registerQueryServer(
                vertx,
                counters,
                meterRegistry,
                0,
                queryThreads,
                0,
                getTranslatorThreads(vertx.sharedData())
            )
            try {
                val webServerDeploymentIDs = registerWebServer(
                    vertx,
                    counters,
                    meterRegistry,
                    getWebServerThreads(vertx.sharedData())
                )
                val allDeploymentIDs = queryDeploymentIDs.union(webServerDeploymentIDs)
                println("Listening on :${getServerPort(vertx.sharedData())}")
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
            0
        }
    }
}

fun main(args: Array<String>) {
    val exitCode = CommandLine(ThoriumCommand()).execute(*args)
    exitProcess(exitCode)
}
