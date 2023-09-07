package name.djsweet.thorium

import io.vertx.core.*
import io.vertx.core.http.HttpServer
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
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
        name="max-safe-key-value-size",
        description = ["Determines the maximum key size before tree operations begin throwing StackOverflowError"]
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
        val vertx = Vertx.vertx()
        val initialSafeKeyValueSize = maxSafeKeyValueSizeSync(vertx)
        establishByteBudget(vertx.sharedData(), initialSafeKeyValueSize)
        registerMessageCodecs(vertx)
        return runBlocking {
            val queryDeploymentIDs = registerQueryServer(
                vertx,
                0,
                getQueryThreads(vertx.sharedData()),
                0,
                getTranslatorThreads(vertx.sharedData())
            )
            try {
                val webServerDeploymentIDs = registerWebServer(vertx, getWebServerThreads(vertx.sharedData()))
                val eventLoop = vertx.nettyEventLoopGroup()
                println("Listening on :${getServerPort(vertx.sharedData())}")
                while (!eventLoop.isTerminated) {
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
