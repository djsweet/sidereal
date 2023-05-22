package name.djsweet.thorium

import io.vertx.core.*
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
}

fun main(args: Array<String>) {
    val exitCode = CommandLine(ThoriumCommand()).execute(*args)
    exitProcess(exitCode)
}
