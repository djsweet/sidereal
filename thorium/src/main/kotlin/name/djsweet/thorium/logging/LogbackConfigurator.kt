package name.djsweet.thorium.logging

import ch.qos.logback.classic.AsyncAppender
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.*
import ch.qos.logback.core.ConsoleAppender
import ch.qos.logback.core.spi.ContextAwareBase

class LogbackConfigurator: ContextAwareBase(), Configurator {
    override fun configure(context: LoggerContext?): Configurator.ExecutionStatus {
        if (context == null) {
            return Configurator.ExecutionStatus.INVOKE_NEXT_IF_ANY
        }

        Runtime.getRuntime().addShutdownHook(Thread {
            context.stop()
        })

        // TODO: Attempt to set the Managed Diagnostic Context

        val encoder = JsonLogEncoder()
        encoder.context = context
        encoder.start()

        val consoleAppender = ConsoleAppender<ILoggingEvent>()
        consoleAppender.context = context
        consoleAppender.encoder = encoder
        consoleAppender.name = "console"
        consoleAppender.start()

        val asyncAppender = AsyncAppender()
        asyncAppender.context = context
        asyncAppender.addAppender(consoleAppender)

        asyncAppender.start()

        val rootLogger = context.getLogger(Logger.ROOT_LOGGER_NAME)
        rootLogger.addAppender(asyncAppender)
        // The default behavior for the rootLogger is to start at DEBUG, but this causes excess logging in
        // some of the automated tests when we involve a Vertx runtime. So, we'll set this to INFO here,
        // and let App.kt reset it as soon as CliKt hands off to the `run` method.
        rootLogger.level = Level.INFO

        return Configurator.ExecutionStatus.DO_NOT_INVOKE_NEXT_IF_ANY
    }
}