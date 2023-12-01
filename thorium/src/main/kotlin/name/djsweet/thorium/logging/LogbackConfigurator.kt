package name.djsweet.thorium.logging

import ch.qos.logback.classic.AsyncAppender
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.*
import ch.qos.logback.core.ConsoleAppender
import ch.qos.logback.core.spi.ContextAwareBase

class LogbackConfigurator: ContextAwareBase(), Configurator {
    private var didInit = false

    @Synchronized
    override fun configure(context: LoggerContext?): Configurator.ExecutionStatus {
        if (this.didInit) {
            return Configurator.ExecutionStatus.DO_NOT_INVOKE_NEXT_IF_ANY
        }

        if (context == null) {
            this.didInit = true
            return Configurator.ExecutionStatus.INVOKE_NEXT_IF_ANY
        }

        Runtime.getRuntime().addShutdownHook(Thread {
            context.stop()
        })

        val self = this

        // This is unfortunately going to run at build-time in `native-image`, because Netty wants to set up logging
        // factories as part of its initialization, causing this to run; where instantiating JsonLogEncoder
        // trips up run-time initialization expectations; and instantiating AsyncAppender spawns a thread, something
        // `native-image` is extremely unhappy about. However, if we somehow trick the build into bypassing this,
        // we're stuck in a situation where something has technically already run us, we've already been set up,
        // and there would be no opportunity to clear out "last time's state" just by starting up.
        //
        // So... we work around all of this by lazily initializing the encoder and appenders, only when logs are
        // explicitly turned on by main(). Down below, we force the default to turn logs off, which causes nothing
        // to be set up at build-time when Netty is instantiating its loggers. However, at the next run-time, we'll
        // set up the appropriate logging level, Logback will invoke our listener, and we'll properly set up our
        // encoder and appenders to have logs.
        context.addListener(object : LoggerContextListener {
            override fun isResetResistant(): Boolean {
                return true
            }

            override fun onStart(context: LoggerContext?) {
                // Not implemented.
            }

            override fun onReset(context: LoggerContext?) {
                // Not implemented.
            }

            override fun onStop(context: LoggerContext?) {
                // Not implemented.
            }

            override fun onLevelChange(logger: Logger?, level: Level?) {
                if (level != null && level != Level.OFF) {
                    self.doFirstTimeSetup(context)
                }
            }
        })

        context.getLogger(Logger.ROOT_LOGGER_NAME).level = Level.OFF

        return Configurator.ExecutionStatus.DO_NOT_INVOKE_NEXT_IF_ANY
    }

    @Synchronized
    private fun doFirstTimeSetup(context: LoggerContext) {
        if (this.didInit) {
            return
        }

        val encoder = JsonLogEncoder()
        encoder.context = context
        encoder.start()

        // TODO: Attempt to set the Mapped Diagnostic Context

        val consoleAppender = ConsoleAppender<ILoggingEvent>()
        consoleAppender.context = context
        consoleAppender.encoder = encoder
        consoleAppender.name = "console"
        consoleAppender.start()

        val asyncAppender = AsyncAppender()
        asyncAppender.context = context
        asyncAppender.addAppender(consoleAppender)

        asyncAppender.start()

        context.getLogger(Logger.ROOT_LOGGER_NAME).addAppender(asyncAppender)
        this.didInit = true
    }

}