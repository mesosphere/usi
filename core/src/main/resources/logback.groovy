import net.logstash.logback.appender.LoggingEventAsyncDisruptorAppender
import net.logstash.logback.composite.LogstashVersionJsonProvider
import net.logstash.logback.composite.loggingevent.*
import net.logstash.logback.encoder.*
import net.logstash.logback.stacktrace.ShortenedThrowableConverter

appender("CONSOLE_JSON", ConsoleAppender) {
    encoder(LoggingEventCompositeJsonEncoder) {
        providers(LoggingEventJsonProviders) {
            timestamp(LoggingEventFormattedTimestampJsonProvider) {
                fieldName = '@ts'
                pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS"
            }
            threadName(ThreadNameJsonProvider) {
                fieldName = 'thread'
            }
            logLevel(LogLevelJsonProvider)
            loggerName(LoggerNameJsonProvider) {
                fieldName = 'logger'
                shortenedLoggerNameLength = 35
            }
            message(MessageJsonProvider) {
                fieldName = 'msg'
            }
            arguments(ArgumentsJsonProvider)
            mdc(MdcJsonProvider)
            stackTrace(StackTraceJsonProvider) {
                throwableConverter(ShortenedThrowableConverter) {
                    shortenedClassNameLength = 40
                    exclude = /sun\..*/
                    exclude = /groovy\..*/
                    exclude = /com\.sun\..*/
                    rootCauseFirst = true
                }
            }
            version(LogstashVersionJsonProvider) {
                writeAsInteger = true
            }
        }
    }
}

appender("ASYNC", LoggingEventAsyncDisruptorAppender) {
    appenderRef("CONSOLE_JSON")
    ringBufferSize = 8192
}

root(INFO, ["ASYNC"])
