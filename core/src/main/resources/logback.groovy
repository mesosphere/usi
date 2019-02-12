import net.logstash.logback.appender.LoggingEventAsyncDisruptorAppender
import net.logstash.logback.appender.LogstashTcpSocketAppender
import net.logstash.logback.composite.ContextJsonProvider
import net.logstash.logback.composite.LogstashVersionJsonProvider
import net.logstash.logback.composite.loggingevent.*
import net.logstash.logback.encoder.*
import net.logstash.logback.fieldnames.LogstashFieldNames
import net.logstash.logback.stacktrace.ShortenedThrowableConverter


String USI_TCP_DESTINATION_ENV_VAR = "USI_TCP_LOG_DESTINATION"
String USI_DISABLE_JSON_LOG_ENV_VAR = "USI_DISABLE_JSON_LOG"

static def defaultJsonProvider() {
    def json = new LoggingEventJsonProviders()
    json.addTimestamp(tsProvider())
    json.addThreadName(threadNameProvider())
    json.addLogLevel(new LogLevelJsonProvider())
    json.addLoggerName(loggerNameProvider())
    json.addMessage(msgProvider())
    json.addCallerData(callerDataProvider())
    json.addStackTrace(stackTraceProvider())
    json.addContext(new ContextJsonProvider())
    json.addMdc(mdcProvider())
    json.addArguments(new ArgumentsJsonProvider())
    json.addVersion(versionProvider())
    json.addContext(new ContextJsonProvider())
    json.addLogstashMarkers(new LogstashMarkersJsonProvider())
    return json
}

static def tsProvider() {
    def ts = new LoggingEventFormattedTimestampJsonProvider()
    ts.setFieldName('ts')
    ts.setPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")
    return ts
}

static def threadNameProvider() {
    def threadName = new ThreadNameJsonProvider()
    threadName.setFieldName("thread")
    return threadName
}

static def loggerNameProvider() {
    def loggerName = new LoggerNameJsonProvider()
    loggerName.setFieldName("logger")
    loggerName.setShortenedLoggerNameLength(35)
    return loggerName
}

static def msgProvider() {
    def msg = new MessageJsonProvider()
    msg.setFieldName("msg")
    return msg
}

static def callerDataProvider() {
    def callerData = new CallerDataJsonProvider()
    LogstashFieldNames f = new LogstashFieldNames()
    f.setCallerClass("[ignore]")
    callerData.setFieldNames(f)
    return callerData
}

static def stackTraceProvider() {
    def stackTrace = new StackTraceJsonProvider()
    def throwableConverter = new ShortenedThrowableConverter()
    throwableConverter.setExcludes(Arrays.asList(
        /sun\..*/,
        /groovy\..*/,
        /com\.sun\..*/
    ))
    throwableConverter.setShortenedClassNameLength(35)
    throwableConverter.setRootCauseFirst(true)
    stackTrace.setThrowableConverter(throwableConverter)
    stackTrace
}

static def mdcProvider() {
    def mdc = new MdcJsonProvider()
    mdc.setFieldName("mdc")
    return mdc
}

static def versionProvider() {
    def ver = new LogstashVersionJsonProvider()
    ver.setWriteAsInteger(true)
    return ver
}

// Conditional appender(s).
def useTcpAppender = false
def USI_TCP_DESTINATION = System.getenv(USI_TCP_DESTINATION_ENV_VAR)
if (USI_TCP_DESTINATION != null && !USI_TCP_DESTINATION.isEmpty()) {
    println "Posting logs over tcp to : $USI_TCP_DESTINATION"
    appender("TCP", LogstashTcpSocketAppender) {
        destination = USI_TCP_DESTINATION
        encoder(LoggingEventCompositeJsonEncoder) {
            providers = defaultJsonProvider()
        }
    }
    useTcpAppender = true
}

appender("CONSOLE", ConsoleAppender) {
    def jsonLogDisabled = System.getenv(USI_DISABLE_JSON_LOG_ENV_VAR)
    if (jsonLogDisabled != null && !jsonLogDisabled.isEmpty()) {
        encoder(PatternLayoutEncoder) {
            pattern = "[%date] %-5level %message \\(%logger:%thread\\)%n"
        }
    } else {
        encoder(LoggingEventCompositeJsonEncoder) {
            providers = defaultJsonProvider()
        }
    }
}

appender("ASYNC", LoggingEventAsyncDisruptorAppender) {
    appenderRef("CONSOLE")
    if (useTcpAppender) {
        appenderRef("TCP")
    }
    includeCallerData = false
    ringBufferSize = 8192
}

root(TRACE, ["ASYNC"])
