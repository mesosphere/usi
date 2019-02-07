import net.logstash.logback.encoder.LogstashEncoder
import net.logstash.logback.appender.LoggingEventAsyncDisruptorAppender
import net.logstash.logback.fieldnames.LogstashFieldNames

appender("CONSOLE", ConsoleAppender) {
    encoder(LogstashEncoder){
        LogstashFieldNames l = new LogstashFieldNames()
        l.setVersion("[ignore]")
        l.setLevelValue("[ignore]")
        fieldNames = l
    }
}

appender("ASYNC", LoggingEventAsyncDisruptorAppender) {
    appenderRef("CONSOLE")
    ringBufferSize = 8192
}

root(INFO, ["ASYNC"])
