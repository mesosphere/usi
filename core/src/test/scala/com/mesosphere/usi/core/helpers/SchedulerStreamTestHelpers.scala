package com.mesosphere.usi.core.helpers
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Source, SourceQueueWithComplete}
import com.mesosphere.usi.core.models.SchedulerCommand

/**
  * It's a little difficult to deal with the subscription input and output types; these methods provide helpers to more
  * easily deal with these shapes in a test context.
  */
object SchedulerStreamTestHelpers {
  val commandInputSource: Source[SchedulerCommand, SourceQueueWithComplete[SchedulerCommand]] = {
    Source
      .queue[SchedulerCommand](32, OverflowStrategy.fail)
  }
}
