package com.mesosphere.usi.core.helpers
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Flow, Keep, Sink, SinkQueueWithCancel, Source, SourceQueueWithComplete}
import com.mesosphere.usi.core.Scheduler
import com.mesosphere.usi.core.models.{SchedulerCommand, StateEvent}

/**
  * It's a little difficult to deal with the subscription input and output types; these methods provide helpers to more
  * easily deal with these shapes in a test context.
  */
object SchedulerStreamTestHelpers {
  val commandInputSource: Source[SchedulerCommand, SourceQueueWithComplete[SchedulerCommand]] = {
    Source
      .queue[SchedulerCommand](32, OverflowStrategy.fail)
  }

  def outputFlatteningSink: Sink[Scheduler.StateOutput, SinkQueueWithCancel[StateEvent]] = {
    Flow[Scheduler.StateOutput].flatMapConcat {
      case (snapshot, updates) =>
        updates.prepend(Source.single(snapshot))
    }.toMat(Sink.queue())(Keep.right)
  }

}
