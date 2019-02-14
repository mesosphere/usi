package com.mesosphere.usi.core.helpers
import akka.NotUsed
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Flow, Keep, Sink, SinkQueueWithCancel, Source, SourceQueueWithComplete}
import com.mesosphere.usi.core.Scheduler
import com.mesosphere.usi.core.models.{SpecEvent, SpecUpdated, SpecsSnapshot, StateEvent}

/**
  * It's a little difficult to deal with the subscription input and output types; these methods provide helpers to more
  * easily deal with these shapes in a test context.
  */
object SchedulerStreamTestHelpers {
  def specInputSource(snapshot: SpecsSnapshot): Source[Scheduler.SpecInput, SourceQueueWithComplete[SpecUpdated]] = {
    Source.queue[SpecEvent](32, OverflowStrategy.fail).mapMaterializedValue { specQueue =>
      specQueue.offer(snapshot)
      specQueue.asInstanceOf[SourceQueueWithComplete[SpecUpdated]]
    }.prefixAndTail(1).map { case (Seq(snapshot), updates) =>
      (snapshot.asInstanceOf[SpecsSnapshot], updates.asInstanceOf[Source[SpecUpdated, NotUsed]])
    }
  }

  def outputFlatteningSink: Sink[Scheduler.StateOutput, SinkQueueWithCancel[StateEvent]] = {
    Flow[Scheduler.StateOutput].flatMapConcat { case (snapshot, updates) =>
      updates.prepend(Source.single(snapshot))
    }.toMat(Sink.queue())(Keep.right)
  }


}
