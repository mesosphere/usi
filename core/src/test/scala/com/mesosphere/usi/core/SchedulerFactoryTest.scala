package com.mesosphere.usi.core

import java.time.Instant
import java.util.UUID

import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.KillSwitches
import akka.{Done, NotUsed}
import com.mesosphere.usi.core.models._
import com.mesosphere.usi.core.models.resources.ScalarRequirement
import com.mesosphere.utils.AkkaUnitTest
import org.scalatest._

import scala.util.{Failure, Success}

class SchedulerFactoryTest extends AkkaUnitTest with Inside {
  def fromSinkAndSourceWithSharedFate[A, B, M1, M2](sink: Sink[A, M1], source: Source[B, M2]): Flow[A, B, NotUsed] = {
    val sinkKillSwitch = KillSwitches.shared(s"shared-fate-input-${UUID.randomUUID()}")
    val sourceKillSwitch = KillSwitches.shared(s"shared-fate-output-${UUID.randomUUID()}")

    val (sinkTerminalSignal, monitoredSink) = Flow[A]
      .via(sinkKillSwitch.flow)
      .watchTermination()(Keep.right)
      .to(sink)
      .preMaterialize()

    val (sourceTerminated, monitoredSource) = source
      .via(sourceKillSwitch.flow)
      .watchTermination()(Keep.right)
      .preMaterialize()

    sinkTerminalSignal.onComplete {
      case Success(_) =>
        sourceKillSwitch.shutdown()
      case Failure(ex) =>
        sourceKillSwitch.abort(ex)
    }

    sourceTerminated.onComplete { _ =>
      sinkKillSwitch.shutdown()
    }

    Flow.fromSinkAndSource(monitoredSink, monitoredSource)
  }

  def newMockedScheduler[M, O](
      snapshot: StateSnapshot = StateSnapshot.empty,
      commandInputSink: Sink[SchedulerCommand, M],
      stateEventsSource: Source[StateEvent, O]): (Flow[SchedulerCommand, Scheduler.StateOutput, NotUsed], M, O) = {

    val (m, preMaterializedCommandInput) =
      Flow[SchedulerCommand]
        .toMat(commandInputSink)(Keep.right)
        .preMaterialize()

    val (o, preMaterializedStateEvents) =
      stateEventsSource.preMaterialize()

    val flow = fromSinkAndSourceWithSharedFate(preMaterializedCommandInput, preMaterializedStateEvents)
      .prefixAndTail(0)
      .map { case (_, events) => snapshot -> events }

    (flow, m, o)
  }

  val podSpec = RunningPodSpec(
    PodId("podId"),
    RunSpec(
      resourceRequirements = List(ScalarRequirement.cpus(1), ScalarRequirement.memory(256)),
      shellCommand = "sleep 3600",
      role = "marathon"
    )
  )
  val deletePodSpec = ExpungePod(podSpec.id)
  val deletePodStatus = PodStatusUpdatedEvent(podSpec.id, None)

  "SchedulerFactory" when {
    "materialized as Source and Sink" should {
      "Complete the snapshot promise once it's available" in {
        val stateSnapshot = StateSnapshot(List(PodRecord(podSpec.id, Instant.now, AgentId("lol"))), Nil)
        val (scheduler, _, _) =
          newMockedScheduler(snapshot = stateSnapshot, commandInputSink = Sink.ignore, stateEventsSource = Source.maybe)

        val (snapshotF, source, _) = Scheduler.asSourceAndSink(scheduler)

        snapshotF.futureValue shouldEqual stateSnapshot
      }

      "Push the SpecUpdatedEvents downstream" in {
        val (scheduler, firstSpec, _) =
          newMockedScheduler(commandInputSink = Sink.head, stateEventsSource = Source.maybe)
        val (_, _, sink) = Scheduler.asSourceAndSink(scheduler)

        Source.repeat(deletePodSpec).runWith(sink)

        firstSpec.futureValue shouldEqual deletePodSpec
      }

      "Push the events from scheduler to the provided Source" in {
        val (scheduler, _, _) =
          newMockedScheduler(commandInputSink = Sink.ignore, stateEventsSource = Source.repeat(deletePodStatus))
        val (_, source, _) = Scheduler.asSourceAndSink(scheduler)

        source.runWith(Sink.head).futureValue shouldEqual deletePodStatus
      }

      "Complete both sink and source when the event stream coming to scheduler is cancelled by the scheduler" in {
        val (scheduler, _, _) =
          newMockedScheduler(commandInputSink = Sink.head, stateEventsSource = Source.repeat(deletePodStatus))
        val (_, source, sink) = Scheduler.asSourceAndSink(scheduler)

        val specStreamCompleted = Source
          .repeat(deletePodSpec)
          .watchTermination()(Keep.right)
          .to(sink)
          .run()

        val stateStreamCompleted = source
          .watchTermination()(Keep.right)
          .to(Sink.ignore)
          .run()

        specStreamCompleted.futureValue shouldBe Done
        stateStreamCompleted.futureValue shouldBe Done
      }

      "Complete both sink and source when the event stream coming to scheduler is cancelled by the client" in {
        val (scheduler, _, _) =
          newMockedScheduler(commandInputSink = Sink.ignore, stateEventsSource = Source.repeat(deletePodStatus))
        val (_, source, sink) = Scheduler.asSourceAndSink(scheduler)

        val specStreamCompleted = Source
          .single(deletePodSpec) // issue single spec update and then cancel the stream
          .watchTermination()(Keep.right)
          .to(sink)
          .run()

        val stateStreamCompleted = source.runWith(Sink.ignore)

        specStreamCompleted.futureValue shouldBe Done
        stateStreamCompleted.futureValue shouldBe Done
      }

      "Complete both sink and source when the event stream outgoing from scheduler is cancelled by the scheduler" in {
        val (scheduler, _, _) =
          newMockedScheduler(commandInputSink = Sink.ignore, stateEventsSource = Source.single(deletePodStatus))

        val (snapshot, source, sink) = Scheduler.asSourceAndSink(scheduler)

        val specStreamCompleted = Source
          .repeat(deletePodSpec)
          .watchTermination()(Keep.right)
          .to(sink)
          .run()

        val stateStreamCompleted = source
          .runWith(Sink.ignore)

        snapshot.futureValue shouldBe StateSnapshot.empty
        specStreamCompleted.futureValue shouldBe Done
        stateStreamCompleted.futureValue shouldBe Done
      }

      "Complete both sink and source when the event stream outgoing from scheduler is cancelled by the client" in {
        val (scheduler, _, _) =
          newMockedScheduler(commandInputSink = Sink.ignore, stateEventsSource = Source.repeat(deletePodStatus))

        val (_, source, sink) = Scheduler.asSourceAndSink(scheduler)

        val specStreamCompleted =
          Source
            .repeat(deletePodSpec)
            .runWith(sink)

        val stateStreamCompleted =
          source
            .watchTermination()(Keep.right)
            .to(Sink.head)
            .run()

        specStreamCompleted.futureValue shouldEqual Done
        stateStreamCompleted.futureValue shouldEqual Done
      }

      "Fail both sink and source when the failure occurred in the upstream" in {
        val (scheduler, _, _) =
          newMockedScheduler(commandInputSink = Sink.ignore, stateEventsSource = Source.repeat(deletePodStatus))

        val (_, source, sink) = Scheduler.asSourceAndSink(scheduler)

        val ex = new RuntimeException("Boom!")

        Source
          .failed(ex)
          .prepend(Source.single(deletePodSpec))
          .runWith(sink)

        val stateStreamCompleted =
          source
            .watchTermination()(Keep.right)
            .runWith(Sink.ignore)

        stateStreamCompleted.failed.futureValue shouldEqual ex
      }

      "Cancels the commandInput sink when the failure occurred in the downstream" in {
        val (scheduler, _, _) =
          newMockedScheduler(commandInputSink = Sink.ignore, stateEventsSource = Source.repeat(deletePodStatus))
        val (_, source, sink) = Scheduler.asSourceAndSink(scheduler)

        val specStreamCompleted = Source
          .repeat(deletePodSpec)
          .runWith(sink)

        source
          .map(_ => throw new Exception("Boom"))
          .runWith(Sink.ignore)

        specStreamCompleted.futureValue shouldEqual Done
      }

      "Propagates an exception to the state events output source if an exception is received from the spec events input" in {
        val (scheduler, _, _) = newMockedScheduler(commandInputSink = Sink.ignore, stateEventsSource = Source.maybe)
        val (_, source, sink) = Scheduler.asSourceAndSink(scheduler)

        val ex = new Exception("boom")
        Source
          .failed(ex)
          .runWith(sink)

        val stateStreamCompleted = source
          .runWith(Sink.ignore)

        stateStreamCompleted.failed.futureValue shouldEqual ex
      }

      "Stop sink and fail source when the failure occurred in the scheduler output" in {
        val ex = new Exception("Boom!")
        val (scheduler, _, _) =
          newMockedScheduler(commandInputSink = Sink.ignore, stateEventsSource = Source.failed(ex))

        val (_, source, sink) = Scheduler.asSourceAndSink(scheduler)

        val specStreamCompleted = Source
          .repeat(deletePodSpec)
          .runWith(sink)

        val stateStreamCompleted = source
          .runWith(Sink.ignore)

        specStreamCompleted.futureValue shouldEqual Done
        stateStreamCompleted.failed.futureValue shouldEqual ex
      }
    }
  }
}
