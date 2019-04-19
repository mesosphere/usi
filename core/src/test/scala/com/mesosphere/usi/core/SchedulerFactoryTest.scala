package com.mesosphere.usi.core

import akka.NotUsed
import akka.stream.scaladsl.{SinkQueueWithCancel, SourceQueueWithComplete}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.OverflowStrategy
import com.mesosphere.usi.core.models.resources.ScalarRequirement
import com.mesosphere.usi.core.models._
import com.mesosphere.utils.AkkaUnitTest
import org.scalatest._

import scala.concurrent.Promise
import scala.util.{Failure, Success}

class SchedulerFactoryTest extends AkkaUnitTest with Inside {

  def createMockedScheduler: (
      Flow[Scheduler.SpecInput, Scheduler.StateOutput, NotUsed],
      SinkQueueWithCancel[Scheduler.SpecInput],
      SourceQueueWithComplete[Scheduler.StateOutput]) = {

    val (outerSinkQueue, outerSink) = Sink.queue[Scheduler.SpecInput]().preMaterialize()
    val (outerSourceQueue, outerSource) =
      Source.queue[Scheduler.StateOutput](16, OverflowStrategy.fail).preMaterialize()

    (Flow.fromSinkAndSource(outerSink, outerSource), outerSinkQueue, outerSourceQueue)
  }

  class SchedulerFixture {
    val podSpec = PodSpec(
      PodId("podId"),
      Goal.Running,
      RunSpec(
        resourceRequirements = List(ScalarRequirement.cpus(1), ScalarRequirement.memory(256)),
        shellCommand = "sleep 3600",
        role = "marathon"
      )
    )
    val specEvent = PodSpecUpdated(podSpec.id, None)
    val stateEvent = PodStatusUpdated(podSpec.id, None)

    val (scheduler, specInputQueue, stateOutputQueue) = createMockedScheduler
  }

  "SchedulerFactory" when {
    "materialized as Source and Sink" should {
      "Propagate a snapshot to the underlying scheduler" in new SchedulerFixture {
        Scheduler.asSourceAndSink(SpecsSnapshot(List(podSpec), Nil), scheduler)
        specInputQueue.pull().futureValue.value._1.podSpecs.head shouldEqual podSpec
      }

      "If snapshot is not provided push an empty snapshot" in new SchedulerFixture {
        Scheduler.asSourceAndSink(SpecsSnapshot.empty, scheduler)
        specInputQueue.pull().futureValue.value._1 shouldEqual SpecsSnapshot.empty
      }

      "Complete the snapshot promise once it's available" in new SchedulerFixture {
        val (snapshotF, _, _) = Scheduler.asSourceAndSink(SpecsSnapshot.empty, scheduler)
        specInputQueue.pull().futureValue

        val stateSnapshot = StateSnapshot(List(PodStatus(podSpec.id, Map.empty)), Nil)

        stateOutputQueue.offer(stateSnapshot -> Source.maybe)

        snapshotF.futureValue shouldEqual stateSnapshot
      }

      "Push the SpecUpdatedEvents downstream" in new SchedulerFixture {
        val (_, _, sink) = Scheduler.asSourceAndSink(SpecsSnapshot.empty, scheduler)

        Source.repeat(specEvent).runWith(sink)

        val done = inside(specInputQueue.pull().futureValue) {
          case Some((_, events)) => events.runWith(Sink.head)
        }

        done.futureValue shouldEqual specEvent

      }

      "Push the events from scheduler to the provided Source" in new SchedulerFixture {
        val (_, source, _) = Scheduler.asSourceAndSink(SpecsSnapshot.empty, scheduler)

        stateOutputQueue.offer(StateSnapshot.empty -> Source.repeat(stateEvent))

        source.runWith(Sink.head).futureValue shouldEqual stateEvent
      }

      "Complete both sink and source when the event stream coming to scheduler is cancelled by the scheduler" in new SchedulerFixture {
        val (_, source, sink) = Scheduler.asSourceAndSink(SpecsSnapshot.empty, scheduler)
        val (_, innerSource) = specInputQueue.pull().futureValue.value

        val stateStreamCompletionPromise = Promise[String]()
        val specStreamCompletionPromise = Promise[String]()

        Source
          .repeat(specEvent)
          .watchTermination()((_, terminationSignal) =>
            terminationSignal.map(_ => specStreamCompletionPromise.trySuccess("completed")))
          .runWith(sink)
        source
          .watchTermination()((_, terminationSignal) =>
            terminationSignal.map(_ => stateStreamCompletionPromise.trySuccess("completed")))
          .runWith(Sink.ignore)

        innerSource.runWith(Sink.cancelled) // cancel incoming updates on scheduler size

        specStreamCompletionPromise.future.futureValue shouldEqual "completed"
        stateStreamCompletionPromise.future.futureValue shouldEqual "completed"
      }

      "Complete both sink and source when the event stream coming to scheduler is cancelled by the client" in new SchedulerFixture {
        val (_, source, sink) = Scheduler.asSourceAndSink(SpecsSnapshot.empty, scheduler)

        inside(specInputQueue.pull().futureValue) {
          case Some((_, innerSource)) => innerSource.runWith(Sink.ignore)
        }

        val stateStreamCompletionPromise = Promise[String]()
        val specStreamCompletionPromise = Promise[String]()

        Source
          .single(specEvent) // issue single spec update and then cancel the stream
          .watchTermination()((_, terminationSignal) =>
            terminationSignal.map(_ => specStreamCompletionPromise.trySuccess("completed")))
          .runWith(sink)
        source
          .watchTermination()((_, terminationSignal) =>
            terminationSignal.map(_ => stateStreamCompletionPromise.trySuccess("completed")))
          .runWith(Sink.ignore)

        specStreamCompletionPromise.future.futureValue shouldEqual "completed"
        stateStreamCompletionPromise.future.futureValue shouldEqual "completed"
      }

      "Complete both sink and source when the event stream outgoing from scheduler is cancelled by the scheduler" in new SchedulerFixture {
        val (_, source, sink) = Scheduler.asSourceAndSink(SpecsSnapshot.empty, scheduler)
        val (_, innerSource) = specInputQueue.pull().futureValue.value
        innerSource.runWith(Sink.ignore) // consume all incoming spec updates

        val stateStreamCompletionPromise = Promise[String]()
        val specStreamCompletionPromise = Promise[String]()

        Source
          .repeat(specEvent)
          .watchTermination()((_, terminationSignal) =>
            terminationSignal.map(_ => specStreamCompletionPromise.trySuccess("completed")))
          .runWith(sink)

        source
          .watchTermination()((_, terminationSignal) =>
            terminationSignal.map(_ => stateStreamCompletionPromise.trySuccess("completed")))
          .runWith(Sink.ignore)

        stateOutputQueue.offer(StateSnapshot.empty -> Source.single(stateEvent)).futureValue

        specStreamCompletionPromise.future.futureValue shouldEqual "completed"
        stateStreamCompletionPromise.future.futureValue shouldEqual "completed"
      }

      "Complete both sink and source when the event stream outgoing from scheduler is cancelled by the client" in new SchedulerFixture {
        val (_, source, sink) = Scheduler.asSourceAndSink(SpecsSnapshot.empty, scheduler)
        val (_, innerSource) = specInputQueue.pull().futureValue.value
        innerSource.runWith(Sink.ignore) // consume all incoming spec updates

        val stateStreamCompletionPromise = Promise[String]()
        val specStreamCompletionPromise = Promise[String]()

        Source
          .repeat(specEvent)
          .watchTermination()((_, terminationSignal) =>
            terminationSignal.map(_ => specStreamCompletionPromise.trySuccess("completed")))
          .runWith(sink)

        source
          .watchTermination()((_, terminationSignal) =>
            terminationSignal.map(_ => stateStreamCompletionPromise.trySuccess("completed")))
          .runWith(Sink.head)

        stateOutputQueue.offer(StateSnapshot.empty -> Source.repeat(stateEvent)).futureValue

        specStreamCompletionPromise.future.futureValue shouldEqual "completed"
        stateStreamCompletionPromise.future.futureValue shouldEqual "completed"
      }

      "Fail both sink and source when the failure occurred in the upstream" in new SchedulerFixture {
        val (_, source, sink) = Scheduler.asSourceAndSink(SpecsSnapshot.empty, scheduler)
        val (_, innerSource) = specInputQueue.pull().futureValue.value
        innerSource.runWith(Sink.ignore) // consume all incoming spec updates

        stateOutputQueue.offer(StateSnapshot.empty -> Source.repeat(stateEvent)).futureValue

        val stateStreamCompletionPromise = Promise[String]()

        Source
          .failed(new RuntimeException("Boom!"))
          .prepend(Source.single(specEvent))
          .runWith(sink)

        source
          .watchTermination()((_, terminationSignal) =>
            terminationSignal.onComplete {
              case Success(_) =>
                stateStreamCompletionPromise.trySuccess("success")

              case Failure(ex) =>
                stateStreamCompletionPromise.trySuccess("failed")
          })
          .runWith(Sink.ignore)

        stateStreamCompletionPromise.future.futureValue shouldEqual "failed"
      }

      "Stop the sink when the failure occurred in the downstream" in new SchedulerFixture {
        val (_, source, sink) = Scheduler.asSourceAndSink(SpecsSnapshot.empty, scheduler)
        val (_, innerSource) = specInputQueue.pull().futureValue.value
        innerSource.runWith(Sink.ignore) // consume all incoming spec updates

        stateOutputQueue.offer(StateSnapshot.empty -> Source.repeat(stateEvent)).futureValue

        val stateStreamCompletionPromise = Promise[String]()

        Source
          .repeat(specEvent)
          .watchTermination()((_, terminationSignal) =>
            terminationSignal.onComplete {
              case Success(_) =>
                stateStreamCompletionPromise.trySuccess("stopped")

              case Failure(ex) =>
                stateStreamCompletionPromise.trySuccess("failed")
          })
          .runWith(sink)

        source
          .map(_ => throw new Exception("Boom"))
          .to(Sink.ignore)
          .run()

        stateStreamCompletionPromise.future.futureValue shouldEqual "stopped"
      }

      "Stop both sink and source when the failure occurred in the scheduler input" in new SchedulerFixture {
        val (_, source, sink) = Scheduler.asSourceAndSink(SpecsSnapshot.empty, scheduler)
        val (_, innerSource) = specInputQueue.pull().futureValue.value
        innerSource.map(_ => throw new Exception("Boom!")).runWith(Sink.ignore) // explosion in the scheduler

        stateOutputQueue.offer(StateSnapshot.empty -> Source.repeat(stateEvent)).futureValue

        val stateStreamCompletionPromise = Promise[String]()
        val specStreamCompletionPromise = Promise[String]()

        Source
          .repeat(specEvent)
          .watchTermination()((_, terminationSignal) =>
            terminationSignal.onComplete {
              case Success(_) =>
                specStreamCompletionPromise.trySuccess("stopped")

              case Failure(ex) =>
                specStreamCompletionPromise.trySuccess("failed")
          })
          .runWith(sink)

        source
          .watchTermination()((_, terminationSignal) =>
            terminationSignal.onComplete {
              case Success(_) =>
                stateStreamCompletionPromise.trySuccess("stopped")

              case Failure(ex) =>
                stateStreamCompletionPromise.trySuccess("failed")
          })
          .runWith(Sink.ignore)

        specStreamCompletionPromise.future.futureValue shouldEqual "stopped"
        stateStreamCompletionPromise.future.futureValue shouldEqual "stopped"
      }

      "Stop sink and fail source when the failure occurred in the scheduler output" in new SchedulerFixture {
        val (_, source, sink) = Scheduler.asSourceAndSink(SpecsSnapshot.empty, scheduler)
        val (_, innerSource) = specInputQueue.pull().futureValue.value
        innerSource.runWith(Sink.ignore)

        stateOutputQueue
          .offer(StateSnapshot.empty -> Source.failed(new Exception("Boom!")))
          .futureValue

        val stateStreamCompletionPromise = Promise[String]()
        val specStreamCompletionPromise = Promise[String]()

        Source
          .repeat(specEvent)
          .watchTermination()((_, terminationSignal) =>
            terminationSignal.onComplete {
              case Success(_) =>
                specStreamCompletionPromise.trySuccess("stopped")

              case Failure(ex) =>
                specStreamCompletionPromise.trySuccess("failed")
          })
          .runWith(sink)

        source.map { e =>
          println(e)
        }.watchTermination()((_, terminationSignal) =>
            terminationSignal.onComplete {
              case Success(_) =>
                stateStreamCompletionPromise.trySuccess("stopped")

              case Failure(ex) =>
                stateStreamCompletionPromise.trySuccess("failed")
          })
          .runWith(Sink.ignore)

        specStreamCompletionPromise.future.futureValue shouldEqual "stopped"
        stateStreamCompletionPromise.future.futureValue shouldEqual "failed"

      }
    }
  }
}
