package com.mesosphere.usi.core

import java.time.Instant

import akka.Done
import akka.stream.FlowShape
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Sink}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import com.mesosphere.mesos.client.MesosCalls
import com.mesosphere.usi.core.conf.SchedulerSettings
import com.mesosphere.usi.core.helpers.MesosMock
import com.mesosphere.usi.core.helpers.SchedulerStreamTestHelpers.commandInputSource
import com.mesosphere.usi.core.models.commands.SchedulerCommand
import com.mesosphere.usi.core.models.{AgentId, PodId, PodRecord, PodRecordUpdatedEvent, StateEvent}
import com.mesosphere.utils.AkkaUnitTest
import com.mesosphere.utils.metrics.DummyMetrics
import com.mesosphere.utils.persistence.InMemoryPodRecordRepository
import org.apache.mesos.v1.scheduler.Protos.{Call => MesosCall, Event => MesosEvent}
import org.scalatest._

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import scala.util.Random

class SchedulerTest extends AkkaUnitTest with Inside {

  val loggingMockMesosFlow: Flow[MesosCall, MesosEvent, Future[Done]] = Flow[MesosCall]
    .watchTermination()(Keep.right)
    .map { call =>
      logger.info(s"Mesos call: ${call}")
      call
    }
    .via(MesosMock.flow)
    .map { event =>
      logger.info(s"Mesos event: ${event}")
      event
    }

  def mockedScheduler: Flow[SchedulerCommand, StateEvent, Future[Done]] = {
    val (_, unconnectedFlow) =
      Scheduler
        .unconnectedGraph(
          new MesosCalls(MesosMock.mockFrameworkId),
          InMemoryPodRecordRepository(),
          DummyMetrics,
          SchedulerSettings.load(),
          MesosMock.masterDomainInfo)
        .futureValue
    Flow.fromGraph {
      GraphDSL.create(unconnectedFlow, loggingMockMesosFlow)((_, materializedValue) => materializedValue) {
        implicit builder =>
          { (graph, mockMesos) =>
            import GraphDSL.Implicits._

            mockMesos ~> graph.in2
            graph.out2 ~> mockMesos

            FlowShape(graph.in1, graph.out1)
          }
      }
    }
  }

  "It closes the Mesos client when the specs input stream terminates" in {
    val ((input, mesosCompleted), _) = commandInputSource
      .viaMat(mockedScheduler)(Keep.both)
      .toMat(Sink.ignore)(Keep.both)
      .run

    input.complete()
    mesosCompleted.futureValue shouldBe Done
  }

  "It closes the Mesos client when the scheduler state events are closed" in {
    val ((_, mesosCompleted), output) = commandInputSource
      .viaMat(mockedScheduler)(Keep.both)
      .toMat(Sink.queue())(Keep.both)
      .run

    output.cancel()
    mesosCompleted.futureValue shouldBe Done
  }

  "Persistence flow pipelines writes" in {
    Given("a persistence storage and a flow using it")
    val fuzzyPodRecordRepo: InMemoryPodRecordRepository = new InMemoryPodRecordRepository {
      val rand: Random = scala.util.Random
      override def store(record: PodRecord): Future[Done] = {
        super.store(record).flatMap { _ =>
          akka.pattern.after(rand.nextInt(100).millis, system.scheduler)(Future.successful(Done))
        }
      }
    }
    val (pub, sub) = TestSource
      .probe[SchedulerEvents]
      .via(Scheduler.persistenceFlow(fuzzyPodRecordRepo, SchedulerSettings.load()))
      .toMat(TestSink.probe[SchedulerEvents])(Keep.both)
      .run()

    And("a list of fake pod record events")
    val podIds = List(List("A", "B"), List("C", "D"), List("E", "F")).map(_.map(PodId))
    val storesAndDeletes = podIds.flatMap { ids =>
      val storeEvents = ids.map(id => PodRecordUpdatedEvent(id, Some(PodRecord(id, Instant.now(), AgentId("-")))))
      val deleteEvents = ids.map(PodRecordUpdatedEvent(_, None))
      List(SchedulerEvents(storeEvents), SchedulerEvents(deleteEvents))
    }

    Then("persistence flow behaves as an Identity (with side-effects)")
    val oneEvent = storesAndDeletes.reduce((e1, e2) => SchedulerEvents(e1.stateEvents ++ e2.stateEvents))
    pub.sendNext(oneEvent)
    sub.requestNext(oneEvent)
    fuzzyPodRecordRepo.readAll().futureValue.keySet shouldBe empty

    And("respects the order of the events")
    sub.request(storesAndDeletes.size)
    storesAndDeletes.reverse.foreach(pub.sendNext)
    storesAndDeletes.reverse.foreach(sub.expectNext)
    fuzzyPodRecordRepo.readAll().futureValue.keySet should contain theSameElementsAs podIds.flatten
  }

  "Persistence flow honors the pipe-lining threshold" in {
    Given("a list of persistence operations with count strictly greater than twice the pipeline limit")
    val limit = SchedulerSettings.load().persistencePipelineLimit
    val deleteEvents = (1 to limit * 2 + 1)
      .map(x => PodRecordUpdatedEvent(PodId("pod-" + x), None))
      .grouped(100)
      .map(x => SchedulerEvents(stateEvents = x.toList))
      .toList

    And("a persistence flow using a controllable pod record repository")
    val controlledRepository = new ControlledRepository()
    val (pub, sub) = TestSource
      .probe[SchedulerEvents]
      .via(Scheduler.persistenceFlow(controlledRepository, SchedulerSettings.load()))
      .toMat(TestSink.probe[SchedulerEvents])(Keep.both)
      .run()

    When("downstream requests for new events and upstream sends the events")
    sub.request(deleteEvents.size)
    deleteEvents.foreach(pub.sendNext)

    Then("flow should throttle the number of futures across multiple events")
    eventually {
      assertThrows[AssertionError](sub.expectNext(10.millis))
      controlledRepository.pendingWrites shouldEqual limit - 1 // limit - 1 because the last element is input itself.
    }

    Then("flow should be able to continue to throttle")
    controlledRepository.markCompleted(limit - 1)
    eventually {
      assertThrows[AssertionError](sub.expectNext(10.millis))
      controlledRepository.pendingWrites shouldEqual limit - 1
    }
  }

  class ControlledRepository extends InMemoryPodRecordRepository {
    import java.util.concurrent.ConcurrentLinkedQueue
    val queue = new ConcurrentLinkedQueue[Promise[Done]]
    override def delete(podId: PodId): Future[Done] = {
      val completed = Promise[Done]
      queue.offer(completed)
      super.delete(podId).flatMap(_ => completed.future)
    }

    def pendingWrites: Int = queue.size()

    @tailrec final def markCompleted(n: Int): Unit = {
      if ((n > 0) && !queue.isEmpty) {
        queue.poll().success(Done)
        markCompleted(n - 1)
      }
    }
  }
}
