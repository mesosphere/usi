package com.mesosphere.usi.core

import akka.Done
import akka.stream.scaladsl.{Flow, GraphDSL, Keep}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.stream.{ActorMaterializer, FlowShape}
import com.mesosphere.mesos.client.MesosCalls
import com.mesosphere.usi.core.conf.SchedulerSettings
import com.mesosphere.usi.core.helpers.MesosMock
import com.mesosphere.usi.core.helpers.SchedulerStreamTestHelpers.{outputFlatteningSink, specInputSource}
import com.mesosphere.usi.core.models._
import com.mesosphere.usi.core.models.resources.ScalarRequirement
import com.mesosphere.usi.repository.InMemoryPodRecordRepository
import com.mesosphere.utils.AkkaUnitTest
import com.typesafe.config.ConfigFactory
import java.time.Instant
import org.apache.mesos.v1.scheduler.Protos.{Call => MesosCall, Event => MesosEvent}
import org.apache.mesos.v1.{Protos => Mesos}
import org.scalatest._
import scala.annotation.tailrec
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._

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

  def mockedScheduler: Flow[Scheduler.SpecInput, Scheduler.StateOutput, Future[Done]] = {
    Flow.fromGraph {
      GraphDSL.create(
        Scheduler.unconnectedGraph(new MesosCalls(MesosMock.mockFrameworkId), InMemoryPodRecordRepository()),
        loggingMockMesosFlow)((_, materializedValue) => materializedValue) { implicit builder =>
        { (graph, mockMesos) =>
          import GraphDSL.Implicits._

          mockMesos ~> graph.in2
          graph.out2 ~> mockMesos

          FlowShape(graph.in1, graph.out1)
        }
      }
    }
  }

  "It reports a running task when I provide " in {
    implicit val materializer = ActorMaterializer()
    val podId = PodId("running-pod-on-a-mocked-mesos")
    val (input, output) = specInputSource(SpecsSnapshot.empty)
      .via(mockedScheduler)
      .toMat(outputFlatteningSink)(Keep.both)
      .run

    input.offer(
      PodSpecUpdated(
        podId,
        Some(
          PodSpec(
            podId,
            Goal.Running,
            RunSpec(
              resourceRequirements = List(ScalarRequirement.cpus(1), ScalarRequirement.memory(256)),
              shellCommand = "sleep 3600",
              "test")
          ))
      ))

    inside(output.pull().futureValue) {
      case Some(snapshot: StateSnapshot) =>
        snapshot shouldBe StateSnapshot.empty
    }
    inside(output.pull().futureValue) {
      case Some(podRecord: PodRecordUpdated) =>
        podRecord.id shouldBe podId
        podRecord.newRecord.get.agentId shouldBe MesosMock.mockAgentId
    }
    inside(output.pull().futureValue) {
      case Some(podStatusChange: PodStatusUpdated) =>
        podStatusChange.newStatus.get.taskStatuses(TaskId(podId.value)).getState shouldBe Mesos.TaskState.TASK_RUNNING
    }
  }

  "It closes the Mesos client when the specs input stream terminates" in {
    val ((input, mesosCompleted), _) = specInputSource(SpecsSnapshot.empty)
      .viaMat(mockedScheduler)(Keep.both)
      .toMat(outputFlatteningSink)(Keep.both)
      .run

    input.complete()
    mesosCompleted.futureValue shouldBe Done
  }

  "It closes the Mesos client when the scheduler state events are closed" in {
    val ((_, mesosCompleted), output) = specInputSource(SpecsSnapshot.empty)
      .viaMat(mockedScheduler)(Keep.both)
      .toMat(outputFlatteningSink)(Keep.both)
      .run

    output.cancel()
    mesosCompleted.futureValue shouldBe Done
  }

  "Persistence flow pipelines writes" in {
    Given("a persistence storage and a flow using it")
    val fuzzyPodRecordRepo = new InMemoryPodRecordRepository {
      val rand = scala.util.Random
      override def store(record: PodRecord): Future[Done] = {
        super.store(record).flatMap { _ =>
          akka.pattern.after(rand.nextInt(100).millis, system.scheduler)(Future.successful(Done))
        }
      }
    }
    val (pub, sub) = TestSource
      .probe[SchedulerEvents]
      .via(Scheduler.persistenceFlow(fuzzyPodRecordRepo))
      .toMat(TestSink.probe[SchedulerEvents])(Keep.both)
      .run()

    And("a list of fake pod record events")
    val podIds = List(List("A", "B"), List("C", "D"), List("E", "F")).map(_.map(PodId))
    val storesAndDeletes = podIds.flatMap { ids =>
      val storeEvents = ids.map(id => PodRecordUpdated(id, Some(PodRecord(id, Instant.now(), AgentId("-")))))
      val deleteEvents = ids.map(PodRecordUpdated(_, None))
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
    val limit = SchedulerSettings.fromConfig(ConfigFactory.load().getConfig("scheduler")).persistencePipelineLimit
    val deleteEvents = (1 to limit * 2 + 1)
      .map(x => PodRecordUpdated(PodId("pod-" + x), None))
      .grouped(100)
      .map(x => SchedulerEvents(stateEvents = x.toList))
      .toList

    And("a persistence flow using a controllable pod record repository")
    val controlledRepository = new ControlledRepository()
    val (pub, sub) = TestSource
      .probe[SchedulerEvents]
      .via(Scheduler.persistenceFlow(controlledRepository))
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

    def pendingWrites = queue.size()

    @tailrec final def markCompleted(n: Int): Unit = {
      if ((n > 0) && !queue.isEmpty) {
        queue.poll().success(Done)
        markCompleted(n - 1)
      }
    }
  }
}
