package com.mesosphere.usi.core

import akka.Done
import akka.stream.scaladsl.{Flow, GraphDSL, Keep}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.stream.{ActorMaterializer, FlowShape}
import com.mesosphere.mesos.client.MesosCalls
import com.mesosphere.usi.core.helpers.MesosMock
import com.mesosphere.usi.core.helpers.SchedulerStreamTestHelpers.{outputFlatteningSink, specInputSource}
import com.mesosphere.usi.core.models._
import com.mesosphere.usi.core.models.resources.ScalarRequirement
import com.mesosphere.usi.repository.InMemoryPodRecordRepository
import com.mesosphere.utils.AkkaUnitTest
import java.time.Instant
import org.apache.mesos.v1.scheduler.Protos.{Call => MesosCall, Event => MesosEvent}
import org.apache.mesos.v1.{Protos => Mesos}
import org.scalatest._
import scala.concurrent.Future
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
              shellCommand = "sleep 3600")
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
    Given("a slow persistence storage and a flow using it")
    val delayPerElement = 100.millis // Delay precision is 10ms
    val slowPodRecordRepo = new InMemoryPodRecordRepository {
      override def storeFlow = super.storeFlow.initialDelay(delayPerElement).delay(delayPerElement)
      override def deleteFlow = super.deleteFlow.initialDelay(delayPerElement).delay(delayPerElement)
    }
    val (pub, sub) = TestSource
      .probe[SchedulerEvents]
      .via(Scheduler.persistenceFlow(slowPodRecordRepo))
      .toMat(TestSink.probe[SchedulerEvents])(Keep.both)
      .run()

    Then("flow should emit the exact element it receives")
    val deleteOp = SchedulerEvents(List(PodRecordUpdated(PodId("A"), None)))
    sub.ensureSubscription()
    sub.request(1)
    pub.expectRequest()
    pub.sendNext(deleteOp)
    sub.expectNext(deleteOp)
    slowPodRecordRepo.readAll().futureValue.keySet shouldBe empty

    Then("flow should pipeline writes pulling only one element at a time")
    sub.request(1)
    pub.sendNext(deleteOp)
    assertThrows[AssertionError](sub.expectNext(delayPerElement / 2))
    sub.expectNext(deleteOp)

    Given("a list of fake pod record events")
    val podIds = List(List("A", "B"), List("C", "D"), List("E", "F")).map(_.map(PodId))
    val deleteEvents = podIds.map(_.map(PodRecordUpdated(_, None)))
    val storeEvents =
      deleteEvents.map(_.map(x => x.copy(newRecord = Some(PodRecord(x.id, Instant.now(), AgentId("-"))))))

    Then("flow should be able to persist records")
    sub.request(podIds.flatten.size)
    storeEvents.foreach(x => pub.sendNext(SchedulerEvents(x)))
    storeEvents.foreach(x => sub.expectNext(SchedulerEvents(x)))
    val result = slowPodRecordRepo.readAll().futureValue
    result.keySet shouldEqual podIds.flatten.toSet
    result.values.toSet shouldEqual storeEvents.flatten.flatMap(_.newRecord).toSet

    Then("flow should be able to delete the persisted records")
    sub.request(podIds.flatten.size)
    deleteEvents.foreach(x => pub.sendNext(SchedulerEvents(x)))
    deleteEvents.foreach(x => sub.expectNext(SchedulerEvents(x)))
    slowPodRecordRepo.readAll().futureValue.keySet shouldBe empty
  }
}
