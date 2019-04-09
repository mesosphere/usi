package com.mesosphere.usi.core

import akka.Done
import akka.stream.scaladsl.{Flow, GraphDSL, Keep}
import akka.stream.{ActorMaterializer, FlowShape}
import com.mesosphere.mesos.client.MesosCalls
import com.mesosphere.usi.core.helpers.MesosMock
import com.mesosphere.usi.core.helpers.SchedulerStreamTestHelpers.{outputFlatteningSink, specInputSource}
import com.mesosphere.usi.core.models._
import com.mesosphere.usi.core.models.resources.ScalarRequirement
import com.mesosphere.utils.AkkaUnitTest
import org.apache.mesos.v1.scheduler.Protos.{Call => MesosCall, Event => MesosEvent}
import org.apache.mesos.v1.{Protos => Mesos}
import org.scalatest._

import scala.concurrent.Future

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
      GraphDSL.create(Scheduler.unconnectedGraph(new MesosCalls(MesosMock.mockFrameworkId)), loggingMockMesosFlow)(
        (_, materializedValue) => materializedValue) { implicit builder =>
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
}
