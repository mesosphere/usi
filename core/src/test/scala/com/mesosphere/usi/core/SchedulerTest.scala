package com.mesosphere.usi.core

import akka.NotUsed
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Sink, Source}
import akka.stream.{ActorMaterializer, FlowShape, OverflowStrategy}
import com.mesosphere.mesos.client.MesosCalls
import com.mesosphere.usi.core.launching.SimpleShellCommandInfoGenerator
import com.mesosphere.usi.core.matching.ScalarResourceRequirement
import com.mesosphere.usi.core.models._
import org.scalatest._
import com.mesosphere.utils.AkkaUnitTest
import org.apache.mesos.v1.Protos
import org.apache.mesos.v1.scheduler.Protos.{Call => MesosCall, Event => MesosEvent}

class SchedulerTest extends AkkaUnitTest with Inside {

  val loggingMesosFakeFlow: Flow[MesosCall, MesosEvent, NotUsed] = Flow[MesosCall].map { call =>
    logger.info(s"Mesos call: ${call}")
    call
  }.via(FakeMesos.flow).map { event =>
    logger.info(s"Mesos event: ${event}")
    event
  }

  val mockedScheduler: Flow[SpecEvent, StateEvent, NotUsed] = {
    Flow.fromGraph {
      GraphDSL.create(Scheduler.unconnectedGraph(new MesosCalls(FakeMesos.fakeFrameworkId)), loggingMesosFakeFlow)((_, _) => NotUsed) { implicit builder =>
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
    val podId = PodId("pod-1")
    val (input, output) = Source
      .queue[SpecEvent](16, OverflowStrategy.fail)
      .via(mockedScheduler)
      .toMat(Sink.queue())(Keep.both)
      .run

    input.offer(PodSpecUpdated(podId, Some(PodSpec(podId, Goal.Running, RunSpec(
      resourceRequirements = List(
        ScalarResourceRequirement(ResourceType.CPUS, 1),
        ScalarResourceRequirement(ResourceType.MEM, 256)),
      commandBuilder = SimpleShellCommandInfoGenerator("sleep 3600"))))))

    inside(output.pull().futureValue) {
      case Some(podRecord: PodRecordUpdated) =>
        podRecord.id shouldBe podId
        podRecord.newRecord.get.agentId shouldBe FakeMesos.fakeAgentId
    }
    inside(output.pull().futureValue) {
      case Some(podStatusChange: PodStatusUpdated) =>
        podStatusChange.newStatus.get.taskStatuses(TaskId(podId.value)).getState shouldBe Protos.TaskState.TASK_RUNNING
    }
  }
}
