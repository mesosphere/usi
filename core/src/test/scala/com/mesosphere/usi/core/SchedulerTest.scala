package com.mesosphere.usi.core

import akka.NotUsed
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Sink, Source}
import akka.stream.{ActorMaterializer, FlowShape, OverflowStrategy}
import com.mesosphere.usi.core.models.{AgentId, Goal, PodId, PodSpec, RunSpec}
import com.mesosphere.usi.models._
import org.scalatest._
import com.mesosphere.utils.AkkaUnitTest

class SchedulerTest extends AkkaUnitTest with Inside {

  val mockedScheduler: Flow[SpecEvent, StateEvent, NotUsed] = {
    Flow.fromGraph {
      GraphDSL.create(Scheduler.unconnectedGraph, FakeMesos.flow)((_, _) => NotUsed) { implicit builder =>
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
    val (input, output) = Source.queue[SpecEvent](16, OverflowStrategy.fail)
      .via(mockedScheduler)
      .toMat(Sink.queue())(Keep.both)
      .run

    input.offer(PodSpecUpdated(podId, Some(PodSpec(podId, Goal.Running, RunSpec(1)))))
    inside(output.pull().futureValue) {
      case Some(podRecord: PodRecordUpdated) =>
        podRecord.id shouldBe podId
        podRecord.newRecord.get.agentId shouldBe fakeAgentId
    }
    inside(output.pull().futureValue) {
      case Some(podStatusChange: PodStatusUpdated) =>
        podStatusChange.newStatus.get.taskStatuses(podId.value) shouldBe Mesos.TaskStatus.TASK_RUNNING
    }
  }
}
