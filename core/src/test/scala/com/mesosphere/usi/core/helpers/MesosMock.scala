package com.mesosphere.usi.core.helpers

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.mesosphere.usi.core.models.AgentId
import com.mesosphere.usi.core.protos.ProtoBuilders
import org.apache.mesos.v1.Protos
import org.apache.mesos.v1.{Protos => Mesos}
import org.apache.mesos.v1.scheduler.Protos.{Call => MesosCall, Event => MesosEvent}
import scala.collection.JavaConverters._

object MesosMock {
  import com.mesosphere.usi.core.protos.ProtoBuilders._
  import com.mesosphere.usi.core.protos.ProtoConversions._
  val mockAgentId: AgentId = AgentId("mock-agent")
  val mockFrameworkId: Mesos.FrameworkID = ProtoBuilders.newFrameworkId("mock-framework")

  val flow: Flow[MesosCall, MesosEvent, NotUsed] = {
    Flow[MesosCall].async.mapConcat { call =>
      val result: List[MesosEvent] = if (call.hasRevive) {
        List(
          MesosEvent
            .newBuilder()
            .setOffers(MesosEvent.Offers
              .newBuilder()
              .addOffers(createMockOffer()))
            .build())
      } else if (call.hasAccept) {
        val events = for {
          operation <- call.getAccept.getOperationsList.iterator.asScala
          taskInfo <- operation.getLaunch.getTaskInfosList.asScala
        } yield {
          val taskStatus = newTaskStatus(
            taskId = taskInfo.getTaskId,
            state = Mesos.TaskState.TASK_RUNNING,
            agentId = mockAgentId.asProto)
          MesosEvent.newBuilder
            .setUpdate(
              MesosEvent.Update
                .newBuilder()
                .setStatus(taskStatus)
            )
            .build()
        }
        events.toList
      } else {
        Nil
      }
      result
    }
  }

  // Add more arguments as needed.
  def createMockOffer(
    cpus: Double = 4,
    mem: Double = 4096,
  ): Mesos.Offer = {
    newOffer(
      id = newOfferId("testing"),
      agentId = mockAgentId.asProto,
      frameworkID = mockFrameworkId,
      hostname = "some-host",
      newResourceAllocationInfo("some-role"),
      resources = Seq(
        newResource(
          "cpus",
          Mesos.Value.Type.SCALAR,
          newResourceAllocationInfo("some-role"),
          scalar = cpus.asProtoScalar),
        newResource("mem", Mesos.Value.Type.SCALAR, newResourceAllocationInfo("some-role"), scalar = mem.asProtoScalar),
        newResource(
          "disk",
          Mesos.Value.Type.SCALAR,
          newResourceAllocationInfo("some-role"),
          scalar = 256000.asProtoScalar)
      )
    )
  }

  def taskUpdateEvent(
    taskStatus : Protos.TaskStatus
  ): MesosEvent = MesosEvent
    .newBuilder()
    .setUpdate(MesosEvent.Update.newBuilder().setStatus(taskStatus))
    .build()

}
