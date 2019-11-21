package com.mesosphere.usi.core.helpers

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.google.protobuf.ByteString
import com.mesosphere.usi.core.models.AgentId
import com.mesosphere.usi.core.protos.ProtoBuilders
import org.apache.mesos.v1.scheduler.Protos.{Call => MesosCall, Event => MesosEvent}
import org.apache.mesos.v1.{Protos => Mesos}

import scala.collection.JavaConverters._

object MesosMock {
  import com.mesosphere.usi.core.protos.ProtoBuilders._
  import com.mesosphere.usi.core.protos.ProtoConversions._
  val mockAgentId: AgentId = AgentId("mock-agent")
  val mockFrameworkId: Mesos.FrameworkID = ProtoBuilders.newFrameworkId("mock-framework")
  val masterDomainInfo: Mesos.DomainInfo = ProtoBuilders.newDomainInfo(region = "home", zone = "a")
  def mockFrameworkInfo(roles: Seq[String] = Seq("test")): Mesos.FrameworkInfo =
    ProtoBuilders.newFrameworkInfo(user = "mock", name = "mock-mesos", id = Some(mockFrameworkId), roles = roles)

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
            agentId = mockAgentId.asProto,
            uuid = ByteString.copyFromUtf8("uuid"))
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
      domain: Mesos.DomainInfo = masterDomainInfo,
      attributes: Seq[Mesos.Attribute] = Nil,
      allocationRole: String = "test"
  ): Mesos.Offer = {
    val allocationInfo = newResourceAllocationInfo(allocationRole)
    newOffer(
      id = newOfferId("testing"),
      agentId = mockAgentId.asProto,
      frameworkID = mockFrameworkId,
      hostname = "some-host",
      allocationInfo = allocationInfo,
      domain = domain,
      attributes = attributes,
      resources = Seq(
        newResource("cpus", Mesos.Value.Type.SCALAR, allocationInfo, scalar = cpus.asProtoScalar),
        newResource("mem", Mesos.Value.Type.SCALAR, allocationInfo, scalar = mem.asProtoScalar),
        newResource("disk", Mesos.Value.Type.SCALAR, allocationInfo, scalar = 256000.asProtoScalar)
      )
    )
  }

}
