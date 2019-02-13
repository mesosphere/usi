package com.mesosphere.usi.core
import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.mesosphere.usi.core.models.AgentId
import org.apache.mesos.v1.{Protos => Mesos}
import org.apache.mesos.v1.scheduler.Protos.{Call => MesosCall, Event => MesosEvent}

import scala.collection.JavaConverters._

object FakeMesos {
  import ProtoConversions._
  import ProtoBuilders._
  val fakeAgentId = AgentId("fake-agent")
  val fakeFrameworkId = ProtoBuilders.newFrameworkId("fake-framework")
  val flow: Flow[MesosCall, MesosEvent, NotUsed] = {
    Flow[MesosCall].async.mapConcat { call =>
      val result: List[MesosEvent] = if (call.hasRevive) {

        val offer = newOffer(
          id = newOfferId("testing"),
          agentId = fakeAgentId.asProto,
          frameworkID = fakeFrameworkId,
          hostname = "some-host",
          resources = Seq(
            newResource("cpus", Mesos.Value.Type.SCALAR, scalar = 4.asProtoScalar),
            newResource("mem", Mesos.Value.Type.SCALAR, scalar = 4096.asProtoScalar),
            newResource("disk", Mesos.Value.Type.SCALAR, scalar = 256000.asProtoScalar)
          )
        )

        List(
          MesosEvent
            .newBuilder()
            .setOffers(MesosEvent.Offers
              .newBuilder()
              .addOffers(offer))
            .build())
      } else if (call.hasAccept) {
        val events = for {
          operation <- call.getAccept.getOperationsList.iterator.asScala
          taskInfo <- operation.getLaunch.getTaskInfosList.asScala
        } yield {
          val taskStatus = newTaskStatus(taskId = taskInfo.getTaskId, state = Mesos.TaskState.TASK_RUNNING)
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

}
