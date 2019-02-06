package com.mesosphere.usi.core
import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.mesosphere.usi.core.models.{AgentId, Mesos}

object FakeMesos {
  val fakeAgentId = AgentId("fake-agent")
  val flow: Flow[Mesos.Call, Mesos.Event, NotUsed] = {
    Flow[Mesos.Call].async.mapConcat {
      case Mesos.Call.Revive =>
        List(Mesos.Event.Offer("fake-offer", fakeAgentId))
      case Mesos.Call.Accept(offerId, operations) =>
        operations.map { o =>
          Mesos.Event.StatusUpdate(o.launch.taskInfo.taskId, Mesos.TaskStatus.TASK_RUNNING)
        }.toList
      case _ =>
        Nil
    }
  }

}
