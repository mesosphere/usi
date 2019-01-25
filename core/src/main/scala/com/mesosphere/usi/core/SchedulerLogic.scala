package com.mesosphere.usi.core
import akka.stream._
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import com.mesosphere.usi.models._


object Mesos {
  sealed trait Call
  case class Accept(offerId: String, operations: Seq[Operation]) extends Call
  // stub class that just launches as
  case class Operation(launch: Launch)
  case class Launch(taskInfo: TaskInfo)
  case class TaskInfo(taskId: String)

  sealed trait Event
  case class Update(status: TaskStatus) extends Event
  case class TaskStatus(taskId: String)
}

class SchedulerLogicGraph extends GraphStage[BidiShape[SpecMessage, StatusMessage, Mesos.Event, Mesos.Call]] {
  val mesosEvents = Inlet[Mesos.Event]("mesos-events")
  val mesosCalls = Outlet[Mesos.Call]("mesos-calls")

  val specs = Inlet[SpecMessage]("specs")
  val statuses = Outlet[StatusMessage]("statuses")
  // Define the (sole) output port of this stage
  val out: Outlet[Int] = Outlet("NumbersSource")
  // Define the shape of this stage, which is SourceShape with the port we defined above
  override val shape: BidiShape[SpecMessage, StatusMessage, Mesos.Event, Mesos.Call] = BidiShape(specs, statuses, mesosEvents, mesosCalls)

  // This is where the actual (possibly stateful) logic will live
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    val schedulerLogic = new SchedulerLogic

    new GraphStageLogic(shape) {
      setHandler(mesosEvents, new InHandler {
        override def onPush(): Unit = {
          grab(mesosEvents)
        }
      })

      setHandler(mesosCalls, new OutHandler {
        override def onPull(): Unit = ???
      })

      setHandler(specs, new InHandler {
        override def onPush(): Unit = {
          schedulerLogic.receive(grab(specs))
        }
      })

      setHandler(statuses, new OutHandler {
        override def onPull(): Unit = ???
      })
    }
  }
}

class SchedulerLogic {
  var podSpecs: Map[PodId, PodSpec] = Map.empty
  var reservationSpecs: Map[ReservationId, ReservationSpec] = Map.empty
  var podRecords: Map[PodId, PodRecord] = Map.empty

  sealed trait Response {
    def statusMessages: Seq[StatusMessage]
    def mesosCalls: Seq[Mesos.Call]
  }

  case class ResponseWithMessages(statusMessages: List[StatusMessage] = Nil, mesosCalls: List[Mesos.Call] = Nil) extends Response

  case class ResponseBuilder(reverseStatusMessage: List[StatusMessage] = Nil, reverseMesosCall: List[Mesos.Call] = Nil) extends Response {
    lazy val statusMessages = reverseStatusMessage.reverse
    lazy val mesosCalls = reverseMesosCall.reverse
    def withStatusMessage(message: StatusMessage): ResponseBuilder = copy(reverseStatusMessage = message :: reverseStatusMessage)
    def withMesosCall(call: Mesos.Call): ResponseBuilder = copy(reverseMesosCall = call :: reverseMesosCall)
  }
  object Response {
    val empty = Response()
  }

  def mockStatusFor(podSpec: PodSpec): PodStatus = {
    podSpec.goal match {
      case Goal.Running =>
        PodStatus(podSpec.id, taskStatuses = Map(podSpec.id -> MesosTaskStatus.TASK_RUNNING))
      case Goal.Terminal =>
        PodStatus(podSpec.id, taskStatuses = Map(podSpec.id -> MesosTaskStatus.TASK_KILLED))
    }
  }

  def receive(msg: SpecMessage): Response = msg match {
    case SpecsSnapshot(podSpecs, reservationSpecs) =>
      this.podSpecs = podSpecs.map { p => p.id -> p }(collection.breakOut)
      this.reservationSpecs = reservationSpecs.map { r => r.id -> r }(collection.breakOut)

      Response.empty
          .withStatusMessage()
      Response(
        statusMessage = StatusSnapshot(
          podStatuses = this.podSpecs.values.map(mockStatusFor)(collection.breakOut),
          reservationStatuses = Nil
        ) :: Nil)
    case ReservationSpecChange(id, _) =>
      ???
    case PodSpecChange(id, None) =>
      this.podSpecs -= id

      List(PodStatusChange(id, None))
    case PodSpecChange(id, Some(podSpec)) =>
      this.podSpecs += (id -> podSpec)

      List(PodStatusChange(id, Some(mockStatusFor(podSpec))))
  }
}
