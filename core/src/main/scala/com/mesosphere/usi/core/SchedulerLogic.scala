package com.mesosphere.usi.core

import akka.stream._
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import com.mesosphere.usi.models._

import scala.collection.mutable


class SchedulerLogicGraph extends GraphStage[BidiShape[SpecMessage, StatusMessage, Mesos.Event, Mesos.Call]] {
  val mesosEvents = Inlet[Mesos.Event]("mesos-events")
  val mesosCalls = Outlet[Mesos.Call]("mesos-calls")

  val specMessages = Inlet[SpecMessage]("specs")
  val statuses = Outlet[StatusMessage]("statuses")
  // Define the (sole) output port of this stage
  val out: Outlet[Int] = Outlet("NumbersSource")
  // Define the shape of this stage, which is SourceShape with the port we defined above
  override val shape: BidiShape[SpecMessage, StatusMessage, Mesos.Event, Mesos.Call] = BidiShape(specMessages, statuses, mesosEvents, mesosCalls)

  // This is where the actual (possibly stateful) logic will live
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    val schedulerLogic = new SchedulerLogic

    new GraphStageLogic(shape) {
      val pendingMesosCalls: mutable.Queue[Mesos.Call] = mutable.Queue.empty
      val pendingStatusMessages: mutable.Queue[StatusMessage] = mutable.Queue.empty

      setHandler(mesosEvents, new InHandler {
        override def onPush(): Unit = {
          handleResponse(schedulerLogic.handleMesosEvent(grab(mesosEvents)))
        }
      })

      setHandler(specMessages, new InHandler {
        override def onPush(): Unit = {
          handleResponse(schedulerLogic.handleSpecMessage(grab(specMessages)))
        }
      })

      setHandler(statuses, new OutHandler {
        override def onPull(): Unit = {
          pushQueuedStatusCall()
        }
      })

      setHandler(mesosCalls, new OutHandler {
        override def onPull(): Unit = {
          if (pendingMesosCalls.nonEmpty)
            push(mesosCalls, pendingMesosCalls.dequeue())
        }
      })

      override def preStart(): Unit = {
        // Start the stream
        pull(specMessages)
        pull(mesosEvents)
      }

      private def pushQueuedStatusCall(): Unit = {
        if (pendingStatusMessages.nonEmpty)
          push(statuses, pendingStatusMessages.dequeue())
      }
      private def pushQueuedMesosCall(): Unit = {
        if (pendingMesosCalls.nonEmpty)
          push(mesosCalls, pendingMesosCalls.dequeue())
      }

      def handleResponse(response: Response): Unit = {
        response.mesosCalls.foreach { call =>
          pendingMesosCalls.enqueue(call)
        }
        response.statusMessages.foreach { status =>
          pendingStatusMessages.enqueue(status)
        }
        maybePush()
      }

      def maybePush(): Unit = {
        if (isAvailable(statuses))
          pushQueuedStatusCall()
        if (isAvailable(mesosCalls))
          pushQueuedMesosCall()
      }

      val BUFFER_SIZE = 32
      def maybePull(): Unit = {
        if ((pendingMesosCalls.length < BUFFER_SIZE) && (pendingStatusMessages.length < BUFFER_SIZE)) {
          if (!hasBeenPulled(mesosEvents)) {
            pull(mesosEvents)
          }
          if (!hasBeenPulled(specMessages)) {
            pull(specMessages)
          }
        }
      }
    }
  }
}

sealed trait Response {
  def statusMessages: Seq[StatusMessage]

  def mesosCalls: Seq[Mesos.Call]
}

case class ResponseWithMessages(statusMessages: List[StatusMessage] = Nil, mesosCalls: List[Mesos.Call] = Nil) extends Response

case class ResponseBuilder(reverseStatusMessage: List[StatusMessage] = Nil, reverseMesosCall: List[Mesos.Call] = Nil) extends Response {
  lazy val statusMessages = reverseStatusMessage.reverse
  lazy val mesosCalls = reverseMesosCall.reverse

  def ++(other: ResponseBuilder): ResponseBuilder = {
    if (other == ResponseBuilder.empty)
      this
    else if (this == ResponseBuilder.empty)
      other
    else
      ResponseBuilder(other.reverseStatusMessage ++ reverseStatusMessage, other.reverseMesosCall ++ reverseMesosCall)
  }

  def withStatusMessage(message: StatusMessage): ResponseBuilder = copy(reverseStatusMessage = message :: reverseStatusMessage)

  def withMesosCall(call: Mesos.Call): ResponseBuilder = copy(reverseMesosCall = call :: reverseMesosCall)
}

object ResponseBuilder {
  val empty = ResponseBuilder()
}

class SchedulerLogic {
  var podSpecs: Map[PodId, PodSpec] = Map.empty
  var reservationSpecs: Map[ReservationId, ReservationSpec] = Map.empty
  var podRecords: Map[PodId, PodRecord] = Map.empty

  var podStatuses: Map[PodId, PodStatus] = Map.empty
  var reservationStatuses: Map[ReservationId, ReservationStatus] = Map.empty

  var pendingLaunchPods: Set[PodId] = Set.empty

  /**
    * Returns a revive mesos call if there are pods to launch
    * @return
    */
  def maybeRevive: ResponseBuilder = {
    if (this.pendingLaunchPods.nonEmpty)
      ResponseBuilder.empty.withMesosCall(Mesos.Call.Revive)
    else
      ResponseBuilder.empty.withMesosCall(Mesos.Call.Suppress)
  }

  def taskIdsFor(pod: PodSpec): Seq[TaskId] = {
    // ignoring momentarily that these might be different
    Seq(pod.id)
  }

  def podIdFor(taskId: TaskId): PodId = taskId

  private def assertValidTransition(oldSpec: PodSpec, newSpec: PodSpec): Unit = {
    if ((oldSpec.goal == Goal.Terminal) && (newSpec.goal == Goal.Running))
      throw new IllegalStateException(s"Illegal state transition Terminal to Running for podId ${oldSpec.id}")
  }

  private def updatePodStatus(podId: PodId, newState: Option[PodStatus], responseBuilder: ResponseBuilder): ResponseBuilder = {
    newState match {
      case Some(podStatus) =>
        this.podStatuses = this.podStatuses.updated(podId, podStatus)
      case None =>
        this.podStatuses -= podId
    }
    responseBuilder.withStatusMessage(PodStatusChange(podId, newState))
  }

  private def handlePodSpecUpdate(podId: PodId, newState: Option[PodSpec], responseBuilder: ResponseBuilder): ResponseBuilder = {
    var b = responseBuilder
    newState match {
      case None =>
        this.podSpecs -= podId
        this.podRecords -= podId
        this.pendingLaunchPods -= podId
        b = updatePodStatus(podId, None, b)

      case Some(podSpec) =>
        this.podSpecs.get(podId).foreach(assertValidTransition(_, podSpec))

        this.podSpecs = this.podSpecs.updated(podId, podSpec)
        val needsLaunch = (podSpec.goal == Goal.Running) && podRecords.contains(podId)
        if (needsLaunch)
          this.pendingLaunchPods += podId
        else
          this.pendingLaunchPods -= podId

        if (podSpec.goal == Goal.Terminal) {
          taskIdsFor(podSpec).foreach { taskId =>
            b = b.withMesosCall(Mesos.Call.Kill(taskId))
          }
        }
    }

    b
  }

  def handleSpecMessage(msg: SpecMessage): Response = msg match {
    case SpecsSnapshot(podSpecSnapshot, reservationSpecs) =>
      if (reservationSpecs.nonEmpty) {
        throw new NotImplementedError("ReservationSpec support not yet implemented")
      }
      val newPodsSpecs: Map[PodId, PodSpec] = podSpecSnapshot.map { pod => pod.id -> pod }(collection.breakOut)

      val responseFromUpdate = (this.podSpecs.keySet ++ newPodsSpecs.keySet).foldLeft(ResponseBuilder.empty) { (response, podId) =>
        handlePodSpecUpdate(podId, newPodsSpecs.get(podId), response)
      }

      responseFromUpdate ++ maybeRevive

    case PodSpecChange(id, newState) =>
      val responseFromUpdate = handlePodSpecUpdate(id, newState, ResponseBuilder.empty)
      responseFromUpdate ++ maybeRevive

    case ReservationSpecChange(id, _) =>
      throw new NotImplementedError("ReservationSpec support not yet implemented")
  }

  def handleMesosEvent(event: Mesos.Event): Response = event match {
    case Mesos.Event.Offer(offerId) =>
      val operations: Seq[Mesos.Operation] = this.pendingLaunchPods
        .iterator
        .flatMap { podId =>
          taskIdsFor(this.podSpecs(podId)).iterator.map { taskId =>
            Mesos.Operation(Mesos.Launch(Mesos.TaskInfo(taskId)))
          }
        }
        .to[Seq]
      val response = ResponseBuilder.empty.withMesosCall(Mesos.Call.Accept(offerId, operations = operations))
      this.pendingLaunchPods = Set.empty

      response ++ maybeRevive

    case Mesos.Event.Update(taskState) =>
      val podId = podIdFor(taskState.taskId)
      if (podSpecs.contains(podId)) {
        updatePodStatus(podId, Some(PodStatus(podId, Map(taskState.taskId -> taskState.status))), ResponseBuilder.empty)
      } else {
        // for now, ignore spurious podStatuses
        ResponseBuilder.empty
      }
  }
}
