package com.mesosphere.usi.helloworld.runspecs

import java.util.concurrent.ConcurrentHashMap

import com.mesosphere.usi.core.models.{
  PodRecord,
  PodRecordUpdatedEvent,
  PodSpecUpdatedEvent,
  PodStateEvent,
  PodStatus,
  PodStatusUpdatedEvent
}
import com.mesosphere.usi.helloworld.runspecs.InstanceStatus._
import com.typesafe.scalalogging.LazyLogging
import org.apache.mesos.v1.Protos.TaskState

import scala.collection.JavaConverters._

/**
  * Service tracker keeps a consistent view of all services that we run.
  * It can process updates in an idempotent way and maintain a correct snapshot of the state.
  */
trait InstanceTracker {

  def processUpdate(event: PodStateEvent): Unit

  def serviceState(id: ServiceSpecId): Option[ServiceState]

  def listStates(): Vector[ServiceState]

}

/**
  * Thread-safe in-memory implementation of the instance tracker.
  */
class InMemoryInstanceTracker extends InstanceTracker with LazyLogging {

  override def listStates(): Vector[ServiceState] = {
    instanceMap.values().asScala.toVector
  }

  override def serviceState(id: ServiceSpecId): Option[ServiceState] = {
    Option(instanceMap.get(id))
  }

  override def processUpdate(event: PodStateEvent): Unit = {

    val serviceSpecInstanceId = ServiceSpecInstanceId.fromPodId(event.id)
    val serviceSpecId = serviceSpecInstanceId.serviceSpecId
    val instanceId = serviceSpecInstanceId.instanceId
    instanceMap.compute(
      serviceSpecId,
      (_, state) => {
        val serviceState = Option(state).getOrElse(ServiceState(serviceSpecId, Map.empty))

        // find the required instance state
        val oldInstanceState = serviceState.instances.get(instanceId)

        // do an update of the instance state
        computeStateChange(oldInstanceState, event) match {
          case NoChanges =>
            serviceState

          case InstanceStateUpdated(updated) =>
            serviceState.copy(instances = serviceState.instances.updated(instanceId, updated))

          case InstanceStateRemoved =>
            serviceState.copy(instances = serviceState.instances - instanceId)

        }

      }
    )
  }

  private val instanceMap: ConcurrentHashMap[ServiceSpecId, ServiceState] = {
    new ConcurrentHashMap[ServiceSpecId, ServiceState]()
  }

  /**
    * State machine that computes the new state given the pod update
    * @param state Some(state) if the instance id is already known, none otherwise
    * @param event pod state event that is needed to be processed
    * @return the result of the event
    */
  private def computeStateChange(state: Option[ServiceInstanceState], event: PodStateEvent): StateChangeResult = {
    state match {
      case None =>
        event match {
          // No existing state but we have a record => we should start tracking related changes
          case PodRecordUpdatedEvent(podId, Some(newRecord)) =>
            val id = ServiceSpecInstanceId.fromPodId(podId)
            InstanceStateUpdated(ServiceInstanceState(id, SentToMesos(newRecord), isTerminating = false))

          // We know nothing about this particular instance, so we will just skip it
          // We also skip the PodSpecUpdatedEvent because PodRecords are enough to know about launching pods
          case _ => NoChanges

        }

      case Some(s) =>
        val podUpdateIncarnation = ServiceSpecInstanceId.fromPodId(event.id).incarnation
        val stateIncarnation = s.id.incarnation

        event match {
          // we have a new incarnation of the instance, and our current state is no longer valid
          case PodStatusUpdatedEvent(_, Some(podStatus)) if podUpdateIncarnation > stateIncarnation =>
            val newInstanceStatus: InstanceStatus = taskState(podStatus) match {
              case StagingTask() => StagingInstance(s.status.podRecord, podStatus)
              case RunningTask() => RunningInstance(s.status.podRecord, podStatus)
              case TerminalTask() => TerminalInstance(s.status.podRecord, podStatus)
            }
            val newIncarnationId = ServiceSpecInstanceId.fromPodId(event.id)
            val newState = s.copy(id = newIncarnationId, status = newInstanceStatus)
            clearStateIfNeeded(newState)

          // updates for the current incarnation
          case PodStatusUpdatedEvent(_, Some(podStatus)) if podUpdateIncarnation == stateIncarnation =>
            val newState = tryUpdatePodStatus(s, podStatus)
            clearStateIfNeeded(newState)

          // We found that there was a command to kill the pod, it means that the instance is no longer
          // important and we're going to remove it from state as soon as we receive the terminal status.
          case PodSpecUpdatedEvent(_, Some(podSpec)) if podSpec.shouldBeTerminal =>
            s.status match {
              // Instance state is already terminal - no more job required!
              case _: TerminalInstance => InstanceStateRemoved
              case _ => InstanceStateUpdated(s.copy(isTerminating = true))
            }

          // we're not interested in those updates
          case _ => NoChanges
        }

    }
  }

  private def clearStateIfNeeded(newState: ServiceInstanceState): StateChangeResult = {
    newState.status match {
      // If the instance state is terminal after the update and we were terminating -
      // we should remove it
      case _: TerminalInstance if newState.isTerminating =>
        InstanceStateRemoved
      // otherwise we mark it as terminated
      case _ =>
        InstanceStateUpdated(newState)
    }
  }

  trait StateChangeResult
  case class InstanceStateUpdated(newState: ServiceInstanceState) extends StateChangeResult
  case object NoChanges extends StateChangeResult
  case object InstanceStateRemoved extends StateChangeResult

  val staging = Set(
    TaskState.TASK_STAGING,
    TaskState.TASK_STARTING
  )

  val running = Set(
    TaskState.TASK_RUNNING,
    TaskState.TASK_KILLING,
  )

  val terminal = Set(
    TaskState.TASK_FINISHED,
    TaskState.TASK_FAILED,
    TaskState.TASK_KILLED,
    TaskState.TASK_ERROR,
    TaskState.TASK_DROPPED,
    TaskState.TASK_GONE,
  )

  val unreachable = Set(
    TaskState.TASK_UNREACHABLE,
    TaskState.TASK_LOST,
  )

  def taskState(podStatus: PodStatus): TaskState = {
    podStatus.taskStatuses.head._2.getState
  }

  object StagingTask {
    def unapply(state: TaskState): Boolean = staging(state)
  }
  object RunningTask {
    def unapply(state: TaskState): Boolean = running(state)
  }
  object TerminalTask {
    def unapply(state: TaskState): Boolean = terminal(state)
  }

  object UnreachableTask {
    def unapply(state: TaskState): Boolean = unreachable(state)
  }

  // TODO support multiple pods per instance
  def tryUpdatePodStatus(state: ServiceInstanceState, podStatus: PodStatus): ServiceInstanceState = {
    def IgnoreUpdate = state.status
    val newStatus = state.status match {
      case s: SentToMesos =>
        taskState(podStatus) match {
          case StagingTask() => s.toStaging(podStatus)
          case RunningTask() => s.toRunning(podStatus)
          case TerminalTask() => s.toTerminal(podStatus)
          case UnreachableTask() => s.toUnreachable(podStatus)
        }

      case s: StagingInstance =>
        taskState(podStatus) match {
          case StagingTask() => IgnoreUpdate
          case RunningTask() => s.toRunning(podStatus)
          case TerminalTask() => s.toTerminal(podStatus)
          case UnreachableTask() => s.toUnreachable(podStatus)
        }

      case s: RunningInstance =>
        taskState(podStatus) match {
          case StagingTask() => IgnoreUpdate
          case RunningTask() => IgnoreUpdate
          case TerminalTask() => s.toTerminal(podStatus)
          case UnreachableTask() => s.toUnreachable(podStatus)
        }

      case UnreachableInstance(podRecord, status, lastSeenStatus) =>
        lastSeenStatus match {
          case _: SentToMesos =>
            taskState(podStatus) match {
              case StagingTask() => StagingInstance(podRecord, podStatus)
              case RunningTask() => RunningInstance(podRecord, podStatus)
              case TerminalTask() => TerminalInstance(podRecord, podStatus)
              case UnreachableTask() => IgnoreUpdate
            }

          case _: StagingInstance =>
            taskState(podStatus) match {
              case StagingTask() => StagingInstance(podRecord, podStatus)
              case RunningTask() => RunningInstance(podRecord, podStatus)
              case TerminalTask() => TerminalInstance(podRecord, podStatus)
              case UnreachableTask() => IgnoreUpdate
            }

          case _: RunningInstance =>
            taskState(podStatus) match {
              case StagingTask() =>
                logger.warn("Ignoring unexpected transition to staging after being Unreachable(running)")
                IgnoreUpdate
              case RunningTask() => RunningInstance(podRecord, podStatus)
              case TerminalTask() => TerminalInstance(podRecord, podStatus)
              case UnreachableTask() => IgnoreUpdate
            }
        }

      case _: TerminalInstance => IgnoreUpdate
    }

    state.copy(status = newStatus)

  }

  def setTerminating(state: ServiceInstanceState): ServiceInstanceState = {
    state.copy(isTerminating = true)
  }
}

case class ServiceState(id: ServiceSpecId, instances: Map[InstanceId, ServiceInstanceState])

case class ServiceInstanceState(id: ServiceSpecInstanceId, status: InstanceStatus, isTerminating: Boolean)

trait InstanceStatus {
  def podRecord: PodRecord
}
object InstanceStatus {

  case class SentToMesos(podRecord: PodRecord) extends InstanceStatus {
    def toStaging(status: PodStatus): StagingInstance = StagingInstance(podRecord, status)
    def toRunning(status: PodStatus): RunningInstance = RunningInstance(podRecord, status)
    def toTerminal(status: PodStatus): TerminalInstance = TerminalInstance(podRecord, status)
    def toUnreachable(status: PodStatus): UnreachableInstance = UnreachableInstance(podRecord, status, this)
  }

  case class StagingInstance(podRecord: PodRecord, podStatus: PodStatus) extends InstanceStatus {
    def toRunning(status: PodStatus): RunningInstance = RunningInstance(podRecord, podStatus)
    def toTerminal(status: PodStatus): TerminalInstance = TerminalInstance(podRecord, podStatus)
    def toUnreachable(status: PodStatus): UnreachableInstance = UnreachableInstance(podRecord, status, this)
  }

  case class RunningInstance(podRecord: PodRecord, podStatus: PodStatus) extends InstanceStatus {
    def toTerminal(status: PodStatus): TerminalInstance = TerminalInstance(podRecord, status)
    def toUnreachable(status: PodStatus): UnreachableInstance = UnreachableInstance(podRecord, status, this)
  }

  case class TerminalInstance(podRecord: PodRecord, podStatus: PodStatus) extends InstanceStatus

  case class UnreachableInstance(podRecord: PodRecord, podStatus: PodStatus, lastSeenInstanceState: InstanceStatus)
      extends InstanceStatus {
    // check against possible bugs
    require {
      lastSeenInstanceState match {
        case _: TerminalInstance => false
        case _: UnreachableInstance => false
        case _ => true
      }
    }
  }

}
