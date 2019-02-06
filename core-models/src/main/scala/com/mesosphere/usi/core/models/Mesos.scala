package com.mesosphere.usi.core.models

/**
  * Mock Mesos calls / state. We'll remove these
  */
object Mesos {

  sealed trait Call

  object Call {

    case object Revive extends Call

    case object Suppress extends Call

    case class Accept(offerId: String, operations: Seq[Operation]) extends Call

    case class Kill(taskId: TaskId) extends Call

  }

  // stub class that just launches as
  case class Operation(launch: Launch)

  case class Launch(taskInfo: TaskInfo)

  case class TaskInfo(taskId: TaskId)

  sealed trait TaskStatus

  object TaskStatus {

    case object TASK_RUNNING extends TaskStatus

    case object TASK_KILLED extends TaskStatus

  }

  sealed trait Event

  object Event {

    case class Offer(offerId: String, agentId: AgentId) extends Event

    case class StatusUpdate(taskId: TaskId, status: TaskStatus) extends Event

  }

}
