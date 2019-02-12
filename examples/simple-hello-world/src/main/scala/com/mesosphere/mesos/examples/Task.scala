package com.mesosphere.mesos.examples

import java.util.UUID

import org.apache.mesos.v1.Protos.{AgentID, TaskState}

/**
  * A minimalistic Mesos task representation
  *
  * @param spec task spec
  * @param taskId task Id that is unique for every Mesos task. It is of the form [[Spec.name]].[[UUID]]
  *               e.g. `sleep.5e5601ab-01c7-4299-9eb1-0741763d2749`
  * @param state current [[TaskState]] as reported by Mesos. It's optional since it does not exists until
  *                the first status update arrives from Mesos
  * @param agentId tasks agent Id. It's optional since it does not exists until the task is scheduled to run
  */
case class Task(spec: Spec, taskId: String, state: Option[TaskState], agentId: Option[AgentID]) {

  def this(spec: Spec, state: Option[TaskState] = None, agentId: Option[AgentID] = None) =
    this(spec, taskId = s"${spec.name}.${UUID.randomUUID()}", state, agentId)

  /**
    * An existing task is always scheduled for launch, unless it's already being launched or is running. In the later
    * case task's agent Id is set (right before we accept an offer to launch the task). This logic also ignores the
    * case of restarting a failed/finished task since this framework doesn't handle this case anyway.
    */
  val isScheduled: Boolean = agentId.isEmpty
}
