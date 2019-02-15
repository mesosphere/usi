package com.mesosphere.usi.core.logic
import com.mesosphere.usi.core.models.{PodId, PodSpec, TaskId}

object SchedulerLogicHelpers {
  def podIdFor(taskId: TaskId): PodId = PodId(taskId.value)

  def taskIdsFor(pod: PodSpec): Seq[TaskId] = {
    // Assumes taskId is podId. This will not be the case once we implement real support for Mesos TaskGroups: DCOS-48502
    Seq(TaskId(pod.id.value))
  }
}
