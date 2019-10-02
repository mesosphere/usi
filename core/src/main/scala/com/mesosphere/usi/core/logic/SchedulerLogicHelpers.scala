package com.mesosphere.usi.core.logic
import com.mesosphere.usi.core.models.{PodId, RunningPodSpec, TaskId}

object SchedulerLogicHelpers {
  def podIdFor(taskId: TaskId): PodId = PodId(taskId.value)

  // TODO - delete
  def taskIdsFor(pod: RunningPodSpec): Seq[TaskId] = {
    // Assumes taskId is podId. This will not be the case once we implement real support for Mesos TaskGroups: DCOS-48502
    Seq(TaskId(pod.id.value))
  }
}
