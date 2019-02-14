package com.mesosphere.usi.core.logic
import com.mesosphere.usi.core.models.{PodId, PodSpec, TaskId}

object SchedulerLogicHelpers {
  def podIdFor(taskId: TaskId): PodId = PodId(taskId.value)

  def taskIdsFor(pod: PodSpec): Seq[TaskId] = {
    // TODO - temporary stub implementation
    Seq(TaskId(pod.id.value))
  }
}
