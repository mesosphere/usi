package com.mesosphere.usi.core.models

abstract class PodTaskIdStrategy {
  def apply(podId: PodId, partialTaskId: PartialTaskId): TaskId
  def unapply(taskId: TaskId): Option[(PodId, PartialTaskId)]
  final def curried(podId: PodId): CurriedPodTaskIdStrategy = CurriedPodTaskIdStrategy(podId, this)
}

object PodTaskIdStrategy {
  def defaultStrategy: PodTaskIdStrategy = new PodTaskIdStrategy {
    override def apply(podId: PodId, partialTaskId: PartialTaskId): TaskId = {
      if (partialTaskId == PartialTaskId.empty)
        TaskId(podId.value)
      else
        TaskId(podId.value + "." + partialTaskId.value)
    }

    override def unapply(taskId: TaskId): Option[(PodId, PartialTaskId)] =
      taskId.value.split("\\.") match {
        case Array(podId, partialTaskId) => Some((PodId(podId), PartialTaskId(partialTaskId)))
        case Array(podId) => Some((PodId(podId), PartialTaskId.empty))
        case _ => None
      }
  }
}
