package com.mesosphere.usi.core.models

import com.typesafe.scalalogging.StrictLogging

/**
  * Strategy which dictates how task ids are generated from their task name.
  */
abstract class PodTaskIdStrategy {
  def apply(podId: PodId, taskName: TaskName): TaskId
  def unapply(taskId: TaskId): Option[(PodId, TaskName)]
  final def curried(podId: PodId): CurriedPodTaskIdStrategy = CurriedPodTaskIdStrategy(podId, this)
}

object PodTaskIdStrategy {

  /**
    * Default strategy to convert between podId and taskName. Uses a '.' to delimit the podId and the taskName.
    *
    * When this strategy is used, it is illegal to use '.' in podIds.
    */
  object DefaultStrategy extends PodTaskIdStrategy with StrictLogging {
    override def apply(podId: PodId, taskName: TaskName): TaskId = {
      if (taskName == TaskName.empty)
        TaskId(podId.value)
      else {
        if (podId.value.contains(".")) {
          logger.error(
            s"podId ${podId.value} contains a '.', which is the delimiter character; this results in an unparseable taskId."
          )
        }
        TaskId(podId.value + "." + taskName.value)
      }
    }

    override def unapply(taskId: TaskId): Option[(PodId, TaskName)] =
      taskId.value.split("\\.", 2) match {
        case Array(podId, taskName) => Some((PodId(podId), TaskName(taskName)))
        case Array(podId) => Some((PodId(podId), TaskName.empty))
        case _ => None
      }
  }
}
