package com.mesosphere.usi.core.models

case class CurriedPodTaskIdStrategy(podId: PodId, strategy: PodTaskIdStrategy) {
  def apply(taskName: TaskName): TaskId = strategy.apply(podId, taskName)
}

object CurriedPodTaskIdStrategy {
  def default(podId: PodId): CurriedPodTaskIdStrategy =
    CurriedPodTaskIdStrategy(podId, PodTaskIdStrategy.DefaultStrategy)
}
