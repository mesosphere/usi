package com.mesosphere.usi.core.models

case class CurriedPodTaskIdStrategy(podId: PodId, strategy: PodTaskIdStrategy) {
  def apply(partialTaskId: PartialTaskId): TaskId = strategy.apply(podId, partialTaskId)
}

object CurriedPodTaskIdStrategy {
  def default(podId: PodId): CurriedPodTaskIdStrategy =
    CurriedPodTaskIdStrategy(podId, PodTaskIdStrategy.defaultStrategy)
}
