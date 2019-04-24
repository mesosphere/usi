package com.mesosphere.usi.core.conf

import com.typesafe.config.Config

case class SchedulerSettings(persistencePipelineLimit: Int)

object SchedulerSettings {
  def fromConfig(schedulerConf: Config): SchedulerSettings = {
    val persistencePipelineLimit = schedulerConf.getInt("persistence.pipeline-limit")
    SchedulerSettings(persistencePipelineLimit)
  }
}
