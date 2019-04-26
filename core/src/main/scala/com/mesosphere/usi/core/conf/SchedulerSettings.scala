package com.mesosphere.usi.core.conf

import com.typesafe.config.Config

case class SchedulerSettings(persistencePipelineLimit: Int, persistenceLoadTimeout: Int)

object SchedulerSettings {
  def fromConfig(schedulerConf: Config): SchedulerSettings = {
    val persistenceConf = schedulerConf.getConfig("persistence")
    val persistencePipelineLimit = persistenceConf.getInt("pipeline-limit")
    val persistenceLoadTimeout = persistenceConf.getInt("load-timeout")
    SchedulerSettings(persistencePipelineLimit, persistenceLoadTimeout)
  }
}
