package com.mesosphere.usi.core.conf

import java.time.Duration

import com.typesafe.config.{Config, ConfigFactory}

class SchedulerSettings private (val persistencePipelineLimit: Int, val persistenceLoadTimeout: Duration) {

  private def copy(
      persistencePipelineLimit: Int = this.persistencePipelineLimit,
      persistenceLoadTimeout: Duration = this.persistenceLoadTimeout): SchedulerSettings =
    new SchedulerSettings(persistencePipelineLimit, persistenceLoadTimeout)

  def withPersistencePipelineLimit(limit: Int): SchedulerSettings = copy(persistencePipelineLimit = limit)

  def withPersistenceLoadTimeout(timeout: Duration): SchedulerSettings = copy(persistenceLoadTimeout = timeout)
}

object SchedulerSettings {
  def fromConfig(schedulerConf: Config): SchedulerSettings = {
    val persistenceConf = schedulerConf.getConfig("persistence")
    val persistencePipelineLimit = persistenceConf.getInt("pipeline-limit")
    val persistenceLoadTimeout = persistenceConf.getDuration("load-timeout")
    new SchedulerSettings(persistencePipelineLimit, persistenceLoadTimeout)
  }

  def load(): SchedulerSettings = {
    val conf = ConfigFactory.load()
    fromConfig(conf.getConfig("usi.scheduler"))
  }

  def load(loader: ClassLoader): SchedulerSettings = {
    val conf = ConfigFactory.load(loader)
    fromConfig(conf.getConfig("usi.scheduler"))
  }
}
