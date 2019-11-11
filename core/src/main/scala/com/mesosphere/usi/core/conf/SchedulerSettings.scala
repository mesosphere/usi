package com.mesosphere.usi.core.conf

import java.time.Duration

import com.typesafe.config.{Config, ConfigFactory}

class SchedulerSettings private (
    val persistencePipelineLimit: Int,
    val persistenceLoadTimeout: Duration,
    val defaultRole: String,
    val debounceReviveInterval: Duration) {

  private def copy(
      persistencePipelineLimit: Int = this.persistencePipelineLimit,
      persistenceLoadTimeout: Duration = this.persistenceLoadTimeout,
      defaultRole: String = this.defaultRole,
      debounceReviveInterval: Duration = this.debounceReviveInterval): SchedulerSettings =
    new SchedulerSettings(persistencePipelineLimit, persistenceLoadTimeout, defaultRole, debounceReviveInterval)

  def withPersistencePipelineLimit(limit: Int): SchedulerSettings = copy(persistencePipelineLimit = limit)

  def withPersistenceLoadTimeout(timeout: Duration): SchedulerSettings = copy(persistenceLoadTimeout = timeout)

  def withDefaultRole(role: String): SchedulerSettings = copy(defaultRole = role)

  def withDebounceReviveInterval(minReviveOffersInterval: Duration): SchedulerSettings =
    copy(debounceReviveInterval = minReviveOffersInterval)
}

object SchedulerSettings {
  def fromConfig(schedulerConf: Config): SchedulerSettings = {
    val persistenceConf = schedulerConf.getConfig("persistence")
    val persistencePipelineLimit = persistenceConf.getInt("pipeline-limit")
    val persistenceLoadTimeout = persistenceConf.getDuration("load-timeout")
    val defaultRole = schedulerConf.getString("default-role")
    val reviveConf = schedulerConf.getConfig("revive")
    val debounceReviveInterval = reviveConf.getDuration("debounce-revive-interval")
    new SchedulerSettings(persistencePipelineLimit, persistenceLoadTimeout, defaultRole, debounceReviveInterval)
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
