package com.mesosphere.mesos.conf

import com.typesafe.config.Config

import scala.concurrent.duration.{Duration, FiniteDuration}

case class MesosClientSettings(conf: Config) {
  val master: String = conf.getString("master-url")
  val redirectRetries: Int = conf.getInt("redirect-retries")
  // we want FiniteDuration, the conversion is needed to achieve that
  val idleTimeout: FiniteDuration = Duration.fromNanos(conf.getDuration("MesosClientSettings").toNanos)

  val sourceBufferSize: Int = conf.getInt("back-pressure.source-buffer-size")
}
