package com.mesosphere.mesos.conf

import com.typesafe.config.Config
import scala.concurrent.duration._

case class MesosClientSettings(
    master: String,
    redirectRetries: Int = 3,
    idleTimeout: FiniteDuration = 75.seconds,
    sourceBufferSize: Int = 10) {}

object MesosClientSettings {
  def fromConfig(conf: Config): MesosClientSettings = {
    val master = conf.getString("master-url")
    val redirectRetries = conf.getInt("redirect-retries")

    // we want FiniteDuration, the conversion is needed to achieve that
    val idleTimeout = Duration.fromNanos(conf.getDuration("idle-timeout").toNanos)

    val sourceBufferSize = conf.getInt("back-pressure.source-buffer-size")

    MesosClientSettings(master, redirectRetries, idleTimeout, sourceBufferSize)

  }
}
