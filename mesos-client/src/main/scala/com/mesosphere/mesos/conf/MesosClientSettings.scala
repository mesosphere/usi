package com.mesosphere.mesos.conf

import java.net.URL

import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.concurrent.duration._

class MesosClientSettings private (
    val masters: Seq[URL],
    val redirectRetries: Int = 3,
    val idleTimeout: FiniteDuration = 75.seconds,
    val sourceBufferSize: Int = 10) {

  private def copy(masters: Seq[URL] = this.masters, redirectRetries: Int = this.redirectRetries, idleTimeout: FiniteDuration = this.idleTimeout, sourceBufferSize: Int = this.sourceBufferSize) =
    new MesosClientSettings(masters, redirectRetries, idleTimeout, sourceBufferSize)


  def withMasters(urls: URL*): MesosClientSettings = copy(masters = urls)
}

object MesosClientSettings {
  def fromConfig(conf: Config): MesosClientSettings = {
    val masterUrls: Seq[URL] = conf.getStringList("master-url").asScala.map(new URL(_))
    val redirectRetries = conf.getInt("redirect-retries")

    // we want FiniteDuration, the conversion is needed to achieve that
    val idleTimeout = Duration.fromNanos(conf.getDuration("idle-timeout").toNanos)

    val sourceBufferSize = conf.getInt("back-pressure.source-buffer-size")

    new MesosClientSettings(masterUrls, redirectRetries, idleTimeout, sourceBufferSize)

  }
}
