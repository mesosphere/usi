package com.mesosphere.mesos.conf

import java.net.URL

import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConverters._
import scala.concurrent.duration._

class MesosClientSettings private (
    val masters: Seq[URL],
    val redirectRetries: Int,
    val idleTimeout: FiniteDuration,
    val sourceBufferSize: Int) {

  private def copy(
      masters: Seq[URL] = this.masters,
      redirectRetries: Int = this.redirectRetries,
      idleTimeout: FiniteDuration = this.idleTimeout,
      sourceBufferSize: Int = this.sourceBufferSize) =
    new MesosClientSettings(masters, redirectRetries, idleTimeout, sourceBufferSize)

  def withMasters(urls: Seq[URL]): MesosClientSettings = copy(masters = urls)

  def withMaster(url: URL) = withMasters(Seq(url))

  def withRedirectRetries(redirectRetries: Int): MesosClientSettings = copy(redirectRetries = redirectRetries)

  def withIdleTimeout(timeout: FiniteDuration): MesosClientSettings = copy(idleTimeout = timeout)

  def withSourceBufferSize(bufferSize: Int): MesosClientSettings = copy(sourceBufferSize = bufferSize)
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

  def load(): MesosClientSettings = {
    val conf = ConfigFactory.load();
    MesosClientSettings.fromConfig(conf.getConfig("mesos-client"))
  }

  def load(loader: ClassLoader): MesosClientSettings = {
    val conf = ConfigFactory.load(loader);
    MesosClientSettings.fromConfig(conf.getConfig("mesos-client"))
  }
}
