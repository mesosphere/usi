package com.mesosphere.mesos.conf

import java.net.URL

import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConverters._
import scala.concurrent.duration._

class MesosClientSettings private (
    val masters: Seq[URL],
    val maxRedirects: Int,
    val idleTimeout: FiniteDuration,
    val sourceBufferSize: Int) {

  private def copy(
      masters: Seq[URL] = this.masters,
      maxRedirects: Int = this.maxRedirects,
      idleTimeout: FiniteDuration = this.idleTimeout,
      sourceBufferSize: Int = this.sourceBufferSize) =
    new MesosClientSettings(masters, maxRedirects, idleTimeout, sourceBufferSize)

  def withMasters(urls: URL*): MesosClientSettings = copy(masters = urls)

  def withMaster(url: URL) = withMasters(url)

  def withMaxRedirects(maxRedirects: Int): MesosClientSettings = copy(maxRedirects = maxRedirects)

  def withIdleTimeout(timeout: FiniteDuration): MesosClientSettings = copy(idleTimeout = timeout)

  def withSourceBufferSize(bufferSize: Int): MesosClientSettings = copy(sourceBufferSize = bufferSize)
}

object MesosClientSettings {
  def fromConfig(conf: Config): MesosClientSettings = {
    val masterUrls: Seq[URL] = conf.getStringList("master-url").asScala.map(new URL(_))
    val maxRedirects = conf.getInt("max-redirects")

    // we want FiniteDuration, the conversion is needed to achieve that
    val idleTimeout = Duration.fromNanos(conf.getDuration("idle-timeout").toNanos)

    val sourceBufferSize = conf.getInt("back-pressure.source-buffer-size")

    new MesosClientSettings(masterUrls, maxRedirects, idleTimeout, sourceBufferSize)

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
