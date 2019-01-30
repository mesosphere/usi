package com.mesosphere.mesos.client

import java.net.URI

import com.mesosphere.mesos.conf.MesosClientSettings
import org.apache.mesos.v1.Protos.FrameworkID

case class MesosConnectionContext(url: URI,
                                  streamId: Option[String],
                                  frameworkId: Option[FrameworkID]) {
  def host = url.getHost
  def port = url.getPort
}

object MesosConnectionContext {
  def apply(conf: MesosClientSettings): MesosConnectionContext =
    MesosConnectionContext(new java.net.URI(s"http://${conf.master}"),
                           None,
                           None)
}
