package com.mesosphere.usi.helloworld

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.mesosphere.mesos.client.MesosClient
import com.mesosphere.mesos.conf.MesosClientSettings
import com.typesafe.config.Config
import org.apache.mesos.v1.Protos.FrameworkInfo

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.sys.SystemProperties

/**
  * Helper that builds a mesos client that can be used by USI
  */
class KeepAliveMesosClientFactory(conf: Config)(implicit system: ActorSystem, mat: ActorMaterializer) {

  val settings = MesosClientSettings(conf.getString("master-url"))
  val frameworkInfo = FrameworkInfo
    .newBuilder()
    .setUser(
      new SystemProperties()
        .get("user.name")
        .getOrElse(throw new IllegalArgumentException("A local user is needed to launch Mesos tasks")))
    .setName("KeepAliveFramework")
    .addRoles("test")
    .addCapabilities(FrameworkInfo.Capability.newBuilder().setType(FrameworkInfo.Capability.Type.MULTI_ROLE))
    .setFailoverTimeout(0d)
    .build()

  val client: MesosClient = Await.result(MesosClient(settings, frameworkInfo).runWith(Sink.head), 10.seconds)
}
