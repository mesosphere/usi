package com.mesosphere.usi.helloworld

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.mesosphere.mesos.client.{CredentialsProvider, MesosClient}
import com.mesosphere.mesos.conf.MesosClientSettings
import org.apache.mesos.v1.Protos.FrameworkInfo

import scala.concurrent.duration._
import scala.concurrent.Await

/**
  * Helper that builds a Mesos client that can be used by USI
  */
class KeepAliveMesosClientFactory(settings: MesosClientSettings, authorization: Option[CredentialsProvider])(
    implicit system: ActorSystem,
    mat: ActorMaterializer) {

  val frameworkInfo = FrameworkInfo
    .newBuilder()
    .setUser("root")
    .setName("KeepAliveFramework")
    .addRoles("test")
    .addCapabilities(FrameworkInfo.Capability.newBuilder().setType(FrameworkInfo.Capability.Type.MULTI_ROLE))
    .setFailoverTimeout(0d)
    .build()

  val client: MesosClient =
    Await.result(MesosClient(settings, frameworkInfo, authorization).runWith(Sink.head), 10.seconds)
}
