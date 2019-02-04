package com.mesosphere.mesos.examples

import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.mesosphere.mesos.client.{MesosClient, StrictLoggingFlow}
import com.mesosphere.mesos.conf.MesosClientSettings
import com.typesafe.config.ConfigFactory
import org.apache.mesos.v1.Protos.{Filters, FrameworkID, FrameworkInfo}
import org.apache.mesos.v1.scheduler.Protos.Event

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import scala.collection.JavaConverters._

/**
  * Run a mesos-client example framework that:
  *  - uses only the raw mesos-client
  *  - successfully subscribes to Mesos master
  *  - declines all offers
  *
  *  Not much, but shows the basic idea. Good to test against local Mesos.
  *
  */
object MesosClientExampleFramework extends App with StrictLoggingFlow {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher

  val frameworkInfo = FrameworkInfo
    .newBuilder()
    .setUser("example")
    .setName("MesosClientExample")
    .setId(FrameworkID.newBuilder.setValue(UUID.randomUUID().toString))
    .addRoles("test")
    .addCapabilities(FrameworkInfo.Capability
      .newBuilder()
      .setType(FrameworkInfo.Capability.Type.MULTI_ROLE))
    .setFailoverTimeout(0d)
    .build()

  val settings = MesosClientSettings(ConfigFactory.load().getConfig("mesos-client"))
  val client = Await.result(MesosClient(settings, frameworkInfo).runWith(Sink.head), 10.seconds)

  client.mesosSource
    .runWith(Sink.foreach { event =>
      if (event.getType == Event.Type.SUBSCRIBED) {
        logger.info("Successfully subscribed to mesos")
      } else if (event.getType == Event.Type.OFFERS) {

        val offerIds = event.getOffers.getOffersList.asScala.map(_.getId).toList

        Source(offerIds)
          .via(info(s"Declining offer with id = ")) // Decline all offers
          .map(
            oId =>
              client.calls.newDecline(
                offerIds = Seq(oId),
                filters = Some(Filters.newBuilder().setRefuseSeconds(5.0).build())
            ))
          .runWith(client.mesosSink)
      }

    })
    .onComplete {
      case Success(res) =>
        logger.info(s"Stream completed: $res"); system.terminate()
      case Failure(e) =>
        logger.error(s"Error in stream: $e"); system.terminate()
    }
}
