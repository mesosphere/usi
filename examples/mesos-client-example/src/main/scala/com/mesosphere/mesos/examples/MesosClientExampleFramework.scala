package com.mesosphere.mesos.examples

import java.net.URL
import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.mesosphere.mesos.client.{CredentialsProvider, JwtProvider, MesosClient, StrictLoggingFlow}
import com.mesosphere.mesos.conf.MesosClientSettings
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
class MesosClientExampleFramework(settings: MesosClientSettings, authorization: Option[CredentialsProvider])(implicit system: ActorSystem, materializer: ActorMaterializer) extends StrictLoggingFlow {
  implicit val executionContext = system.dispatcher

  val frameworkInfo = FrameworkInfo
    .newBuilder()
    .setPrincipal("strict-usi")
    .setUser("example")
    .setName("MesosClientExample")
    .setId(FrameworkID.newBuilder.setValue(UUID.randomUUID().toString))
    .addRoles("test")
    .addCapabilities(FrameworkInfo.Capability
      .newBuilder()
      .setType(FrameworkInfo.Capability.Type.MULTI_ROLE))
    .setFailoverTimeout(0d)
    .build()

  val client = Await.result(MesosClient(settings, frameworkInfo, authorization).runWith(Sink.head), 10.seconds)

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

object MesosClientExampleFramework {

  /**
    * Strict mode demo:
    *
    * 1. Launch strict cluster.
    * 2. Create key pair:
    *   {{{dcos security org service-accounts keypair usi.private.pem usi.pub.pem}}}
    * 3. Create user strict-usi:
    *   {{{dcos security org service-accounts create -p usi.pub.pem -d "For testing USI on strict" strict-usi}}}
    * 4. Grant strict-usi access:
    *   {{{curl -L -X PUT -k -H "Authorization: token=$(dcos config show core.dcos_acs_token)" "$(dcos config show core.dcos_url)/acs/api/v1/acls/dcos:superuser/users/strict-usi/full"}}}
    * 5. Download SSL certs:
    *   {{{wget --no-check-certificate -O dcos-ca.crt "$(dcos config show core.dcos_url)/ca/dcos-ca.crt"}}}
    * 6. Replace {{{dcosRoot}}} with public IP of cluster.
    * 7. Run!
    */
  def main(args: Array[String]): Unit = {

    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    implicit val context = system.dispatcher

    val dcosRoot = "https://34.209.126.195"
    val privateKey = scala.io.Source.fromFile("/Users/kjeschkies/Projects/usi/usi.private.pem").mkString
    val provider = JwtProvider("strict-usi", privateKey, dcosRoot)

    val mesosUrl = new URL(s"$dcosRoot/mesos")
    val clientSettings = MesosClientSettings.load().withMasters(Seq(mesosUrl))
    new MesosClientExampleFramework(clientSettings, Some(provider))
  }

//  def apply(settings: MesosClientSettings): MesosClientExampleFramework = new MesosClientExampleFramework(settings)
}
