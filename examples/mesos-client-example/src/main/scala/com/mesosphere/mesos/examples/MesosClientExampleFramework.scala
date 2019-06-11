package com.mesosphere.mesos.examples

import java.net.URL
import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl.Sink
import com.mesosphere.mesos.client.{CredentialsProvider, DcosServiceAccountProvider, MesosClient, StrictLoggingFlow}
import com.mesosphere.mesos.conf.MesosClientSettings
import org.apache.mesos.v1.Protos
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
class MesosClientExampleFramework(settings: MesosClientSettings, authorization: Option[CredentialsProvider] = None)(
    implicit system: ActorSystem,
    materializer: Materializer)
    extends StrictLoggingFlow {
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
    .mapConcat[Protos.OfferID] { event =>
      if (event.getType == Event.Type.SUBSCRIBED) {
        logger.info("Successfully subscribed to mesos")
        List.empty
      } else if (event.getType == Event.Type.OFFERS) {
        event.getOffers.getOffersList.asScala.map(_.getId).toList
      } else {
        logger.info(s"Ignoring event $event")
        List.empty
      }
    }
    .via(info(s"Declining offer with id = ")) // Decline all offers
    .map { oId =>
      client.calls.newDecline(
        offerIds = Seq(oId),
        filters = Some(Filters.newBuilder().setRefuseSeconds(5.0).build())
      )
    }
    .runWith(client.mesosSink)
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
    * 1. Launch strict cluster and setup the DC/OS CLI.
    * 2. Create key pair:
    *   {{{dcos security org service-accounts keypair usi.private.pem usi.pub.pem}}}
    * 3. Create user strict-usi:
    *   {{{dcos security org service-accounts create -p usi.pub.pem -d "For testing USI on strict" strict-usi}}}
    * 4. Grant strict-usi access:
    *   {{{curl -L -X PUT -k -H "Authorization: token=$(dcos config show core.dcos_acs_token)" "$(dcos config show core.dcos_url)/acs/api/v1/acls/dcos:superuser/users/strict-usi/full"}}}
    * 5. Download SSL certs:
    *   {{{wget --no-check-certificate -O dcos-ca.crt "$(dcos config show core.dcos_url)/ca/dcos-ca.crt"}}}
    * 6. Run with {{{DCOS_CERT=path/to/dcos-ca.crt ./gradlew :mesos-client-example:run --stacktrace --args "https://<DC/OS IP> path/to/usi.private.pem"}}}
    */
  def main(args: Array[String]): Unit = {

    require(args.length == 2, "Too many arguments")

    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    implicit val context = system.dispatcher

    val dcosRoot = new URL(args(0))
    val privateKey = scala.io.Source.fromFile(args(1)).mkString
    val provider = DcosServiceAccountProvider("strict-usi", privateKey, dcosRoot)

    val mesosUrl = new URL(s"$dcosRoot/mesos")
    val clientSettings = MesosClientSettings.load().withMasters(Seq(mesosUrl))
    new MesosClientExampleFramework(clientSettings, Some(provider))
  }

  def apply(settings: MesosClientSettings)(
      implicit system: ActorSystem,
      materializer: Materializer): MesosClientExampleFramework = new MesosClientExampleFramework(settings)
}
