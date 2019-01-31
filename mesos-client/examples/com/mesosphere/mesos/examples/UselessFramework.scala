package mesosphere.mesos.examples

import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.config.ConfigFactory
import com.mesosphere.mesos.client.{MesosClient, StrictLoggingFlow}
import com.mesosphere.mesos.conf.MesosClientSettings
import org.apache.mesos.v1.Protos.{Filters, FrameworkID, FrameworkInfo}
import org.apache.mesos.v1.scheduler.Protos.Event

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * Run Foo framework that:
  *  - successfully subscribes
  *  - declines all offers.
  *
  *  Not much, but shows the basic idea. Good to test against local mesos.
  *
  */
object UselessFramework extends App with StrictLoggingFlow {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher

  val frameworkInfo = FrameworkInfo.newBuilder()
    .setUser("foo")
    .setName("Useless Framework")
    .setId(FrameworkID.newBuilder.setValue(UUID.randomUUID().toString))
    .addRoles("test")
    .addCapabilities(FrameworkInfo.Capability
      .newBuilder()
      .setType(FrameworkInfo.Capability.Type.MULTI_ROLE))
    .build()

  val conf = MesosClientSettings(ConfigFactory.load().getConfig("mesos-client"))
  val client = Await.result(MesosClient(conf, frameworkInfo).runWith(Sink.head), 10.seconds)

  client.mesosSource
    .runWith(Sink.foreach { event =>
      if (event.getType == Event.Type.SUBSCRIBED) {
        logger.info("Successfully subscribed to mesos")
      } else if (event.getType == Event.Type.OFFERS) {

        val offerIds = event.getOffers.getOffersList.asScala.map(_.getId).toList

        Source(offerIds)
          .via(log(s"Declining offer with id = ")) // Decline all offers
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
