package mesosphere.mesos.examples

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.scalalogging.StrictLogging
import mesosphere.mesos.client.{MesosClient, StrictLoggingFlow}
import mesosphere.mesos.conf.MesosClientConf
import org.apache.mesos.v1.mesos.{Filters, FrameworkInfo}
import org.apache.mesos.v1.scheduler.scheduler.Event
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

  val frameworkInfo = FrameworkInfo(
    user = "foo",
    name = "Example FOO Framework",
    roles = Seq("test"),
    capabilities = Seq(FrameworkInfo.Capability(`type` = Some(FrameworkInfo.Capability.Type.MULTI_ROLE)))
  )

  val conf = new MesosClientConf(master = s"127.0.0.1:5050")
  val client = Await.result(MesosClient(conf, frameworkInfo).runWith(Sink.head), 10.seconds)

  client.mesosSource.runWith(Sink.foreach { event =>

    if (event.`type`.get == Event.Type.SUBSCRIBED) {
      logger.info("Successfully subscribed to mesos")
    } else if (event.`type`.get == Event.Type.OFFERS) {

      val offerIds = event.offers.get.offers.map(_.id).toList

      Source(offerIds)
        .via(log(s"Declining offer with id = ")) // Decline all offers
        .map(oId => client.calls.newDecline(
          offerIds = Seq(oId),
          filters = Some(Filters(Some(5.0f)))
        ))
        .runWith(client.mesosSink)
    }

  }).onComplete{
    case Success(res) =>
      logger.info(s"Stream completed: $res"); system.terminate()
    case Failure(e) => logger.error(s"Error in stream: $e"); system.terminate()
  }
}
