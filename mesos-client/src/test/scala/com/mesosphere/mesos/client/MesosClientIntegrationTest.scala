package com.mesosphere.mesos.client

import java.util.UUID

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.mesosphere.mesos.conf.MesosClientSettings
import com.mesosphere.utils.AkkaUnitTest
import com.mesosphere.utils.mesos.MesosClusterTest
import com.typesafe.config.ConfigFactory
import org.apache.mesos.v1.Protos.{Filters, FrameworkID, FrameworkInfo}
import org.apache.mesos.v1.scheduler.Protos.Event

import scala.annotation.tailrec
import scala.concurrent.Future

class MesosClientIntegrationTest extends AkkaUnitTest with MesosClusterTest {

  "Mesos client should successfully subscribe to mesos without framework Id" in withFixture() { f =>
    Then("a framework successfully subscribes without a framework Id")
    f.client.frameworkId.getValue shouldNot be(empty)

    And("connection context should be initialized")
    f.client.connectionInfo.url.getHost shouldBe f.mesosHost
    f.client.connectionInfo.url.getPort shouldBe f.mesosPort
    f.client.connectionInfo.streamId.length should be > 1
    f.client.frameworkId.getValue.length should be > 1
  }

  "Mesos client should successfully subscribe to mesos with framework Id" in {
    val frameworkID = FrameworkID.newBuilder.setValue(UUID.randomUUID().toString)

    When("a framework subscribes with a framework Id")
    withFixture(Some(frameworkID)) { f =>
      Then("the client should identify as the specified frameworkId")
      f.client.frameworkId.getValue shouldBe frameworkID.getValue
    }
  }

  "Mesos client should successfully receive heartbeat" in withFixture() { f =>
    When("a framework subscribes")
    val heartbeat = f.pullUntil(_.getType == Event.Type.HEARTBEAT)

    Then("a heartbeat event should arrive")
    heartbeat shouldNot be(empty)
  }

  "Mesos client should successfully receive offers" in withFixture() { f =>
    When("a framework subscribes")
    val offer = f.pullUntil(_.getType == Event.Type.OFFERS)

    And("an offer should arrive")
    offer shouldNot be(empty)
  }

  "Mesos client should successfully declines offers" in withFixture() { f =>
    When("a framework subscribes")
    And("an offer event is received")
    val Some(offer) = f.pullUntil(_.getType == Event.Type.OFFERS)
    val offerId = offer.getOffers.getOffers(0).getId

    And("and an offer is declined")
    val decline = f.client.calls
      .newDecline(offerIds = Seq(offerId), filters = Some(Filters.newBuilder.setRefuseSeconds(0.0).build()))

    Source.single(decline).runWith(f.client.mesosSink)

    Then("eventually a new offer event arrives")
    val nextOffer = f.pullUntil(_.getType == Event.Type.OFFERS)
    nextOffer shouldNot be(empty)
  }

  "Mesos client publisher sink and event source are both stopped with the kill switch" in withFixture() { f =>
    val sinkDone = Source.fromFuture(Future.never).runWith(f.client.mesosSink)

    f.client.killSwitch.shutdown()
    sinkDone.futureValue.shouldBe(Done)
    eventually {
      f.queue.pull().futureValue shouldBe None
    }
  }

  def withFixture(frameworkId: Option[FrameworkID.Builder] = None)(fn: Fixture => Unit): Unit = {
    val f = new Fixture(frameworkId)
    try fn(f)
    finally {
      f.client.killSwitch.shutdown()
    }
  }

  class Fixture(existingFrameworkId: Option[FrameworkID.Builder] = None) {
    implicit val system: ActorSystem = ActorSystem()
    implicit val materializer: ActorMaterializer = ActorMaterializer()

    val frameworkInfo = FrameworkInfo
      .newBuilder()
      .setUser("test")
      .setName("Mesos Client Integration Tests")
      .setId(existingFrameworkId.getOrElse(FrameworkID.newBuilder.setValue(UUID.randomUUID().toString)))
      .addRoles("test")
      .setFailoverTimeout(0.0f)
      .addCapabilities(FrameworkInfo.Capability.newBuilder().setType(FrameworkInfo.Capability.Type.MULTI_ROLE))
      .build()

    val mesosUrl = new java.net.URI(mesosFacade.url)
    val mesosHost = mesosUrl.getHost
    val mesosPort = mesosUrl.getPort

    val config = ConfigFactory.parseString(s"""
         |mesos-client.master-url="${mesosUrl.getHost}:${mesosUrl.getPort}"
    """.stripMargin).withFallback(ConfigFactory.load())

    val settings = MesosClientSettings.fromConfig(config.getConfig("mesos-client"))

    val client = MesosClient(settings, frameworkInfo).runWith(Sink.head).futureValue

    val queue = client.mesosSource.runWith(Sink.queue())

    /**
      * Pull (and drop) elements from the queue until the predicate returns true. Does not cancel the upstream.
      *
      * Returns Some(element) when queue emits an event which matches the predicate
      * Returns None if queue ends (client closes) before the predicate matches
      * TimeoutException is thrown if no event is available within the `patienceConfig.timeout` duration.
      *
      * @param predicate Function to evaluate to see if event matches
      * @return matching event, if any
      */
    @tailrec final def pullUntil(predicate: Event => Boolean): Option[Event] =
      queue.pull().futureValue match {
        case e @ Some(event) if (predicate(event)) =>
          e
        case None =>
          None
        case _ =>
          pullUntil(predicate)
      }
  }
}
