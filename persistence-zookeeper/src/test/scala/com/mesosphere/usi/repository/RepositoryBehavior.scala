package com.mesosphere.usi.repository

import akka.NotUsed
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.testkit.scaladsl.TestSource
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import com.mesosphere.usi.core.models.{AgentId, PodId, PodRecord}
import com.mesosphere.utils.AkkaUnitTestLike
import com.mesosphere.utils.UnitTest

/**
  * The [[RepositoryBehavior]] defines the behavior each implementation of [[PodRecordRepository]] and [[ReservationRecordRepository]]
  * should follow. See the unit tests of the Zookeeper persistence package for an example.
  */
trait RepositoryBehavior extends AkkaUnitTestLike { this: UnitTest =>

  val podCount = new AtomicInteger()

  /**
    * This defines the expected behavior of creating/updating pods in a repository.
    *
    * @param newRepo A repo factory function. Each test case creates its own repository.
    */
  def podRecordStore(newRepo: () => PodRecordRepository): Unit = {

    "create a record" in {
      val f = Fixture()
      val repo = newRepo()
      val (pub, sub) = pubAndSub(repo.storeFlow)
      pub.sendNext(f.record)
      sub.requestNext(f.record.podId)
    }

    "update a record" in {
      val f = Fixture()
      val podId = PodId("second_pod")
      val record = f.record.copy(podId = podId)

      Given("a record already exists")
      val repo = newRepo()
      val (pub, sub) = pubAndSub(repo.storeFlow)
      pub.sendNext(record)
      sub.requestNext(podId)

      Then("the record is updated")
      pub.sendNext(record)
      sub.requestNext(podId)
    }
  }

  /**
    * This defines the expected behavior of reading pods from a repository.
    *
    * @param newRepo A repo factory function. Each test case creates its own repository.
    */
  def podRecordReadAll(newRepo: () => PodRecordRepository): Unit = {

    "read all records" in {
      val f = Fixture()

      Given(s"a known record id ${f.podId}")
      val repo = newRepo()
      val (pub, sub) = pubAndSub(repo.storeFlow)
      pub.sendNext(f.record)
      sub.requestNext(f.record.podId)

      When("all records are read")
      val maybeRecord = repo.readAll().runWith(Sink.head).futureValue

      Then("the stored record is returned")
      maybeRecord.head should be(f.record.podId -> f.record)
    }

    "read all records should return all the records" in {
      val fixtures = Seq(Fixture(), Fixture(), Fixture(), Fixture(), Fixture())

      Given(s"few known record ids ${fixtures.map(_.podId)}")
      val repo = newRepo()
      val (pub, sub) = pubAndSub(repo.storeFlow)
      fixtures.map(_.record).map(pub.sendNext)
      sub.request(fixtures.size)
      fixtures.map(_.record.podId).map(sub.expectNext)

      When("all records are read")
      val records = repo.readAll().runWith(Sink.head).futureValue

      Then("all the stored records are returned")
      records.values should contain theSameElementsAs fixtures.map(_.record)
    }
  }

  /**
    * This defines the expected behavior of deleting pods from a repository.
    *
    * @param newRepo A repo factory function. Each test case creates its own repository.
    */
  def podRecordDelete(newRepo: () => PodRecordRepository): Unit = {

    "delete a record" in {
      val f = Fixture()

      Given(s"a known record id ${f.podId}")
      val repo = newRepo()
      val (pub, sub) = pubAndSub(repo.storeFlow)
      pub.sendNext(f.record)
      sub.requestNext(f.record.podId)

      When("the record is deleted")
      val (delPub, delSub) = pubAndSub(repo.deleteFlow)
      delPub.sendNext(f.record.podId)
      delSub.requestNext(())

      Then("the record should not exist")
      repo.readAll().runWith(Sink.head).futureValue should not contain f.podId -> f.record
    }

    "delete is idempotent" in {
      Given(s"an unknown record id")
      val repo = newRepo()
      val unknownPodId = PodId("unknown")

      When("the unknown record is deleted")
      val (pub, sub) = pubAndSub(repo.deleteFlow)
      pub.sendNext(unknownPodId)

      Then("no error is returned")
      sub.requestNext(())

      And("the record should not exist")
      repo.readAll().runWith(Sink.head).futureValue shouldBe empty
    }
  }

  def pubAndSub[In, Out](flow: Flow[In, Out, NotUsed]): (TestPublisher.Probe[In], TestSubscriber.Probe[Out]) =
    TestSource.probe[In].via(flow).toMat(TestSink.probe[Out])(Keep.both).run()

  case class Fixture() {
    val podId = PodId(s"pod_${podCount.getAndIncrement()}")
    val agentId = AgentId("my_agent")
    val record = PodRecord(podId, Instant.now(), agentId)
  }
}
