package com.mesosphere.usi.repository

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import com.mesosphere.usi.core.models.{AgentId, PodId, PodRecord}
import com.mesosphere.utils.UnitTest

/**
  * The [[RepositoryBehavior]] defines the behavior each implementation of [[PodRecordRepository]] and [[ReservationRecordRepository]]
  * should follow. See the unit tests of the Zookeeper persistence package for an example.
  */
trait RepositoryBehavior { this: UnitTest =>

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
      repo.store(f.record).futureValue should be(f.record.podId)
    }

    "update a record" in {
      val f = Fixture()
      val podId = PodId("second_pod")
      val record = f.record.copy(podId = podId)

      Given("a record already exists")
      val repo = newRepo()
      repo.store(record).futureValue should be(podId)

      Then("the record is updated")
      repo.store(record).futureValue should be(podId)
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
      repo.store(f.record).futureValue

      When("all records are read")
      val maybeRecord = repo.readAll().futureValue

      Then("the stored record is returned")
      maybeRecord.head should be(f.record.podId -> f.record)
    }

    "read all records should return all the records" in {
      val fixtures = Seq(Fixture(), Fixture(), Fixture(), Fixture(), Fixture())

      Given(s"few known record ids ${fixtures.map(_.podId)}")
      val repo = newRepo()
      fixtures.map(_.record).map(repo.store).map(_.futureValue)

      When("all records are read")
      val records = repo.readAll().futureValue

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
      repo.store(f.record).futureValue

      When("the record is deleted")
      repo.delete(f.podId).futureValue

      Then("the record should not exist")
      repo.readAll().futureValue should not contain f.podId -> f.record
    }

    "delete is idempotent" in {
      Given(s"an unknown record id")
      val repo = newRepo()
      val unknownPodId = PodId("unknown")

      When("the unknown record is deleted")
      val result = repo.delete(unknownPodId)

      Then("no error is returned")
      result.futureValue

      And("the record should not exist")
      repo.readAll().futureValue shouldBe empty
    }
  }

  case class Fixture() {
    val podId = PodId(s"pod_${podCount.getAndIncrement()}")
    val agentId = AgentId("my_agent")
    val record = PodRecord(podId, Instant.now(), agentId)
  }
}
