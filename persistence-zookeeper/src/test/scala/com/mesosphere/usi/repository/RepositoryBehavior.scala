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
    * This defines the expected behavior of creating/saving pods in a repository.
    *
    * @param newRepo A repo factory function. Each test case creates its own repository.
    */
  def podRecordCreate(newRepo: () => PodRecordRepository): Unit = {

    "create a record" in {
      val f = Fixture()
      val repo = newRepo()
      repo.create(f.record).futureValue should be(f.record.podId)
    }

    "no create a record a second time" in {
      val f = Fixture()
      val podId = PodId("second_pod")
      val record = f.record.copy(podId = podId)

      Given("a record already exists")
      val repo = newRepo()
      repo.create(record).futureValue should be(podId)

      When("the record is created again")
      val result = repo.create(record).failed.futureValue

      Then("the creation should fail")
      result should be(RecordAlreadyExistsException(podId.value))
    }
  }

  /**
    * This defines the expected behavior of reading pods from a repository.
    *
    * @param newRepo A repo factory function. Each test case creates its own repository.
    */
  def podRecordRead(newRepo: () => PodRecordRepository): Unit = {

    "read a record" in {
      val f = Fixture()

      Given(s"a known record id ${f.podId}")
      val repo = newRepo()
      repo.create(f.record).futureValue

      When("the record is read by id")
      val maybeRecord = repo.read(f.podId).futureValue

      Then("the record is returned")
      maybeRecord.value should be(f.record)
    }

    "read an unknown record" in {
      Given(s"an unknown record id")
      val repo = newRepo()
      val podId = PodId("unknown")

      When("the record is read by id")
      val maybeRecord = repo.read(podId).futureValue

      Then("the record is None")
      maybeRecord should be(None)
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
      repo.create(f.record).futureValue

      When("the record is deleted")
      val podId = repo.delete(f.podId).futureValue

      Then("the record id should be returned")
      podId should be(f.podId)

      And("the record should not exist")
      repo.read(f.podId).futureValue should be(None)
    }

    "delete an unknown record" in {
      Given(s"an unknown record id")
      val repo = newRepo()
      val unknownPodId = PodId("unknown")

      When("the unknown record is deleted")
      val result = repo.delete(unknownPodId)

      Then("no error is returned")
      result.futureValue

      And("the record should not exist")
      repo.read(unknownPodId).futureValue should be(None)
    }
  }

  /**
    * This defines the expected behavior of updating pods from a repository.
    *
    * @param newRepo A repo factory function. Each test case creates its own repository.
    */
  def podRecordUpdate(newRepo: () => PodRecordRepository): Unit = {

    "update a record" in {
      val f = Fixture()

      Given(s"a known record id ${f.podId}")
      val repo = newRepo()
      repo.create(f.record).futureValue

      And("and updated record")
      val newAgentId = AgentId("new_agent")
      val updatedRecord = f.record.copy(agentId = newAgentId)

      When("the record is updated")
      repo.update(updatedRecord).futureValue

      Then("the updated record should be saved")
      repo.read(f.podId).futureValue should be(Some(updatedRecord))

      And("the old record should be gone")
      repo.read(f.podId).futureValue should not be (Some(f.record))
    }

    "update an unknown record" in {
      Given(s"an unknown record id")
      val repo = newRepo()
      val unknownPodId = PodId("unknown")

      When("the unknown record is updated")
      val unknownRecord = PodRecord(unknownPodId, Instant.now(), AgentId("my_agent"))
      val result = repo.update(unknownRecord).failed.futureValue

      Then("an error is returned")
      result should be(RecordNotFoundException(unknownPodId.value))
    }
  }

  case class Fixture() {
    val podId = PodId(s"pod_${podCount.getAndIncrement()}")
    val agentId = AgentId("my_agent")
    val record = PodRecord(podId, Instant.now(), agentId)
  }
}
