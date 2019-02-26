package com.mesosphere.usi.repository

import java.time.Instant

import com.mesosphere.usi.core.models.{AgentId, PodId, PodRecord}
import com.mesosphere.utils.UnitTest

/**
  * The [[RepositoryBehavior]] defines the behavior each implementation of [[PodRecordRepository]] and [[ReservationRecordRepository]]
  * should follow. See the unit tests of the Zookeeper persistence package for an example.
  */
trait RepositoryBehavior { this: UnitTest =>

  def podRecordRepository(newRepo: => PodRecordRepository): Unit = {

    "create a record" in new Fixture() { f =>
      val repo = newRepo
      repo.create(f.record).futureValue should be(f.record.podId)
    }

    "no create a record a second time" in new Fixture() { f =>
      Given("a record already exists")
      val repo = newRepo
      repo.create(f.record).futureValue should be(f.record.podId)

      When("the record is created again")
      val result = repo.create(f.record).failed.futureValue

      Then("the creation should fail")
      result should be(RecordAlreadyExistsException(f.podId.value))
    }

    "read a record" in new Fixture() { f =>
      Given(s"a known record id ${f.podId}")
      val repo = newRepo
      repo.create(f.record).futureValue

      When("the record is read by id")
      val maybeRecord = repo.read(f.podId).futureValue

      Then("the record is returned")
      maybeRecord.value should be(f.record)
    }

    "read an unknown record" in {
      Given(s"an unknown record id")
      val repo = newRepo
      val podId = PodId("unknown")

      When("the record is read by id")
      val maybeRecord = repo.read(podId).futureValue

      Then("the record is None")
      maybeRecord should be(None)
    }
  }

  class Fixture() {
    val podId = PodId("my_pod")
    val agentId = AgentId("my_agent")
    val record = PodRecord(podId, Instant.now(), agentId)
  }
}
