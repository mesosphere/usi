package com.mesosphere.usi.examples

import akka.NotUsed
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.TestSubscriber
import com.mesosphere.usi.core.Scheduler.SpecInput
import com.mesosphere.usi.core.Scheduler.StateOutput
import com.mesosphere.usi.core.models.PodRecordUpdated
import com.mesosphere.usi.core.models.PodStatusUpdated
import com.mesosphere.usi.core.models.SpecUpdated
import com.mesosphere.usi.core.models.SpecsSnapshot
import com.mesosphere.usi.core.models.StateEvent
import com.mesosphere.usi.core.models.StateSnapshot
import com.mesosphere.usi.repository.InMemoryPodRecordRepository
import com.mesosphere.utils.AkkaUnitTest
import com.mesosphere.utils.mesos.MesosClusterTest
import com.mesosphere.utils.mesos.MesosFacade.ITFramework
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import java.util
import org.apache.mesos.v1.Protos.FrameworkID
import org.scalatest.Inside
import scala.concurrent.duration._

class CoreHelloWorldFrameworkTest extends AkkaUnitTest with MesosClusterTest with Inside {

  "CoreHelloWorldFramework should successfully connect to Mesos" in withFixture() { f =>
    Then("once example framework is connected, Mesos should return it's framework Id")
    val frameworks: Seq[ITFramework] = mesosFacade.frameworks().value.frameworks

    val exampleFramework: ITFramework = frameworks.head
    exampleFramework.id shouldBe f.framework.frameworkId.getValue

    And("example framework should be active and connected")
    exampleFramework.active shouldBe true
    exampleFramework.connected shouldBe true

    And("eventually hello-world task should be up and running")
    eventually {
      val framework = mesosFacade.frameworks().value.frameworks.head
      val task = framework.tasks.head
      task.name should startWith("hello-world")
      task.state.get shouldBe "TASK_RUNNING"
    }
  }

  "CoreHelloWorldFramework should adhere to at most once launch guarantee during a crash-recovery" in {
    Given("a scheduler graph that uses a persistence layer")
    val conf = loadConfig
    val frameworkInfo = CoreHelloWorldFramework.buildFrameworkInfo
    val customPersistenceStore = InMemoryPodRecordRepository()
    customPersistenceStore.readAll().futureValue.size shouldBe 0
    val (mesosClient, scheduler) = CoreHelloWorldFramework.buildGraph(conf, customPersistenceStore, frameworkInfo)

    And("an initial pod spec is launched")
    val specsSnapshot = CoreHelloWorldFramework.generateSpecSnapshot
    val (_, sub1) = runGraphAndFeedSnapshot(scheduler, specsSnapshot)

    Then("receive an empty snapshot followed by other state events")
    val e1 = sub1.requestNext()
    e1 shouldBe a[StateSnapshot]
    e1.asInstanceOf[StateSnapshot].podRecords shouldBe empty
    val e2 = sub1.requestNext()
    e2 shouldBe a[PodRecordUpdated]
    val e3 = sub1.requestNext()
    e3 shouldBe a[PodStatusUpdated]
    val podStatus = e3.asInstanceOf[PodStatusUpdated].newStatus
    podStatus shouldBe defined

    And("persistence storage has correct set of records")
    val podRecords = customPersistenceStore.readAll().futureValue.values
    podRecords.size shouldBe 1
    podRecords.head.podId shouldEqual specsSnapshot.podSpecs.head.id

    When("the scheduler is crashed")
    mesosClient.killSwitch.abort(new RuntimeException("an intentional crash"))

    Then("upon recovery, emitted snapshot should have valid information")
    val (newClient, newScheduler) = CoreHelloWorldFramework.buildGraph(conf, customPersistenceStore, frameworkInfo)
    val (_, sub2) = runGraphAndFeedSnapshot(newScheduler, specsSnapshot)
    val e4 = sub2.requestNext()
    e4 shouldBe a[StateSnapshot]
    e4.asInstanceOf[StateSnapshot].podRecords should contain theSameElementsAs podRecords

    And("no new pod records have been created")
    val newPodRecords = customPersistenceStore.readAll().futureValue.values
    newPodRecords should contain theSameElementsAs podRecords

    And("no further elements should be emitted")
    assertThrows[AssertionError](sub2.expectNext(5.seconds)) // This is just a best effort check.
    newClient.killSwitch.shutdown()
  }

  private def runGraphAndFeedSnapshot(
      graph: Flow[SpecInput, StateOutput, NotUsed],
      specs: SpecsSnapshot
  ): (TestPublisher.Probe[SpecUpdated], TestSubscriber.Probe[StateEvent]) = {
    val specUpdatePub = TestPublisher.probe[SpecUpdated]()
    val stateEventSub = TestSubscriber.probe[StateEvent]()
    Source.maybe
      .prepend(Source.single(specs))
      .map(snapshot => (snapshot, Source.fromPublisher(specUpdatePub)))
      .via(graph)
      .flatMapConcat { case (snapshot, updates) => updates.prepend(Source.single(snapshot)) }
      .runWith(Sink.fromSubscriber(stateEventSub))
    (specUpdatePub, stateEventSub)
  }

  def withFixture(frameworkId: Option[FrameworkID.Builder] = None)(fn: Fixture => Unit): Unit = {
    val f = new Fixture(frameworkId)
    try fn(f)
    finally {
      f.framework.killSwitch.shutdown()
    }
  }

  def loadConfig: Config = {
    val mesosUrl = new java.net.URI(mesosFacade.url)

    ConfigFactory
      .parseMap(util.Map.of("mesos-client.master-url", s"${mesosUrl.getHost}:${mesosUrl.getPort}"))
      .withFallback(ConfigFactory.load())
      .getConfig("mesos-client")
  }

  class Fixture(existingFrameworkId: Option[FrameworkID.Builder] = None) {
    val framework = CoreHelloWorldFramework.run(loadConfig)
  }
}
