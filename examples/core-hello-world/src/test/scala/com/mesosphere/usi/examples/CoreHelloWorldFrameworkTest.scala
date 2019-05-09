package com.mesosphere.usi.examples

import java.util

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.testkit.{TestPublisher, TestSubscriber}
import com.mesosphere.usi.core.Scheduler.StateOutput
import com.mesosphere.usi.core.models.{PodStatusUpdated, SchedulerCommand, StateEvent, StateSnapshot}
import com.mesosphere.utils.AkkaUnitTest
import com.mesosphere.utils.mesos.MesosClusterTest
import com.mesosphere.utils.mesos.MesosFacade.ITFramework
import com.mesosphere.utils.persistence.InMemoryPodRecordRepository
import com.typesafe.config.{Config, ConfigFactory}
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
    val (mesosClient, scheduler) = CoreHelloWorldFramework.init(conf, customPersistenceStore, frameworkInfo)

    And("an initial pod spec is launched")
    val launchCommand = CoreHelloWorldFramework.generateLaunchCommand
    val (pub, sub) = testScheduler(scheduler)
    pub.sendNext(launchCommand)

    Then("receive an empty snapshot followed by other state events")
    inside(sub.requestNext()) { case snap: StateSnapshot => snap.podRecords shouldBe empty }
    eventually {
      inside(sub.requestNext()) { case podStatus: PodStatusUpdated => podStatus.newStatus shouldBe defined }
    }

    And("persistence storage has correct set of records")
    val podRecords = customPersistenceStore.readAll().futureValue.values
    podRecords.size shouldBe 1
    podRecords.head.podId shouldEqual launchCommand.podId

    When("the scheduler is crashed")
    mesosClient.killSwitch.abort(new RuntimeException("an intentional crash"))

    Then("upon recovery, emitted snapshot should have valid information")
    val (newClient, newScheduler) = CoreHelloWorldFramework.init(conf, customPersistenceStore, frameworkInfo)
    val (newPub, newSub) = testScheduler(newScheduler)
    newPub.sendNext(launchCommand)
    inside(newSub.requestNext()) {
      case snap: StateSnapshot => snap.podRecords should contain theSameElementsAs podRecords
    }

    And("no new pod records have been created")
    val newPodRecords = customPersistenceStore.readAll().futureValue.values
    newPodRecords should contain theSameElementsAs podRecords

    And("no further elements should be emitted")
    assertThrows[AssertionError](newSub.expectNext(5.seconds)) // This is just a best effort check.
    newClient.killSwitch.shutdown()
  }

  private def testScheduler(
      graph: Flow[SchedulerCommand, StateOutput, NotUsed]
  ): (TestPublisher.Probe[SchedulerCommand], TestSubscriber.Probe[StateEvent]) = {
    val commandPublisher = TestPublisher.probe[SchedulerCommand]()
    val stateEventSub = TestSubscriber.probe[StateEvent]()
    Source
      .fromPublisher(commandPublisher)
      .via(graph)
      .flatMapConcat { case (snapshot, updates) => updates.prepend(Source.single(snapshot)) }
      .runWith(Sink.fromSubscriber(stateEventSub))
    (commandPublisher, stateEventSub)
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
      .parseMap(util.Collections.singletonMap("mesos-client.master-url", s"${mesosUrl.getHost}:${mesosUrl.getPort}"))
      .withFallback(ConfigFactory.load())
      .getConfig("mesos-client")
  }

  class Fixture(existingFrameworkId: Option[FrameworkID.Builder] = None) {
    val framework: CoreHelloWorldFramework = CoreHelloWorldFramework.run(loadConfig)
  }
}
