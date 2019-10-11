package com.mesosphere.usi.examples

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.testkit.{TestPublisher, TestSubscriber}
import com.mesosphere.mesos.conf.MesosClientSettings
import com.mesosphere.usi.core.models.commands.SchedulerCommand
import com.mesosphere.usi.core.models.{PodStatusUpdatedEvent, StateEvent}
import com.mesosphere.utils.AkkaUnitTest
import com.mesosphere.utils.mesos.MesosClusterTest
import com.mesosphere.utils.mesos.MesosFacade.ITFramework
import com.mesosphere.utils.persistence.InMemoryPodRecordRepository
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
      task.id should startWith("hello-world")
      task.state.get shouldBe "TASK_RUNNING"
    }
  }

  "CoreHelloWorldFramework should adhere to at most once launch guarantee during a crash-recovery" in {
    Given("a scheduler graph that uses a persistence layer")
    val frameworkInfo = CoreHelloWorldFramework.buildFrameworkInfo
    val customPersistenceStore = InMemoryPodRecordRepository()
    customPersistenceStore.readAll().futureValue.size shouldBe 0
    val (mesosClient, _, scheduler) = CoreHelloWorldFramework.init(
      MesosClientSettings.load().withMasters(Seq(mesosFacade.url)),
      customPersistenceStore,
      frameworkInfo)

    And("an initial pod spec is launched")
    val launchCommand = CoreHelloWorldFramework.generateLaunchCommand
    val (pub, sub) = testScheduler(scheduler)
    pub.sendNext(launchCommand)

    Then("receive an empty snapshot followed by other state events")
    eventually {
      inside(sub.requestNext()) { case podStatus: PodStatusUpdatedEvent => podStatus.newStatus shouldBe defined }
    }

    And("persistence storage has correct set of records")
    val podRecords = customPersistenceStore.readAll().futureValue.values
    podRecords.size shouldBe 1
    podRecords.head.podId shouldEqual launchCommand.podId

    When("the scheduler is crashed")
    mesosClient.killSwitch.abort(new RuntimeException("an intentional crash"))

    Then("upon recovery, emitted snapshot should have valid information")
    val (newClient, _, newScheduler) = CoreHelloWorldFramework.init(
      MesosClientSettings.load().withMasters(Seq(mesosFacade.url)),
      customPersistenceStore,
      frameworkInfo)
    val (newPub, newSub) = testScheduler(newScheduler)
    newPub.sendNext(launchCommand)

    And("no new pod records have been created")
    val newPodRecords = customPersistenceStore.readAll().futureValue.values
    newPodRecords should contain theSameElementsAs podRecords

    And("no further elements should be emitted")
    assertThrows[AssertionError](newSub.expectNext(5.seconds)) // This is just a best effort check.
    newClient.killSwitch.shutdown()
  }

  private def testScheduler(
      graph: Flow[SchedulerCommand, StateEvent, NotUsed]
  ): (TestPublisher.Probe[SchedulerCommand], TestSubscriber.Probe[StateEvent]) = {
    val commandPublisher = TestPublisher.probe[SchedulerCommand]()
    val stateEventSub = TestSubscriber.probe[StateEvent]()
    Source
      .fromPublisher(commandPublisher)
      .via(graph)
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

  class Fixture(existingFrameworkId: Option[FrameworkID.Builder] = None) {
    val framework: CoreHelloWorldFramework =
      CoreHelloWorldFramework.run(MesosClientSettings.load().withMasters(Seq(mesosFacade.url)))
  }
}
