package com.mesosphere.usi.examples

import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.mesosphere.usi.core.models.PodStatusUpdated
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
import org.apache.mesos.v1.Protos.TaskState.TASK_RUNNING
import org.scalatest.Inside
import org.scalatest.exceptions.TestFailedDueToTimeoutException
import scala.collection.mutable.ListBuffer

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
    val mutableEventList = ListBuffer[StateEvent]()
    runAndGetTaskCount(scheduler, specsSnapshot, mutableEventList)

    Then("the first ever snapshot is empty and our tasks have been launched")
    eventually {
      mutableEventList.collect {
        case PodStatusUpdated(_, Some(status)) => status.taskStatuses.count(_._2.getState == TASK_RUNNING)
      }.sum shouldBe 1
      val stateSnapshots = mutableEventList.collect { case x: StateSnapshot => x }
      stateSnapshots.size shouldBe 1
      stateSnapshots.head.podRecords.size shouldBe 0
    }

    And("persistence storage has correct set of records")
    val podRecords = customPersistenceStore.readAll().futureValue.values
    podRecords.size shouldBe 1
    podRecords.head.podId shouldEqual specsSnapshot.podSpecs.head.id

    When("the scheduler is crashed")
    mesosClient.killSwitch.abort(new RuntimeException("an intentional crash"))

    Then("upon recovery, emitted snapshot should have valid information")
    val (newClient, newScheduler) = CoreHelloWorldFramework.buildGraph(conf, customPersistenceStore, frameworkInfo)
    mutableEventList.clear()
    runAndGetTaskCount(newScheduler, specsSnapshot, mutableEventList)
    eventually {
      val stateSnapshots = mutableEventList.collect { case x: StateSnapshot => x }
      stateSnapshots.size shouldBe 1
      stateSnapshots.head.podRecords.size shouldBe 1
      stateSnapshots.head.podRecords.head shouldEqual podRecords.head
    }

    And("no new pod records have been created")
    val newPodRecords = customPersistenceStore.readAll().futureValue.values
    newPodRecords.size shouldBe 1
    newPodRecords.head shouldEqual podRecords.head

    And(s"a ${PodStatusUpdated.getClass.getSimpleName} should never be emitted")
    intercept[TestFailedDueToTimeoutException](eventually {
      mutableEventList.count(_.isInstanceOf[PodStatusUpdated]) should be > 0
    })
    newClient.killSwitch.shutdown()
  }

  private val runAndGetTaskCount =
    (graph: CoreHelloWorldFramework.SchedulerFlow, specs: SpecsSnapshot, eventCapturer: ListBuffer[StateEvent]) => {
      Source.maybe
        .prepend(Source.single(specs))
        .map(snapshot => (snapshot, Source.empty))
        .via(graph)
        .flatMapConcat { case (snapshot, updates) => updates.prepend(Source.single(snapshot)) }
        .runWith(Sink.foreach[StateEvent](eventCapturer.append(_)))
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
