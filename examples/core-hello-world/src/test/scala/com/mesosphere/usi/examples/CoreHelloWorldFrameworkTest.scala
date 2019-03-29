package com.mesosphere.usi.examples

import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.mesosphere.usi.core.models.PodStatusUpdated
import com.mesosphere.usi.core.models.SpecsSnapshot
import com.mesosphere.usi.repository.InMemoryPodRecordRepository
import com.mesosphere.utils.AkkaUnitTest
import com.mesosphere.utils.mesos.MesosClusterTest
import com.mesosphere.utils.mesos.MesosFacade.ITFramework
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import java.util
import java.util.concurrent.atomic.AtomicInteger
import org.apache.mesos.v1.Protos.FrameworkID
import org.apache.mesos.v1.Protos.TaskState.TASK_RUNNING
import org.scalatest.Inside

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

    When("an scheduler graph is generated")
    val conf = loadConfig
    val frameworkInfo = CoreHelloWorldFramework.buildFrameworkInfo
    val customPersistenceStore = InMemoryPodRecordRepository()
    customPersistenceStore.readAll().futureValue.size shouldBe 0
    val (mesosClient, scheduler) = CoreHelloWorldFramework.buildGraph(conf, customPersistenceStore, frameworkInfo)

    And("an initial pod spec is successfully launched")
    val specsSnapshot = CoreHelloWorldFramework.generateSpecSnapshot
    val activeTasks = new AtomicInteger(0)
    runAndGetTaskCount(scheduler, specsSnapshot, activeTasks)

    And("the persistence storage has non-zero records")
    eventually {
      activeTasks.get() shouldBe 1
      customPersistenceStore.readAll().futureValue.size shouldBe 1
    }

    And("if the scheduler is crashed")
    mesosClient.killSwitch.abort(new RuntimeException)

    Then("upon recovery, the same task should not be relaunched")
    val (newClient, newScheduler) = CoreHelloWorldFramework.buildGraph(conf, customPersistenceStore, frameworkInfo)
    activeTasks.set(0)
    runAndGetTaskCount(newScheduler, specsSnapshot, activeTasks)
    intercept[Exception](eventually {
      activeTasks.get() should be > 0
    })
    newClient.killSwitch.shutdown()
  }

  private val runAndGetTaskCount =
    (graph: CoreHelloWorldFramework.SchedulerFlow, specs: SpecsSnapshot, taskCounter: AtomicInteger) => {
      Source.maybe
        .prepend(Source.single(specs))
        .map(snapshot => (snapshot, Source.empty))
        .via(graph)
        .flatMapConcat { case (snapshot, updates) => updates.prepend(Source.single(snapshot)) }
        .map {
          case PodStatusUpdated(_, status) if status.exists(_.taskStatuses.exists(_._2.getState == TASK_RUNNING)) => 1
          case event => {
            logger.info(s"--------> $event")
            0
          }
        }
        .runWith(Sink.foreach[Int](taskCounter.addAndGet))
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
