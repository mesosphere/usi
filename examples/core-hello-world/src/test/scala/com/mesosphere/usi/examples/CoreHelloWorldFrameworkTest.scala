package com.mesosphere.usi.examples

import java.util

import com.mesosphere.utils.AkkaUnitTest
import com.mesosphere.utils.mesos.MesosClusterTest
import com.mesosphere.utils.mesos.MesosFacade.ITFramework
import com.typesafe.config.ConfigFactory
import org.apache.mesos.v1.Protos.FrameworkID
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CoreHelloWorldFrameworkTest extends AkkaUnitTest with MesosClusterTest {

  "CoreHelloWorldFramework should successfully connect to Mesos" in withFixture() { f =>
    Then("once example framework is connected, Mesos should return it's framework Id")
    val frameworks: Seq[ITFramework] = mesosFacade.frameworks().value.frameworks

    val exampleFramework: ITFramework = frameworks.head
    exampleFramework.id shouldBe f.framework.client.frameworkId.getValue

    And("example framework should be active and connected")
    exampleFramework.active shouldBe true
    exampleFramework.connected shouldBe true

    And("eventually hello-world task should be up and running")
    eventually {
      val framework = mesosFacade.frameworks().value.frameworks.head
      val task = framework.tasks.head
      task.name shouldBe "hello-world" withClue (s"Task name was supposed to be 'hello-world' but was ${task.name}")
      task.state.get shouldBe "TASK_RUNNING" withClue (s"Task state was supposed to be 'TASK_RUNNING' but was ${task.state.get}")
    }
  }

  def withFixture(frameworkId: Option[FrameworkID.Builder] = None)(fn: Fixture => Unit): Unit = {
    val f = new Fixture(frameworkId)
    try fn(f)
    finally {
      f.framework.client.killSwitch.shutdown()
    }
  }

  class Fixture(existingFrameworkId: Option[FrameworkID.Builder] = None) {
    val mesosUrl = new java.net.URI(mesosFacade.url)

    val conf = ConfigFactory
      .parseMap(util.Map.of("mesos-client.master-url", s"${mesosUrl.getHost}:${mesosUrl.getPort}"))
      .withFallback(ConfigFactory.load())

    val framework = CoreHelloWorldFramework(conf.getConfig("mesos-client"))
  }
}
