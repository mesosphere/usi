package com.mesosphere.usi.examples

import java.util

import com.mesosphere.mesos.conf.MesosClientSettings
import com.mesosphere.utils.AkkaUnitTest
import com.mesosphere.utils.mesos.MesosClusterTest
import com.mesosphere.utils.mesos.MesosFacade.ITFramework
import com.typesafe.config.ConfigFactory
import org.apache.mesos.v1.Protos.FrameworkID
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

  def withFixture(frameworkId: Option[FrameworkID.Builder] = None)(fn: Fixture => Unit): Unit = {
    val f = new Fixture(frameworkId)
    try fn(f)
    finally {
      f.framework.killSwitch.shutdown()
    }
  }

  class Fixture(existingFrameworkId: Option[FrameworkID.Builder] = None) {
    val mesosUrl = mesosFacade.url

    val conf = ConfigFactory
      .parseMap(util.Collections.singletonMap("mesos-client.master-url", s"${mesosUrl.getHost}:${mesosUrl.getPort}"))
      .withFallback(ConfigFactory.load())

    val framework = CoreHelloWorldFramework.run(MesosClientSettings.fromConfig(conf))
  }
}
