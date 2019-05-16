package com.mesosphere.mesos.examples

import com.mesosphere.mesos.conf.MesosClientSettings
import com.mesosphere.utils.AkkaUnitTest
import com.mesosphere.utils.mesos.MesosClusterTest
import com.mesosphere.utils.mesos.MesosFacade.ITFramework
import org.apache.mesos.v1.Protos.FrameworkID

class SimpleHelloWorldFrameworkTest extends AkkaUnitTest with MesosClusterTest {

  "MesosClientExampleFramework should successfully connect to Mesos" in withFixture() { f =>
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
      task.name shouldBe "hello-world"
      task.state.get shouldBe "TASK_RUNNING"
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
    val settings = MesosClientSettings.load().withMasters(Seq(mesosFacade.url))
    val framework = SimpleHelloWorldFramework(settings)
  }
}
