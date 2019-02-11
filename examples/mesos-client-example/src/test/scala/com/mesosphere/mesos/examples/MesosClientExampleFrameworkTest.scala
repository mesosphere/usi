package com.mesosphere.mesos.examples
import com.mesosphere.utils.AkkaUnitTest
import com.mesosphere.utils.mesos.MesosClusterTest
import com.mesosphere.utils.mesos.MesosFacade.ITFramework
import com.typesafe.config.ConfigFactory
import org.apache.mesos.v1.Protos.FrameworkID
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MesosClientExampleFrameworkTest extends AkkaUnitTest with MesosClusterTest {

  "MesosClientExampleFramework should successfully connect to Mesos" in withFixture() { f =>
    Then("once example framework is connected, Mesos should return it's framework Id")
    val frameworks: Seq[ITFramework] = mesosFacade.frameworks().value.frameworks

    val exampleFramework: ITFramework = frameworks.head
    exampleFramework.id shouldBe f.framework.client.frameworkId.getValue

    And("example framework should be active and connected")
    exampleFramework.active shouldBe true
    exampleFramework.connected shouldBe true
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
    val mesosHost = mesosUrl.getHost
    val mesosPort = mesosUrl.getPort

    val conf = ConfigFactory.parseString(s"""
                                              |mesos-client.master-url="${mesosUrl.getHost}:${mesosUrl.getPort}"
                                            """.stripMargin).withFallback(ConfigFactory.load())

    val framework = MesosClientExampleFramework(conf.getConfig("mesos-client"))
  }
}
