package com.mesosphere.mesos.examples

import com.mesosphere.mesos.conf.MesosClientSettings
import com.mesosphere.utils.AkkaUnitTest
import com.mesosphere.utils.mesos.MesosClusterTest
import com.mesosphere.utils.mesos.MesosFacade.ITFramework
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.mesos.v1.Protos.FrameworkID

class MesosClientExampleFrameworkTest extends AkkaUnitTest with MesosClusterTest {

  override lazy val akkaConfig: Config = ConfigFactory.parseString(s"""
    |akka.test.default-timeout=${patienceConfig.timeout.millisPart}
    |akka.ssl-config.trustManager.stores = [ { path: $${java.home}/lib/security/cacerts } ]
    """.stripMargin).withFallback(ConfigFactory.load()).resolve()

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
    val settings = MesosClientSettings.load().withMasters(Seq(mesosFacade.url))
    val framework = MesosClientExampleFramework(settings)
  }
}
