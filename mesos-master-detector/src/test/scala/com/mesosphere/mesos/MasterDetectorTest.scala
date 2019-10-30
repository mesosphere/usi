package com.mesosphere.mesos

import com.mesosphere.utils.AkkaUnitTest
import com.mesosphere.utils.mesos.{MesosClusterTest, MesosConfig}
import com.mesosphere.utils.metrics.DummyMetrics

import scala.compat.java8.FutureConverters._

class MasterDetectorTest extends AkkaUnitTest with MesosClusterTest {

  override lazy val mesosConfig = MesosConfig(numMasters = 2)
  lazy val metrics = DummyMetrics

  "The master detector factory" should {
    "detect Zookeeper" in {
      MasterDetector("zk://host1:port1,host2:port2/path/to/master", metrics) shouldBe a[Zookeeper]
    }

    "detect Standalone" in {
      MasterDetector("http://host1:5050", metrics) shouldBe a[Standalone]
      MasterDetector("https://host1:5050", metrics) shouldBe a[Standalone]
      MasterDetector("host1:5050", metrics) shouldBe a[Standalone]
    }
  }

  "The Zookeeper Detector" should {
    "extract the path and servers" in {
      Given("the master string zk://host1:port1,host2:port2/path/to/master")
      val master = "zk://host1:port1,host2:port2/path/to/master"

      When("we parse it")
      val result = Zookeeper(master, metrics).parse()

      Then("the servers and path were extracted")
      result.auth should be('empty)
      result.servers should be("host1:port1,host2:port2")
      result.path should be("/path/to/master")
    }

    "extract authentication" in {
      Given("the master string zk://user:password@host1:port1,host2:port2/path/to/master")
      val master = "zk://user:password@host1:port1,host2:port2/path/to/master"

      When("we parse it")
      val result = Zookeeper(master, metrics).parse()

      Then("the servers and path were extracted")
      result.auth.value should be("user:password")
    }

    "default the path to /" in {
      Given("the master string zk://user:password@host1:port1,host2:port2")
      val master = "zk://user:password@host1:port1,host2:port2"

      When("we parse it")
      val result = Zookeeper(master, metrics).parse()

      Then("the path should default to /")
      result.path should be("/")
    }

    "validates master strings" in {
      MasterDetector("zk://host1:port1,host2:port2/path/to/master", metrics).isValid() should be(true)
      MasterDetector("zk://user:pass@me@host1:port1,host2:port2/path/to/master", metrics).isValid() should be(false)
      MasterDetector("host:port1", metrics).isValid() should be(false)
      MasterDetector("host:5050", metrics).isValid() should be(true)
    }
  }

  "The master detector" should {
    "detect the Mesos master" in {
      Given(s"the Zookeeper server at $mesosMasterZkUrl and a detector")
      val detector = MasterDetector(mesosMasterZkUrl, metrics)
      val expectedLeader = mesosCluster.waitForLeader().toString

      When("we detect the Mesos master")
      val masterUrl = detector.getMaster().toScala.futureValue

      Then("the URL is the same as our master")
      masterUrl.toString should be(expectedLeader)
    }
  }
}
