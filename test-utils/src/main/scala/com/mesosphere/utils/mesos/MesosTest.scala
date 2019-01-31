package com.mesosphere.utils.mesos

import java.io.File
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}

import akka.actor.{ActorSystem, Scheduler}
import akka.stream.Materializer
import com.mesosphere.utils.zookeeper.ZookeeperServerTest
import com.mesosphere.utils.{PortAllocator, ProcessOutputToLogStream}
import org.apache.commons.io.FileUtils
import org.scalatest.Suite
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.sys.process.{Process, ProcessBuilder}
import scala.util.Try

/**
  * Mesos gent configuration parameters. For more information see
  * [Mesos documentation](http://mesos.apache.org/documentation/latest/configuration/agent/)
  *
  * @param launcher The launcher to be used for Mesos containerizer. It could either be linux or posix
  * @param containerizers Comma-separated list of containerizer implementations to compose in order to provide containerization.
  *                       Available options are mesos and docker (on Linux).
  * @param isolation isolation mechanisms to use, e.g., posix/cpu,posix/mem
  * @param imageProviders Comma-separated list of supported image providers, e.g., APPC,DOCKER.
  */
case class MesosAgentConfig(launcher: String = "posix",
                            containerizers: String = "mesos",
                            isolation: Option[String] = None,
                            imageProviders: Option[String] = None)

/**
  * Basic class that wraps starting/stopping a Mesos cluster with passed agent/master configuration and configurable
  * number of masters/agents.
  *
  * @param suiteName suite name that uses the cluster. will be included into logging messages
  * @param numMasters number of Mesos master
  * @param numSlaves number of Mesos slaves
  * @param masterUrl Mesos master ZK url in the form of zk://zk-1/mesos
  * @param quorumSize Mesos master quorum size
  * @param autoStart if true, the cluster will be started automatically
  * @param agentsConfig Mesos agent configuration
  * @param waitForMesosTimeout time to wait for Mesos master/agent to start
  * @param mastersFaultDomains Mesos master fault domain configuration
  * @param agentsFaultDomains Mesos agents fault domain configuration
  * @param agentsGpus number of GPUs per agent
  */
case class MesosCluster(suiteName: String,
                        numMasters: Int,
                        numSlaves: Int,
                        masterUrl: String,
                        quorumSize: Int = 1,
                        autoStart: Boolean = false,
                        agentsConfig: MesosAgentConfig = MesosAgentConfig(),
                        waitForMesosTimeout: FiniteDuration = 5.minutes,
                        mastersFaultDomains: Seq[Option[FaultDomain]],
                        agentsFaultDomains: Seq[Option[FaultDomain]],
                        agentsGpus: Option[Int] = None)(implicit system: ActorSystem, mat: Materializer) extends AutoCloseable with Eventually {
  require(quorumSize > 0 && quorumSize <= numMasters)
  require(agentsFaultDomains.isEmpty || agentsFaultDomains.size == numSlaves)

  lazy val masters: Seq[Master] = 0.until(numMasters).map { i =>
    val faultDomainJson = if (mastersFaultDomains.nonEmpty && mastersFaultDomains(i).nonEmpty) {
      val faultDomain = mastersFaultDomains(i).get
      val faultDomainJson = s"""
                               |{
                               |  "fault_domain":
                               |    {
                               |      "region":
                               |        {
                               |          "name": "${faultDomain.region}"
                               |        },
                               |      "zone":
                               |        {
                               |          "name": "${faultDomain.zone}"
                               |        }
                               |    }
                               |}
        """.stripMargin
      Some(faultDomainJson)
    } else None

    Master(extraArgs = Seq(
      "--slave_ping_timeout=1secs",
      "--max_slave_ping_timeouts=4",
      s"--quorum=$quorumSize") ++ faultDomainJson.map(fd => s"--domain=$fd"))
  }

  lazy val agents: Seq[Agent] = 0.until(numSlaves).map { i =>
    val (faultDomainAgentAttributes: Map[String, Option[String]], mesosFaultDomainAgentCmdOption) = if (agentsFaultDomains.nonEmpty && agentsFaultDomains(i).nonEmpty) {
      val faultDomain = agentsFaultDomains(i).get
      val mesosFaultDomainCmdOption = s"""
                                         |{
                                         |  "fault_domain":
                                         |    {
                                         |      "region":
                                         |        {
                                         |          "name": "${faultDomain.region}"
                                         |        },
                                         |      "zone":
                                         |        {
                                         |          "name": "${faultDomain.zone}"
                                         |        }
                                         |    }
                                         |}
        """.stripMargin

      val nodeAttributes = Map(
        "fault_domain_region" -> Some(faultDomain.region.value),
        "fault_domain_zone" -> Some(faultDomain.zone.value))
      (nodeAttributes, Some(mesosFaultDomainCmdOption))
    } else Map.empty -> None

    // Uniquely identify each agent node, useful for constraint matching
    val attributes: Map[String, Option[String]] = Map("node" -> Some(i.toString)) ++ faultDomainAgentAttributes

    val renderedAttributes: String = attributes.map { case (key, maybeVal) => s"$key${maybeVal.map(v => s":$v").getOrElse("")}" }.mkString(";")

    // We can add additional resources constraints for our test clusters here.
    // IMPORTANT: we give each cluster's agent it's own port range! Otherwise every Mesos will offer the same port range
    // to it's frameworks, leading to multiple tasks (from different test suits) trying to use the same port!
    // First-come-first-served task will bind successfully where the others will fail leading to a lot inconsistency and
    // flakiness in tests.
    Agent(resources = new Resources(ports = PortAllocator.portsRange(), gpus = agentsGpus), extraArgs = Seq(
      s"--attributes=$renderedAttributes"
    ) ++ mesosFaultDomainAgentCmdOption.map(fd => s"--domain=$fd"))
  }

  if (autoStart) {
    start()
  }

  def start(): String = {
    masters.foreach(_.start())
    agents.foreach(_.start())
    val masterUrl = waitForLeader()
    waitForAgents(masterUrl)
    masterUrl
  }

  def waitForLeader(): String = {
    val firstMaster = s"http://${masters.head.ip}:${masters.head.port}"
    val mesosFacade = new MesosFacade(firstMaster)

    val leader = eventually(timeout(waitForMesosTimeout), interval(1.seconds)){
      mesosFacade.redirect().headers.find(_.lowercaseName() == "location").map(_.value()).get
    }
    s"http:$leader"
  }

  def waitForAgents(masterUrl: String): Unit = {
    val mesosFacade = new MesosFacade(masterUrl)
    eventually(timeout(waitForMesosTimeout), interval(1.seconds)){
      mesosFacade.state.value.agents.size == agents.size
    }
  }

  // format: OFF
  case class Resources(cpus: Option[Int] = None, mem: Option[Int] = None, ports: (Int, Int), gpus: Option[Int] = None) {
    // Generates mesos-agent resource string e.g. "cpus:2;mem:124;ports:[10000-110000]"
    def resourceString(): String = {
      s"""
         |${cpus.fold("")(c => s"cpus:$c;")}
         |${mem.fold("")(m => s"mem:$m;")}
         |${gpus.fold("")(g => s"gpus:$g;")}
         |${ports match {case (f, t) => s"ports:[$f-$t]"}}
       """.stripMargin.replaceAll("[\n\r]", "");
    }
  }
  // format: ON

  /**
    * Basic trait representing Mesos Master/Agent. Takes care of starting/stopping actual Mesos process and cleaning
    * up after the execution which included collecting agent sandboxes.
    */
  trait Mesos extends AutoCloseable {
    val extraArgs: Seq[String]
    val ip = IP.routableIPv4
    val port = PortAllocator.ephemeralPort()
    val workDir: File
    val processBuilder: ProcessBuilder
    val processName: String
    private var process: Option[Process] = None

    if (autoStart) {
      start()
    }

    def start(): Unit = if (process.isEmpty) {
      process = Some(processBuilder.run(ProcessOutputToLogStream(s"$suiteName-Mesos$processName-$port")))
    }

    def stop(): Unit = {
      process.foreach(_.destroy())
      process = None
    }

    override def close(): Unit = {
      def copySandboxFiles() = {
        val projectDir = sys.props.getOrElse("user.dir", ".")
        FileUtils.copyDirectory(workDir, Paths.get(projectDir, "sandboxes", suiteName).toFile)
      }
      // Copy all sandbox files (useful for debugging) into current directory for build job to archive it:
      Try(copySandboxFiles())
      stop()
      Try(FileUtils.deleteDirectory(workDir))
    }
  }

  def teardown(): Unit = {
    val facade = new MesosFacade(waitForLeader(), waitForMesosTimeout)
    val frameworkIds = facade.frameworkIds().value

    // Call mesos/teardown for all framework Ids in the cluster and wait for the teardown to complete
    frameworkIds.foreach { fId =>
      facade.teardown(fId)
      eventually(timeout(1.minutes), interval(2.seconds)) { facade.completedFrameworkIds().value.contains(fId) }
    }
  }

  override def close(): Unit = {
    Try(teardown())
    agents.foreach(_.close())
    masters.foreach(_.close())
  }

  private def mesosEnv(mesosWorkDir: File): Seq[(String, String)] = {
    def write(dir: File, fileName: String, content: String): String = {
      val file = File.createTempFile(fileName, "", dir)
      file.deleteOnExit()
      FileUtils.write(file, content, Charset.defaultCharset)
      file.setReadable(true)
      file.getAbsolutePath
    }

    val credentialsPath = write(mesosWorkDir, fileName = "credentials", content = "principal1 secret1")
    val aclsPath = write(mesosWorkDir, fileName = "acls.json", content =
      """
        |{
        |  "run_tasks": [{
        |    "principals": { "type": "ANY" },
        |    "users": { "type": "ANY" }
        |  }],
        |  "register_frameworks": [{
        |    "principals": { "type": "ANY" },
        |    "roles": { "type": "ANY" }
        |  }],
        |  "reserve_resources": [{
        |    "roles": { "type": "ANY" },
        |    "principals": { "type": "ANY" },
        |    "resources": { "type": "ANY" }
        |  }],
        |  "create_volumes": [{
        |    "roles": { "type": "ANY" },
        |    "principals": { "type": "ANY" },
        |    "volume_types": { "type": "ANY" }
        |  }]
        |}
      """.stripMargin)
    Seq(
      "MESOS_WORK_DIR" -> mesosWorkDir.getAbsolutePath,
      "MESOS_RUNTIME_DIR" -> new File(mesosWorkDir, "runtime").getAbsolutePath,
      "MESOS_LAUNCHER" -> "posix",
      "MESOS_CONTAINERIZERS" -> agentsConfig.containerizers,
      "MESOS_LAUNCHER" -> agentsConfig.launcher,
      "MESOS_ROLES" -> "public,foo",
      "MESOS_ACLS" -> s"file://$aclsPath",
      "MESOS_CREDENTIALS" -> s"file://$credentialsPath",
      "MESOS_SYSTEMD_ENABLE_SUPPORT" -> "false",
      "MESOS_SWITCH_USER" -> "false") ++
      agentsConfig.isolation.map("MESOS_ISOLATION" -> _).to[Seq] ++
      agentsConfig.imageProviders.map("MESOS_IMAGE_PROVIDERS" -> _).to[Seq]
  }

  // Get a random port from a random agent from the port range that was given to the agent during initialisation
  // This is useful for integration tests that need to bind to an accessible port. It still can happen that the
  // requested port is already bound but the chances should be slim. If not - blame @kjeschkies. Integration test
  // suites should not use the same port twice in their tests.
  def randomAgentPort(): Int = {
    import scala.util.Random
    val (min, max) = Random.shuffle(agents).head.resources.ports
    val range = min to max
    range(Random.nextInt(range.length))
  }

  // format: OFF
  case class Master(extraArgs: Seq[String]) extends Mesos {
    override val workDir = Files.createTempDirectory(s"$suiteName-mesos-master-$port").toFile
    override val processBuilder = Process(
      command = Seq(
        "mesos",
        "master",
        s"--ip=$ip",
        s"--hostname=$ip",
        s"--port=$port",
        s"--zk=$masterUrl",
        s"--work_dir=${workDir.getAbsolutePath}") ++ extraArgs,
      cwd = None, extraEnv = mesosEnv(workDir): _*)

    val processName: String = "Master"
  }

  case class Agent(resources: Resources, extraArgs: Seq[String]) extends Mesos {
    override val workDir = Files.createTempDirectory(s"$suiteName-mesos-agent-$port").toFile
    override val processBuilder = Process(
      command = Seq(
        "mesos",
        "agent",
        s"--ip=$ip",
        s"--hostname=$ip",
        s"--port=$port",
        s"--resources=${resources.resourceString()}",
        s"--master=$masterUrl",
        s"--work_dir=${workDir.getAbsolutePath}") ++ extraArgs,
      cwd = None, extraEnv = mesosEnv(workDir): _*)

    override val processName = "Agent"
  }
  // format: ON
}

trait MesosTest {
  def mesosFacade: MesosFacade
  val mesosMasterZkUrl: String
}

/**
  * Basic trait to include in tests. It comes with Zookeeper (in-memory) and a minimal Mesos cluster: one master, one
  * agent with default configuration parameters. To use, simply extend your test class from it.
  */
trait MesosClusterTest extends Suite with ZookeeperServerTest with MesosTest with ScalaFutures {
  implicit val system: ActorSystem
  implicit val mat: Materializer
  implicit val ctx: ExecutionContext
  implicit val scheduler: Scheduler

  def mastersFaultDomains: Seq[Option[FaultDomain]] = Seq.empty

  def agentsFaultDomains: Seq[Option[FaultDomain]] = Seq.empty

  def agentsGpus: Option[Int] = None

  lazy val mesosMasterZkUrl = s"zk://${zkserver.connectUrl}/mesos"
  lazy val mesosNumMasters = 1
  lazy val mesosNumSlaves = 1
  lazy val mesosQuorumSize = 1
  lazy val agentConfig = MesosAgentConfig()
  lazy val mesosLeaderTimeout: FiniteDuration = patienceConfig.timeout.toMillis.milliseconds
  lazy val mesosCluster = MesosCluster(suiteName, mesosNumMasters, mesosNumSlaves, mesosMasterZkUrl, mesosQuorumSize,
    autoStart = false, agentsConfig = agentConfig, mesosLeaderTimeout, mastersFaultDomains, agentsFaultDomains, agentsGpus = agentsGpus)
  lazy val mesosFacade = new MesosFacade(mesosCluster.waitForLeader())

  abstract override def beforeAll(): Unit = {
    super.beforeAll()
    mesosCluster.start()
  }

  abstract override def afterAll(): Unit = {
    mesosCluster.close()
    super.afterAll()
  }
}

object IP {
  import sys.process._

  lazy val routableIPv4: String =
    sys.env.getOrElse("MESOSTEST_IP_ADDRESS", inferRoutableIP)

  private def detectIpScript: String = {
    val resource = getClass.getClassLoader.getResource("detect-routable-ip.sh")
    Option(resource).flatMap { r => Option(r.getPath) }.getOrElse {
      throw new RuntimeException(
        s"Couldn't find file for detect-routable-ip.sh resource; is ${resource}. Are you running from a JAR?")
    }
  }

  private def inferRoutableIP: String = {
    Seq("bash", detectIpScript).!!.trim
  }
}
