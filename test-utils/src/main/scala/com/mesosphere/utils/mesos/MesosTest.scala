package com.mesosphere.utils.mesos

import java.io.File
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}

import akka.Done
import akka.actor.{ActorSystem, Scheduler}
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding.Get
import akka.stream.Materializer
import com.mesosphere.utils.retry.Retry
import com.mesosphere.utils.zookeeper.ZookeeperServerTest
import com.mesosphere.utils.{PortAllocator, ProcessOutputToLogStream}
import org.apache.commons.io.FileUtils
import org.scalatest.Suite
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.async.Async._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.sys.process.{Process, ProcessBuilder}
import scala.util.Try

case class MesosConfig(
                        launcher: String = "posix",
                        containerizers: String = "mesos",
                        isolation: Option[String] = None,
                        imageProviders: Option[String] = None)

case class MesosCluster(
                         suiteName: String,
                         numMasters: Int,
                         numSlaves: Int,
                         masterUrl: String,
                         quorumSize: Int = 1,
                         autoStart: Boolean = false,
                         config: MesosConfig = MesosConfig(),
                         waitForMesosTimeout: FiniteDuration = 5.minutes,
                         mastersFaultDomains: Seq[Option[FaultDomain]],
                         agentsFaultDomains: Seq[Option[FaultDomain]],
                         agentsGpus: Option[Int] = None)(implicit
                                                         system: ActorSystem,
                                                         mat: Materializer,
                                                         ctx: ExecutionContext,
                                                         scheduler: Scheduler) extends AutoCloseable with Eventually {
  require(quorumSize > 0 && quorumSize <= numMasters)
  require(agentsFaultDomains.isEmpty || agentsFaultDomains.size == numSlaves)

  lazy val masters = 0.until(numMasters).map { i =>
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

  lazy val agents = 0.until(numSlaves).map { i =>
    // We can add additional resources constraints for our test clusters here.
    // IMPORTANT: we give each cluster's agent it's own port range! Otherwise every mesos will offer the same port range
    // to it's marathon, leading to multiple tasks (from different IT suits) trying to use the same port!
    // First-come-first-served task will bind successfully where the others will fail leading to a lot inconsistency and
    // flakiness in tests.

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

    // uniquely identify each agent node, useful for constraint matching
    val attributes: Map[String, Option[String]] = Map("node" -> Some(i.toString)) ++ faultDomainAgentAttributes

    val renderedAttributes: String = attributes.map { case (key, maybeVal) => s"$key${maybeVal.map(v => s":$v").getOrElse("")}" }.mkString(";")

    Agent(resources = new Resources(ports = PortAllocator.portsRange(), gpus = agentsGpus), extraArgs = Seq(
      s"--attributes=$renderedAttributes"
    ) ++ mesosFaultDomainAgentCmdOption.map(fd => s"--domain=$fd"))
  }

  if (autoStart) {
    start()
  }

  def start(): Future[String] = async {
    masters.foreach(_.start())
    agents.foreach(_.start())
    val masterUri = await(waitForLeader())
    await(waitForAgents(masterUri))
    masterUri
  }

  def waitForLeader(): Future[String] = async {
    val firstMaster = s"http://${masters.head.ip}:${masters.head.port}"
    val result = Retry("wait for leader", maxAttempts = Int.MaxValue, maxDuration = waitForMesosTimeout) {
      Http().singleRequest(Get(firstMaster + "/redirect")).map { result =>
        result.discardEntityBytes() // forget about the body
        if (result.status.isFailure()) {
          throw new Exception(s"Couldn't determine leader: $result")
        }
        result
      }
    }

    def maybeFixURI(uri: String): String = {
      // some versions of mesos issue redirects with broken Location headers; fix them here
      if (uri.indexOf("//") == 0) {
        "http:" + uri
      } else {
        uri
      }
    }

    val location = await(result).headers.find(_.lowercaseName() == "location").map(_.value()).getOrElse(firstMaster)
    maybeFixURI(location)
  }

  def waitForAgents(masterUri: String): Future[Done] = {
    val mesosFacade = new MesosFacade(masterUri)
    Retry.blocking("wait for agents", maxAttempts = Int.MaxValue, maxDuration = waitForMesosTimeout) {
      val numAgents = mesosFacade.state.value.agents.size
      if (numAgents != agents.size) {
        throw new Exception(s"Agents are not ready. Found $numAgents but expected ${agents.size}")
      }
      Done
    }
  }

  def stop(): Unit = {
    masters.foreach(_.stop())
    agents.foreach(_.stop())
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
      "MESOS_CONTAINERIZERS" -> config.containerizers,
      "MESOS_LAUNCHER" -> config.launcher,
      "MESOS_ROLES" -> "public,foo",
      "MESOS_ACLS" -> s"file://$aclsPath",
      "MESOS_CREDENTIALS" -> s"file://$credentialsPath",
      "MESOS_SYSTEMD_ENABLE_SUPPORT" -> "false",
      "MESOS_SWITCH_USER" -> "false") ++
      config.isolation.map("MESOS_ISOLATION" -> _).to[Seq] ++
      config.imageProviders.map("MESOS_IMAGE_PROVIDERS" -> _).to[Seq]
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

  trait Mesos extends AutoCloseable {
    val extraArgs: Seq[String]
    val ip = IP.routableIPv4
    val port = PortAllocator.ephemeralPort()
    val workDir: File
    val processBuilder: ProcessBuilder
    val processName: String
    private var process = Option.empty[Process]

    if (autoStart) {
      start()
    }

    def start(): Unit = if (process.isEmpty) {
      process = Some(create())
    }

    def stop(): Unit = {
      process.foreach(_.destroy())
      process = Option.empty[Process]
    }

    private def create(): Process = {
      processBuilder.run(ProcessOutputToLogStream(s"$suiteName-Mesos$processName-$port"))
    }

    override def close(): Unit = {
      def copySandboxFiles() = {
        val projectDir = sys.props.getOrElse("user.dir", ".")
        FileUtils.copyDirectory(workDir, Paths.get(projectDir, "sandboxes", suiteName).toFile)
      }
      // Copy all sandbox files (useful for debugging) into current directory for Jenkins to archive it:
      Try(copySandboxFiles())
      stop()
      Try(FileUtils.deleteDirectory(workDir))
    }
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

  def state = new MesosFacade(Await.result(waitForLeader(), waitForMesosTimeout)).state

  def teardown(): Unit = {
    val facade = new MesosFacade(Await.result(waitForLeader(), waitForMesosTimeout))
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
}
// format: ON

trait MesosTest {
  def mesos: MesosFacade
  val mesosMasterUrl: String
}

trait MesosClusterTest extends Suite with ZookeeperServerTest with MesosTest with ScalaFutures {
  implicit val system: ActorSystem
  implicit val mat: Materializer
  implicit val ctx: ExecutionContext
  implicit val scheduler: Scheduler

  def mastersFaultDomains: Seq[Option[FaultDomain]] = Seq.empty

  def agentsFaultDomains: Seq[Option[FaultDomain]] = Seq.empty

  def agentsGpus: Option[Int] = None

  private val localMesosUrl = sys.env.get("USE_LOCAL_MESOS")
  lazy val mesosMasterUrl = s"zk://${zkserver.connectUri}/mesos"
  lazy val mesosNumMasters = 1
  lazy val mesosNumSlaves = 1
  lazy val mesosQuorumSize = 1
  lazy val mesosConfig = MesosConfig()
  lazy val mesosLeaderTimeout: FiniteDuration = patienceConfig.timeout.toMillis.milliseconds
  lazy val mesosCluster = MesosCluster(suiteName, mesosNumMasters, mesosNumSlaves, mesosMasterUrl, mesosQuorumSize,
    autoStart = false, config = mesosConfig, mesosLeaderTimeout, mastersFaultDomains, agentsFaultDomains, agentsGpus = agentsGpus)
  lazy val mesos = new MesosFacade(localMesosUrl.getOrElse(mesosCluster.waitForLeader().futureValue))

  abstract override def beforeAll(): Unit = {
    super.beforeAll()
    localMesosUrl.fold(mesosCluster.start().futureValue)(identity)
  }

  abstract override def afterAll(): Unit = {
    localMesosUrl.fold(mesosCluster.close())(_ => ())
    super.afterAll()
  }
}

object IP {
  import sys.process._

  lazy val routableIPv4: String =
    sys.env.getOrElse("MESOSTEST_IP_ADDRESS", inferRoutableIP)

  private def detectIpScript = {
    val resource = getClass.getClassLoader.getResource("detect-routable-ip.sh")
    Option(resource).flatMap { f => Option(f.getFile) }.getOrElse {
      throw new RuntimeException(
        s"Couldn't find file for detect-routable-ip.sh resource; is ${resource}. Are you running from a JAR?")
    }
  }

  private def inferRoutableIP: String = {
    Seq("bash", detectIpScript).!!.trim
  }
}
