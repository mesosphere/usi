package com.mesosphere.utils.mesos

import java.io.File
import java.net.{InetAddress, NetworkInterface, URL}
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}

import akka.actor.{ActorSystem, Scheduler}
import akka.stream.Materializer
import com.mesosphere.utils.mesos.MesosFacade.ITAgentDetails
import com.mesosphere.utils.mesos.MesosTest.AgentLike
import com.mesosphere.utils.zookeeper.ZookeeperServerTest
import com.mesosphere.utils.{PortAllocator, ProcessOutputToLogStream}
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.FileUtils
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{Matchers, Suite}

import scala.collection.JavaConverters._
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
  * @param seccompConfigDir
  * @param seccompProfileName
  * @param executorRegistrationTimeout Defaults to one minute.
  * @param fetcherStallTimeout Defaults to one minute and must be lower or equal to the executor registration timeout.
  */
case class MesosAgentConfig(
    launcher: String = "posix",
    containerizers: String = "mesos",
    isolation: Option[String] = None,
    imageProviders: Option[String] = None,
    seccompConfigDir: Option[String] = None,
    seccompProfileName: Option[String] = None,
    executorRegistrationTimeout: Duration = 1.minute,
    fetcherStallTimeout: Duration = 1.minute,
    cgroupsEnableCfs: Boolean = false
) {

  require(
    executorRegistrationTimeout >= fetcherStallTimeout,
    s"Executor registration timeout $executorRegistrationTimeout must be bigger or equal than fetcher stall timeout $fetcherStallTimeout."
  )

  require(
    validSeccompConfig,
    "To enable seccomp, agentSeccompConfigDir should be defined and isolation \"linux/seccomp\" used"
  )

  def validSeccompConfig: Boolean =
    seccompConfigDir.isEmpty || (seccompConfigDir.isDefined && isolation.get.contains("linux/seccomp"))
}

/**
  *
  * @param numMasters number of Mesos master
  * @param numAgents number of Mesos agents
  * @param quorumSize Mesos master quorum size
  * @param mastersFaultDomains Mesos master fault domain configuration
  * @param agentsFaultDomains Mesos agents fault domain configuration
  * @param agentsGpus number of GPUs per agent
  * @param restrictedToRoles
  */
case class MesosConfig(
    numMasters: Int = 1,
    numAgents: Int = 1,
    quorumSize: Int = 1,
    mastersFaultDomains: Seq[Option[FaultDomain]] = Seq.empty,
    agentsFaultDomains: Seq[Option[FaultDomain]] = Seq.empty,
    agentsGpus: Option[Int] = None, // TODO: move agent related config into MesosAgentConfig
    restrictedToRoles: Option[String] = Some("public,foo,test")
) {

  require(validQuorumSize, "Mesos quorum size should be 0 or smaller than number of agents")
  require(validFaultDomainConfig, "Fault domains should be configure for all agents or not at all")

  def validQuorumSize: Boolean = quorumSize > 0 && quorumSize <= numMasters
  def validFaultDomainConfig: Boolean = agentsFaultDomains.isEmpty || agentsFaultDomains.size == numAgents
}

/**
  * Basic class that wraps starting/stopping a Mesos cluster with passed agent/master configuration and configurable
  * number of masters/agents.
  *
  * @param suiteName suite name that uses the cluster. will be included into logging messages
  * @param masterUrl Mesos master ZK url in the form of zk://zk-1/mesos
  * @param autoStart if true, the cluster will be started automatically
  * @param config Mesos master configuration
  * @param agentsConfig Mesos agent configuration
  * @param waitForMesosTimeout time to wait for Mesos master/agent to start
  */
case class MesosCluster(
    suiteName: String,
    masterUrl: String,
    autoStart: Boolean = false,
    config: MesosConfig = MesosConfig(),
    agentsConfig: MesosAgentConfig = MesosAgentConfig(),
    waitForMesosTimeout: FiniteDuration = 5.minutes
)(implicit system: ActorSystem, mat: Materializer)
    extends AutoCloseable
    with Eventually
    with StrictLogging {

  lazy val masters: Seq[Master] = 0.until(config.numMasters).map { i =>
    val faultDomainJson = if (config.mastersFaultDomains.nonEmpty && config.mastersFaultDomains(i).nonEmpty) {
      val faultDomain = config.mastersFaultDomains(i).get
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

    Master(extraArgs =
      Seq(
        "--agent_ping_timeout=1secs",
        "--max_agent_ping_timeouts=4",
        s"--quorum=${config.quorumSize}"
      ) ++ faultDomainJson
        .map(fd => s"--domain=$fd")
    )
  }

  private var initialCachedAgentDetails: Map[AgentLike, ITAgentDetails] = Map.empty

  lazy val agents: Seq[Agent] = 0.until(config.numAgents).map { i =>
    val (faultDomainAgentAttributes: Map[String, Option[String]], mesosFaultDomainAgentCmdOption) =
      if (config.agentsFaultDomains.nonEmpty && config.agentsFaultDomains(i).nonEmpty) {
        val faultDomain = config.agentsFaultDomains(i).get
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
          "fault_domain_zone" -> Some(faultDomain.zone.value)
        )
        (nodeAttributes, Some(mesosFaultDomainCmdOption))
      } else Map.empty -> None

    // Uniquely identify each agent node, useful for constraint matching
    val attributes: Map[String, Option[String]] = Map("node" -> Some(i.toString)) ++ faultDomainAgentAttributes

    val renderedAttributes: String = attributes.map {
      case (key, maybeVal) => s"$key${maybeVal.map(v => s":$v").getOrElse("")}"
    }.mkString(";")

    // We can add additional resources constraints for our test clusters here.
    // IMPORTANT: we give each cluster's agent it's own port range! Otherwise every Mesos will offer the same port range
    // to it's frameworks, leading to multiple tasks (from different test suits) trying to use the same port!
    // First-come-first-served task will bind successfully where the others will fail leading to a lot inconsistency and
    // flakiness in tests.
    Agent(
      resources = new Resources(ports = PortAllocator.portsRange(), gpus = config.agentsGpus),
      agentsConfig = agentsConfig,
      renderedAttributes = renderedAttributes,
      mesosFaultDomainAgentCmdOption = mesosFaultDomainAgentCmdOption
    )
  }

  if (autoStart) {
    start()
  }

  /**
    * Starts a Mesos cluster.
    *
    * @param prefix An optional prefix for the master and angent logs.
    * @return The leader URL.
    */
  def start(prefix: Option[String] = None): URL = {
    masters.foreach(_.start(prefix))
    agents.foreach(_.start(prefix))
    val masterUrl = waitForLeader()
    waitForAgents(masterUrl)
    logger.info(s"Started Mesos cluster with master on $masterUrl")
    masterUrl
  }

  /**
    * Kills the Mesos master to force a different master to become leader.
    *
    * @return The URL of the new leader.
    */
  def failover(): URL = {
    require(config.numMasters > 1, s"The number of masters ${config.numMasters} is not bigger than 1.")

    val leaderPort = waitForLeader().getPort
    val oldLeader = masters
      .find(_.port == leaderPort)
      .getOrElse(
        throw new IllegalStateException(s"Could not find leader $leaderPort in ${masters.map(_.port).mkString(", ")}.")
      )
    oldLeader.restart()
    eventually(timeout(waitForMesosTimeout), interval(1.seconds)) {
      val possibleNewLeader = waitForLeader();
      assert(leaderPort != possibleNewLeader.getPort, "Leader did not change.")
      logger.info(s"Changed leader from $leaderPort to ${possibleNewLeader.getPort}.")
      possibleNewLeader
    }
  }

  def waitForLeader(): URL = {
    val firstMaster = new URL(s"http://${masters.head.ip}:${masters.head.port}")
    val mesosFacade = new MesosFacade(firstMaster)

    val leader = eventually(timeout(waitForMesosTimeout), interval(1.seconds)) {
      mesosFacade.redirect().headers.find(_.lowercaseName() == "location").map(_.value()).get
    }
    new URL(s"http:$leader")
  }

  def waitForAgents(masterUrl: URL): Unit = {
    val mesosFacade = new MesosFacade(masterUrl)
    eventually(timeout(waitForMesosTimeout), interval(1.seconds)) {
      assert(mesosFacade.agents().value.slaves.size == agents.size)
    }
    initialCachedAgentDetails = agents.map { a =>
      a -> mesosFacade.agentDetails(a).value
    }.toMap
  }

  /** Returns a agent id for an agent as it was initially queried during test cluster launch
    *
    * @param agent
    */
  def agentIdFor(agent: AgentLike): String = {
    initialCachedAgentDetails(agent).id
  }

  /**
    * Return the cached agent details that were returned when the agent first initialized
    * @param agent Reference to the MesosTest Agent process
    */
  def initialAgentDetailsFor(agent: AgentLike): ITAgentDetails =
    initialCachedAgentDetails(agent)

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
    val ip = IP.routableIPv4
    val port = PortAllocator.ephemeralPort()
    def host(): URL = new URL("http", ip, port, "")

    val workDir: File
    val processBuilder: ProcessBuilder
    val processName: String
    private var process: Option[Process] = None

    private var prefix: Option[String] = None

    if (autoStart) {
      start()
    }

    def isAlive(): Boolean = {
      process.exists(_.isAlive())
    }

    /**
      * Starts a Mesos master or agent.
      * @param prefix May be defined to override the log prefix. It defaults to [[suiteName]].
      */
    def start(prefix: Option[String] = None): Unit =
      if (process.isEmpty) {
        assert(!isAlive(), s"Mesos process ${processName} already started")

        // Capture prefix if required. [[MesosClusterExtension]] is using this feature.
        if (prefix.nonEmpty) this.prefix = prefix

        val name = this.prefix.getOrElse(suiteName)
        process = Some(processBuilder.run(ProcessOutputToLogStream(s"$name-Mesos$processName-$port")))
      }

    def stop(): Unit = {
      process.foreach(_.destroy())
      process = None
    }

    def restart(): Unit = {
      if (isAlive()) {
        process.foreach { p =>
          p.destroy()
          // Wait for process to exit
          p.exitValue()
        }
        process = None
      }

      start()
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
    logger.info("Stopping Mesos cluster.")
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

    val credentialsPath = write(
      mesosWorkDir,
      fileName = "credentials",
      content = """
        |{
        |  "credentials" : [{ "principal": "principal1", "secret": "secret1" }]
        |}
      """.stripMargin
    )

    val aclsPath = write(
      mesosWorkDir,
      fileName = "acls.json",
      content = """
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
      """.stripMargin
    )
    Seq(
      "MESOS_WORK_DIR" -> mesosWorkDir.getAbsolutePath,
      "MESOS_RUNTIME_DIR" -> new File(mesosWorkDir, "runtime").getAbsolutePath,
      "MESOS_CONTAINERIZERS" -> agentsConfig.containerizers,
      "MESOS_LAUNCHER" -> agentsConfig.launcher,
      "MESOS_ACLS" -> s"file://$aclsPath",
      "MESOS_CREDENTIALS" -> s"file://$credentialsPath",
      "MESOS_SYSTEMD_ENABLE_SUPPORT" -> "false",
      "MESOS_SWITCH_USER" -> "false"
    ) ++
      config.restrictedToRoles.map("MESOS_ROLES" -> _).toSeq ++
      agentsConfig.isolation.map("MESOS_ISOLATION" -> _).toSeq ++
      agentsConfig.imageProviders.map("MESOS_IMAGE_PROVIDERS" -> _).toSeq
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


  /**
    * Mesos agent closure that captures work directory, cgroups, resources etc.
    *
    * @param resources The resources assigned to the agent.
    * @param logVerbosityLevel The log level for GLOG_v.
    */
  case class Agent(resources: Resources, agentsConfig: MesosAgentConfig, renderedAttributes: String, mesosFaultDomainAgentCmdOption: Option[String], logVerbosityLevel: Int = 2) extends Mesos with MesosTest.AgentLike {
    /**
      * We can only specify the cgroups_root flag if running the integration tests under Linux; on Mac OS this flag is unrecognized.
      */
    private val cgroupsArgs: Seq[String] =
      if (MesosTest.isLinux)
        Seq(
          s"--cgroups_root=mesos$port", // See MESOS-9960 for more info
          if (agentsConfig.cgroupsEnableCfs) "--cgroups_enable_cfs" else "--no-cgroups_enable_cfs")
      else
        Nil

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
        s"--work_dir=${workDir.getAbsolutePath}",
        s"""--executor_environment_variables={"GLOG_v": "$logVerbosityLevel"}""",
        s"--attributes=$renderedAttributes",
        s"--executor_registration_timeout=${agentsConfig.executorRegistrationTimeout.toSeconds}secs",
        s"--fetcher_stall_timeout=${agentsConfig.fetcherStallTimeout.toSeconds}secs") ++
        cgroupsArgs ++
        mesosFaultDomainAgentCmdOption.map(fd => s"--domain=$fd") ++
        agentsConfig.seccompConfigDir.map(dir => s"--seccomp_config_dir=$dir") ++
        agentsConfig.seccompProfileName.map(prf => s"--seccomp_profile_name=$prf"),
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
trait MesosClusterTest
    extends Suite
    with ZookeeperServerTest
    with MesosTest
    with ScalaFutures
    with Eventually
    with Matchers {
  implicit val system: ActorSystem
  implicit val mat: Materializer
  implicit val ctx: ExecutionContext
  implicit val scheduler: Scheduler

  def mastersFaultDomains: Seq[Option[FaultDomain]] = Seq.empty

  def agentsFaultDomains: Seq[Option[FaultDomain]] = Seq.empty

  def agentsGpus: Option[Int] = None

  lazy val mesosMasterZkUrl = s"zk://${zkserver.connectUrl}/mesos"
  lazy val mesosConfig = MesosConfig()
  lazy val agentConfig = MesosAgentConfig()
  lazy val mesosLeaderTimeout: FiniteDuration = patienceConfig.timeout.toMillis.milliseconds
  lazy val mesosCluster = MesosCluster(
    suiteName,
    mesosMasterZkUrl,
    autoStart = false,
    config = mesosConfig,
    agentsConfig = agentConfig,
    waitForMesosTimeout = mesosLeaderTimeout
  )
  lazy val mesosFacade = {
    assert(
      mesosCluster.masters.exists(_.isAlive()),
      "Order of initialization error! Reference to MesosFacade obtained before Mesos master is started. Did you accidentally access something non lazily (before the test suite beforeAll is run)?"
    )
    new MesosFacade(mesosCluster.waitForLeader())
  }

  abstract override def beforeAll(): Unit = {
    super.beforeAll()
    mesosCluster.start()
  }

  abstract override def afterAll(): Unit = {
    mesosCluster.close()
    super.afterAll()
  }
}

object MesosTest {
  import sys.process._
  lazy val isLinux: Boolean =
    Seq("uname").!!.trim.startsWith("Linux")
  trait AgentLike {
    def ip: String
    def port: Int
  }
}

object IP extends StrictLogging {

  lazy val routableIPv4: String = sys.env.getOrElse("MESOSTEST_IP_ADDRESS", inferRoutableIP)

  private def inferRoutableIP: String = {
    logger.info(s"=== Scanning InetAddresses ===")
    val nets: Iterator[NetworkInterface] = NetworkInterface.getNetworkInterfaces.asScala

    // Get all available InetAdresses from all network interfaces on the machine
    val addresses: Iterator[InetAddress] = nets.map { net =>
      net.getInetAddresses.asScala.map { addr =>
        logger.info(s"""|Display name: ${net.getDisplayName}
                       |Name: ${net.getName}
                       |InetAddress: ${addr}
                       |Loopback: ${addr.isLoopbackAddress}
                       |Site-local: ${addr.isSiteLocalAddress}
                       |Link-local: ${addr.isLinkLocalAddress}
                       |Multicast: ${addr.isMulticastAddress}
       """.stripMargin)
        addr
      }
    }.flatten

    // We filter out loopback addresses (not what we want) and afterwards the list of priorities is:
    // 1. A site-local InetAddress (isSiteLocalAddress == true)
    // 2. InetAddress.getLocalHost as a fallback is the above didn't work
    //
    // There is also a possibility to get a link-local InetAddress before falling back to the getLocalHost method
    // but I'm not sure if that's actually needed.
    //
    // [Site-local and link-local addresses](https://4sysops.com/archives/ipv6-tutorial-part-6-site-local-addresses-and-link-local-addresses/)
    // [Useful Stackoverflow](https://stackoverflow.com/questions/9481865/getting-the-ip-address-of-the-current-machine-using-java/38342964#38342964)
    val siteLocal: Option[InetAddress] = addresses
      .filter(addr => !addr.isLoopbackAddress)
      .find(addr => addr.isSiteLocalAddress)

    siteLocal
      .getOrElse(InetAddress.getLocalHost)
      .getHostAddress
  }
}
