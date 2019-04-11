package com.mesosphere.utils.mesos
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.Materializer
import org.junit.jupiter.api.extension.{AfterAllCallback, BeforeAllCallback, ExtensionContext}

import scala.concurrent.duration.FiniteDuration

class MesosClusterExtension(val mesosCluster: MesosCluster) extends BeforeAllCallback with AfterAllCallback {

  private var mesosUrl: String = null

  def getMesosUrl: String = mesosUrl

  override def beforeAll(context: ExtensionContext): Unit = {
    this.mesosUrl = mesosCluster.start(Some(context.getDisplayName))
  }

  override def afterAll(context: ExtensionContext): Unit = { mesosCluster.close() }
}

/**
  * Provides a JUnit 5 extension for booting a Mesos cluster for tests.
  *
  * A minimal configuration is
  *
  * {{{
  * import akka.actor.ActorSystem;
  * import akka.stream.ActorMaterializer;
  * import com.mesosphere.utils.mesos.MesosClusterExtension;
  * import com.mesosphere.utils.zookeeper.ZookeeperServerExtension;
  * import org.junit.jupiter.api.Test;
  * import org.junit.jupiter.api.extension.RegisterExtension;
  *
  * class ConnectionTest {
  *
  *   @RegisterExtension static ZookeeperServerExtension zkServer = new ZookeeperServerExtension();
  *
  *   static ActorSystem system = ActorSystem.create("mesos-scheduler-test");
  *   static ActorMaterializer materializer = ActorMaterializer.create(system);
  *
  *   @RegisterExtension
  *   static MesosClusterExtension mesosCluster =
  *       MesosClusterExtension.builder()
  *           .withMesosMasterUrl(String.format("zk://%s/mesos", zkServer.getConnectionUrl()))
  *           .build(system, materializer);
  *
  *   @Test
  *   public void connectMesosApi() throws InterruptedException, ExecutionException {
  *     System.out.println("Started Mesos cluster at " + mesosCluster.getMesosUrl());
  *   }
  * }
  * }}}
  */
object MesosClusterExtension {

  def builder = new Builder

  class Builder {

    private[mesos] var suiteName: String = "unknown"
    private[mesos] var mesosMasterZkUrl: String = null
    private[mesos] var mesosNumMasters = 1
    private[mesos] var mesosNumAgents = 1
    private[mesos] var mesosQuorumSize = 1
    private[mesos] var agentConfig = new MesosAgentConfig("posix", "mesos", Option.empty, Option.empty)
    private[mesos] var mesosLeaderTimeout = new FiniteDuration(30, TimeUnit.SECONDS)

    // So far we do not allow any modification to the fault domains and GPU configuration.
    private[mesos] var mastersFaultDomains: Seq[Option[FaultDomain]] = Vector.empty
    private[mesos] var agentsFaultDomains: Seq[Option[FaultDomain]] = Vector.empty
    private[mesos] val agentsGpus = Option.empty

    /**
      * Configure the Zookeeper URL where the Mesos master URL is stored.
      * @param url Mesos master ZK url in the form of {{{zk://<url>/mesos}}}.
      * @return the updated builder.
      */
    def withMesosMasterUrl(url: String): Builder = {
      this.mesosMasterZkUrl = url
      this
    }

    /** @return the builder with the number of masters updated to {{{num}}}. Defaults to 1. */
    def withNumMasters(num: Int): Builder = {
      this.mesosNumMasters = num
      this
    }

    /** @return the builder with the number of agents updated to {{{num}}}. Defaults to 1. */
    def withNumAgents(num: Int): Builder = {
      this.mesosNumAgents = num
      this
    }

    /** @return the builder with the Mesos quorum size updated to {{{num}}}. Defaults to 1. */
    def withQuorumSize(num: Int): Builder = {
      this.mesosQuorumSize = num
      this
    }

    /** @return the builder updated with the given [[MesosAgentConfig]]. */
    def withAgentConfig(config: MesosAgentConfig): Builder = {
      this.agentConfig = config
      this
    }

    /** @return the builder updated with a timeout waiting for the leader to start. Defaults to 30 seconds. */
    def withLeaderTimeout(length: Long, unit: TimeUnit): Builder = {
      this.mesosLeaderTimeout = new FiniteDuration(length, unit)
      this
    }

    /** @return the build updated with a {{{prefix}}} as the log prefix. */
    def withLogPrefix(prefix: String): Builder = {
      this.suiteName = prefix
      this
    }

    /**
      * Builds the Mesos cluster and the extensions.
      *
      * @param system The Akka actor system to use for all API connections.
      * @param materializer The Akka streams materializer to use for all API connections.
      * @return
      */
    def build(system: ActorSystem, materializer: Materializer): MesosClusterExtension = {
      val mesosCluster = new MesosCluster(
        suiteName,
        mesosNumMasters,
        mesosNumAgents,
        mesosMasterZkUrl,
        mesosQuorumSize,
        false,
        agentConfig,
        mesosLeaderTimeout,
        mastersFaultDomains,
        agentsFaultDomains,
        agentsGpus
      )(system, materializer)

      new MesosClusterExtension(mesosCluster)
    }
  }
}
