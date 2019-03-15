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
    // this.mesosCluster.suiteName = context.getDisplayName()
    this.mesosUrl = mesosCluster.start
  }

  override def afterAll(context: ExtensionContext): Unit = { mesosCluster.close() }
}

object MesosClusterExtension {

  def builder = new Builder

  class Builder {

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

    def withMesosMasterUrl(url: String): Builder = {
      this.mesosMasterZkUrl = url
      this
    }

    def withNumMasters(num: Int): Builder = {
      this.mesosNumMasters = num
      this
    }

    def withNumAgents(num: Int): Builder = {
      this.mesosNumAgents = num
      this
    }

    def withQuorumSize(num: Int): Builder = {
      this.mesosQuorumSize = num
      this
    }

    def withAgentConfig(config: MesosAgentConfig): Builder = {
      this.agentConfig = config
      this
    }

    def withLeaderTimeout(length: Long, unit: TimeUnit): Builder = {
      this.mesosLeaderTimeout = new FiniteDuration(length, unit)
      this
    }

    def build(system: ActorSystem, materializer: Materializer): MesosClusterExtension = {
      val mesosCluster = new MesosCluster(
        "connection-test", // Use extension context
        this.mesosNumMasters,
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
