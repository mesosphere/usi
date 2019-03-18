package com.mesosphere.utils.zookeeper
import org.junit.jupiter.api.extension.{AfterAllCallback, BeforeAllCallback, ExtensionContext}

/**
  * Provides a JUnit 5 extension for booting a Zookeeper server for tests.
  *
  * @see [[com.mesosphere.utils.mesos.MesosClusterExtension]] for an example.
  */
class ZookeeperServerExtension extends BeforeAllCallback with AfterAllCallback {
  val zkserver = ZookeeperServer(autoStart = false)

  /** @return the URL of the started Zookeeper server. */
  def getConnectionUrl(): String = zkserver.connectUrl

  override def beforeAll(context: ExtensionContext): Unit = {
    zkserver.start()
  }

  override def afterAll(context: ExtensionContext): Unit = {
    zkserver.stop()
  }

}
