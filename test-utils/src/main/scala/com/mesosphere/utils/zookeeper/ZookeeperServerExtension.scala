package com.mesosphere.utils.zookeeper
import org.junit.jupiter.api.extension.{AfterAllCallback, BeforeAllCallback, ExtensionContext}

class ZookeeperServerExtension extends BeforeAllCallback with AfterAllCallback {
  val zkserver = ZookeeperServer(autoStart = false)

  def getConnectionUrl(): String = zkserver.connectUrl

  override def beforeAll(context: ExtensionContext): Unit = {
    zkserver.start()
  }

  override def afterAll(context: ExtensionContext): Unit = {
    zkserver.stop()
  }

}
