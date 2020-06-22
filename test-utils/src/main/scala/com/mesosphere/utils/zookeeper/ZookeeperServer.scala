package com.mesosphere.utils.zookeeper

import java.util
import java.util.concurrent.CopyOnWriteArrayList

import com.mesosphere.utils.PortAllocator
import com.typesafe.scalalogging.StrictLogging
import org.apache.curator.RetryPolicy
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.test.InstanceSpec
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.apache.curator.test.TestingServer

import scala.collection.JavaConverters._

/**
  * Runs ZooKeeper server in memory at the given port.
  * The server can be started and stopped at will.
  *
  * We wrap ZookeeperServer to give it an interface more consistent with how we start Mesos and frameworks.
  *
  * close() should be called when the server is no longer necessary (e.g. try-with-resources)
  *
  * @param autoStart Start zookeeper in the background
  * @param port The port to run ZK on
  */
case class ZookeeperServer(autoStart: Boolean = true, val port: Int = PortAllocator.ephemeralPort())
    extends AutoCloseable
    with StrictLogging {

  private val config = {
    new InstanceSpec(
      null, // auto-create workdir
      port,
      PortAllocator.ephemeralPort(),
      PortAllocator.ephemeralPort(),
      true, // deleteDataDirectoryOnClose = true
      -1, // default serverId
      -1, // default tickTime
      -1,
      util.Collections.singletonMap("admin.enableServer", Boolean.box(false))
    )
  }
  private var running = autoStart
  private val testingServer = new TestingServer(config, autoStart)

  def connectUrl = testingServer.getConnectString

  /**
    * Starts or restarts the server. If the server is currently running it will be stopped
    * and restarted. If it's not currently running then it will be started. If
    * it has been closed (had close() called on it) then an exception will be
    * thrown.
    */
  def start(): Unit =
    synchronized {
      /* With Curator's TestingServer, if you call start() after stop() was called, then, sadly, nothing is done.
       * However, restart works for both the first start and second start.
       *
     * We make the start method idempotent by only calling restart if the process isn't already running, matching the
       * start/stop behavior of a local Zookeeper server.
       */
      if (!running) {
        testingServer.restart()
        running = true
      }
    }

  /**
    * Stop the server without deleting the temp directory
    */
  def stop(): Unit =
    synchronized {
      if (running) {
        testingServer.stop()
        running = false
      }
    }

  /**
    * Close the server and any open clients and delete the temp directory
    */
  def close(): Unit = {
    testingServer.close()
    running = false
  }
}

/**
  * A trait that can be mixed in to run integration tests with a in-memory Zookeeper server. Note that
  * because this trait uses [[BeforeAndAfterAll]] which invokes super.run to run the suite, you may need
  * to mix this trait in last to get the desired behavior.
  */
trait ZookeeperServerTest extends BeforeAndAfterAll with StrictLogging { this: Suite =>
  val zkserver = ZookeeperServer(autoStart = false)
  private val zkclients = new CopyOnWriteArrayList[CuratorFramework]()

  def zkClient(retryPolicy: RetryPolicy = NoRetryPolicy, namespace: Option[String] = None): CuratorFramework = {
    zkserver.start()
    val client: CuratorFramework = CuratorFrameworkFactory.newClient(zkserver.connectUrl, retryPolicy)
    client.start()

    if (
      !client.blockUntilConnected(
        client.getZookeeperClient.getConnectionTimeoutMs,
        java.util.concurrent.TimeUnit.MILLISECONDS
      )
    ) {
      throw new IllegalStateException("Failed to connect to Zookeeper. Will exit now.")
    }
    val namespaced = namespace.map(client.usingNamespace(_)).getOrElse(client)

    // No need to add the namespaced client to the list - it's just a facade for the underlying one
    zkclients.add(client)

    namespaced
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    zkserver.start()
  }

  override def afterAll(): Unit = {
    zkclients.asScala.foreach(_.close())
    zkclients.clear()

    zkserver.close()
    super.afterAll()
  }
}
