package com.mesosphere.mesos

import java.net.URL
import java.nio.charset.StandardCharsets
import java.util.concurrent.CompletionStage

import com.mesosphere.usi.metrics.Metrics
import com.mesosphere.usi.storage.zookeeper.PersistenceStore.Node
import com.mesosphere.usi.storage.zookeeper.{
  AsyncCuratorBuilderFactory,
  AsyncCuratorBuilderSettings,
  ZooKeeperPersistenceStore
}
import com.typesafe.scalalogging.StrictLogging
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.RetryOneTime
import org.apache.mesos.v1.Protos
import play.api.libs.functional.syntax._
import play.api.libs.json._
import play.api.libs.json.Reads._

import scala.async.Async.{async, await}
import scala.compat.java8.FutureConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

trait MasterDetector {

  /** @return the Mesos master URL for the cluster. */
  def getMaster()(implicit ex: ExecutionContext): CompletionStage[URL]

  /** @return whether the master string is valid. */
  def isValid(): Boolean
}

object MasterDetector {

  /**
    * Constructs a master detector base on the master string passed.
    *
    * @param master The master string should be one of:
    *               host:port
    *               http://host:port
    *               zk://host1:port1,host2:port2,.../path
    *               zk://username:password@host1:port1,host2:port2,.../path
    * @return A master detector.
    */
  def apply(master: String, metrics: Metrics): MasterDetector = {
    if (master.startsWith("zk://")) {
      Zookeeper(master, metrics)
    } else {
      Standalone(master)
    }
  }
}

case class Zookeeper(master: String, metrics: Metrics) extends MasterDetector with StrictLogging {
  require(master.startsWith("zk://"), s"$master does not start with zk://")

  case class ZkUrl(auth: Option[String], servers: String, path: String)

  implicit val mesosInfoRead: Reads[Protos.MasterInfo] = (
    (JsPath \ "hostname").read[String] ~
      (JsPath \ "port").read[Int] ~
      (JsPath \ "id").read[String] ~
      (JsPath \ "ip").read[Int]
  ) { (hostname, port, id, ip) =>
    Protos.MasterInfo.newBuilder().setHostname(hostname).setPort(port).setId(id).setIp(ip).build()
  }

  override def isValid(): Boolean = Try(parse()).map(_ => true).getOrElse(false)

  override def getMaster()(implicit ex: ExecutionContext): CompletionStage[URL] = {
    val ZkUrl(auth, servers, path) = parse()

    val client = {
      val clientBuilder = CuratorFrameworkFactory.builder().connectString(servers).retryPolicy(new RetryOneTime(100))
      auth.foreach(userPassword => clientBuilder.authorization("digest", userPassword.getBytes))
      clientBuilder.build()
    }
    client.start()

    if (!client.blockUntilConnected(
        client.getZookeeperClient.getConnectionTimeoutMs,
        java.util.concurrent.TimeUnit.MILLISECONDS)) {
      throw new IllegalStateException("Failed to connect to Zookeeper. Will exit now.")
    }

    val clientSettings = AsyncCuratorBuilderSettings(createOptions = Set.empty, compressedData = false)
    val factory: AsyncCuratorBuilderFactory = AsyncCuratorBuilderFactory(client, clientSettings)
    val store: ZooKeeperPersistenceStore = new ZooKeeperPersistenceStore(metrics, factory, parallelism = 1)

    val future = async {
      val children = await(store.children(path, false)).get
      logger.info(s"Found Mesos leader node children $children")
      val leader = children.filter(_.startsWith("json.info")).min

      val leaderPath = s"$path/$leader"
      logger.info(s"Connecting to Zookeeper at $servers and fetching Mesos master from $leaderPath.")

      val Node(_, bytes) = await(store.read(leaderPath)).get
      logger.info(s"Mesos leader data: ${bytes.decodeString(StandardCharsets.UTF_8)}")

      val masterInfo = Json.parse(bytes.decodeString(StandardCharsets.UTF_8)).as[Protos.MasterInfo]
      // TODO: how do we know it's http or https.
      val url = new URL(s"http://${masterInfo.getHostname}:${masterInfo.getPort}")
      url
    }

    // Ensure Zookeeper client is closed.
    future.onComplete {
      case Failure(t) =>
        logger.error("Failed to get Mesos master leader node from ZK: ", t)
        client.close()
      case Success(_) =>
        client.close()
    }

    future.toJava
  }

  /** @return proper Zookeeper connection string as per {@link ZooKeeper#ZooKeeper(String, int, Watcher)} etc. */
  def parse(): ZkUrl = {
    // Strip leading zk://
    val stripped = master.substring(5)

    // Extract path
    val pathIndex = stripped.indexOf('/')
    val path = if (pathIndex < 0) "/" else stripped.substring(pathIndex, stripped.length)

    // Find optional authentication
    val endIndex = if (pathIndex < 0) stripped.length else pathIndex
    stripped.substring(0, endIndex).split('@') match {
      case Array(auth, servers) => ZkUrl(Some(auth), servers, path)
      case Array(servers) => ZkUrl(None, servers, path)
      case _ => throw new IllegalArgumentException(s"$master contained more than one authentication.")
    }
  }
}

case class Standalone(master: String) extends MasterDetector {
  def url = if (master.startsWith("http") || master.startsWith("https")) new URL(master) else new URL(s"http://$master")

  override def isValid(): Boolean = Try(url).map(_ => true).getOrElse(false)

  override def getMaster()(implicit ex: ExecutionContext): CompletionStage[URL] = Future.successful(url).toJava
}
