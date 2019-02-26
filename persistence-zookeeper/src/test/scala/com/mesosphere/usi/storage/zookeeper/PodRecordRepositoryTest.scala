package com.mesosphere.usi.storage.zookeeper

import akka.actor.ActorSystem
import com.mesosphere.usi.metrics.Metrics
import com.mesosphere.usi.repository.RepositoryBehavior
import com.mesosphere.utils.UnitTest
import com.mesosphere.utils.metrics.DummyMetrics
import com.mesosphere.utils.zookeeper.ZookeeperServerTest
import org.apache.curator.framework.CuratorFramework
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.concurrent.ExecutionContext

@RunWith(classOf[JUnitRunner])
class PodRecordRepositoryTest extends UnitTest with ZookeeperServerTest with RepositoryBehavior {

  implicit val system: ActorSystem = ActorSystem()
  implicit val ec: ExecutionContext = system.dispatcher

  lazy val client: CuratorFramework = zkClient(namespace = Some("test"))
  lazy val factory: AsyncCuratorBuilderFactory = AsyncCuratorBuilderFactory(client)
  lazy val metrics: Metrics = DummyMetrics
  lazy val store: ZooKeeperPersistenceStore = new ZooKeeperPersistenceStore(metrics, factory, parallelism = 1)

  def podRepo(): PodRecordRepository = new PodRecordRepository(store)

  "The Zookeeper backed pod record repository" should { behave like podRecordRepository(podRepo) }
}
