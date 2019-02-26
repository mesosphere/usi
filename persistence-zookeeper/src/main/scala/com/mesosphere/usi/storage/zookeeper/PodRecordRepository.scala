package com.mesosphere.usi.storage.zookeeper

import akka.util.ByteString
import com.mesosphere.usi.core.models.{PodId, PodRecord}
import com.mesosphere.usi.repository.{PodRecordRepository => PodRecordRepositoryInterface}
import com.mesosphere.usi.storage.zookeeper.PersistenceStore.Node
import org.apache.zookeeper.KeeperException.NoNodeException

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

class PodRecordRepository(val store: PersistenceStore) extends PodRecordRepositoryInterface {

  override def create(record: PodRecord): Future[PodId] = async {
    val data = record.toString
    val node = Node(record.podId.value, ByteString(data))
    await(store.create(node))
    record.podId
    // TODO: RecordAlreadyExistsException
  }

  override def read(recordId: PodId): Future[Option[PodRecord]] = async {
    // We uncheck our match because the only failure case is the [[NoNodeException]]. The future fails in all other cases.
    (await(store.read(recordId.value)): @unchecked) match {
      case Success(node) =>
        val record: PodRecord = ??? // deserialize node
        Some(record)
      case Failure(_: NoNodeException) =>
        None
    }
  }

  override def delete(record: PodRecord): Future[PodId] = async {
    val path = await(store.delete(record.podId.value))
    PodId(path)
    // TODO: RecordAlreadyExistsException
  }

  override def update(record: PodRecord): Future[PodId] = async {
    val data = record.toString
    val node = Node(record.podId.value, ByteString(data))
    val path = await(store.update(node))
    PodId(path)
    // TODO: RecordAlreadyExistsException
  }
}
