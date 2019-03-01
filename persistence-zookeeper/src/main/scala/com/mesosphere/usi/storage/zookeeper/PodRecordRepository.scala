package com.mesosphere.usi.storage.zookeeper

import java.io.ByteArrayInputStream
import java.time.Instant

import akka.util.ByteString
import com.google.protobuf.Timestamp
import com.mesosphere.usi.core.models.{AgentId, PodId, PodRecord}
import com.mesosphere.usi.repository.{
  RecordAlreadyExistsException,
  RecordNotFoundException,
  PodRecordRepository => PodRecordRepositoryInterface
}
import com.mesosphere.usi.storage.zookeeper.PersistenceStore.Node
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.NoNodeException

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

class PodRecordRepository(val store: PersistenceStore) extends PodRecordRepositoryInterface {

  private def serialize(record: PodRecord): ByteString = {
    val launchedAt = Timestamp
      .newBuilder()
      .setSeconds(record.launchedAt.getEpochSecond)
      .setNanos(record.launchedAt.getNano)
      .build()
    val data = ZooKeeperStorageModel.PodRecord
      .newBuilder()
      .setId(record.podId.value)
      .setLaunchedAt(launchedAt)
      .setAgentId(record.agentId.value)
      .build()
      .toByteArray()

    ByteString(data)
  }

  private def deserialize(bytes: ByteString): PodRecord = {
    val data = ZooKeeperStorageModel.PodRecord.parseFrom(new ByteArrayInputStream(bytes.toArray))
    val launchedAt = Instant.ofEpochSecond(data.getLaunchedAt().getSeconds(), data.getLaunchedAt().getNanos().toInt)
    PodRecord(PodId(data.getId()), launchedAt, AgentId(data.getAgentId()))
  }

  override def create(record: PodRecord): Future[PodId] = {

    val path = s"/${record.podId.value}"

    val node = Node(path, serialize(record))
    store.create(node).transform {
      case Success(_) => Success(record.podId)
      case Failure(_: KeeperException.NodeExistsException) => Failure(RecordAlreadyExistsException(record.podId.value))
      case Failure(other) => Failure(other)
    }
  }

  override def read(podId: PodId): Future[Option[PodRecord]] = async {

    val path = s"/${podId.value}"

    // We uncheck our match because the only failure case is the [[NoNodeException]]. The future fails in all other cases.
    (await(store.read(path)): @unchecked) match {
      case Success(node) => Some(deserialize(node.data))
      case Failure(_: NoNodeException) => None
    }
  }

  override def delete(podId: PodId): Future[PodId] = {
    val path = s"/${podId.value}"
    store.delete(path).transform {
      case Success(_) => Success(podId)
      case Failure(_: KeeperException.NoNodeException) => Failure(RecordNotFoundException(podId.value))
      case Failure(other) => Failure(other)
    }
  }

  override def update(record: PodRecord): Future[PodId] = {
    val path = s"/${record.podId.value}"

    val node = Node(path, serialize(record))
    store.update(node).transform {
      case Success(_) => Success(record.podId)
      case Failure(_: KeeperException.NoNodeException) => Failure(RecordNotFoundException(record.podId.value))
      case Failure(other) => Failure(other)
    }
  }
}
