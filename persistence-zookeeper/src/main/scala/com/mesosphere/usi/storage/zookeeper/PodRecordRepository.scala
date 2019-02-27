package com.mesosphere.usi.storage.zookeeper

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import akka.util.ByteString
import com.mesosphere.usi.core.models.{PodId, PodRecord}
import com.mesosphere.usi.repository.{RecordAlreadyExistsException, RecordNotFoundException, PodRecordRepository => PodRecordRepositoryInterface}
import com.mesosphere.usi.storage.zookeeper.PersistenceStore.Node
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.NoNodeException

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

class PodRecordRepository(val store: PersistenceStore) extends PodRecordRepositoryInterface {

  override def create(record: PodRecord): Future[PodId] = {
    val bos = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(bos)
    out.writeObject(record)
    out.flush()
    val data = bos.toByteArray()

    val path = s"/${record.podId.value}"

    val node = Node(path, ByteString(data))
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
      case Success(node) =>
        val ois = new ObjectInputStream(new ByteArrayInputStream(node.data.toArray))
        val record = ois.readObject.asInstanceOf[PodRecord]

        Some(record)
      case Failure(_: NoNodeException) =>
        None
    }
  }

  override def delete(podId: PodId): Future[PodId] = {
    val path = s"/${podId.value}"
    store.delete(path).transform {
      case Success(_) => Success(podId)
      case Failure(_: KeeperException.NoNodeException)  => Failure(RecordNotFoundException(podId.value))
      case Failure(other) => Failure(other)
    }
  }

  override def update(record: PodRecord): Future[PodId] = {
    val path = s"/${record.podId.value}"

    val bos = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(bos)
    out.writeObject(record)
    out.flush()
    val data = bos.toByteArray()

    val node = Node(path, ByteString(data))
    store.update(node).transform {
      case Success(_) => Success(record.podId)
      case Failure(_: KeeperException.NoNodeException)  => Failure(RecordNotFoundException(record.podId.value))
      case Failure(other) => Failure(other)
    }
  }
}
