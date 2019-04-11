package com.mesosphere.usi.repository

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.mesosphere.usi.core.models.{PodId, PodRecord}
import com.typesafe.scalalogging.StrictLogging
import scala.collection.mutable
import scala.concurrent.Future

/**
  * A simple in memory implementation of the [[PodRecordRepository]]. It should not be used in production but merely
  * defines a common behavior to all CRUD repositories used by USI.
  */
case class InMemoryPodRecordRepository() extends PodRecordRepository with StrictLogging {
  val data = mutable.Map.empty[PodId, PodRecord]

  override def store(record: PodRecord): Future[PodId] = synchronized {
    logger.info(s"Create record ${record.podId}")
    data += record.podId -> record
    Future.successful(record.podId)
  }

  override def storeFlow: Flow[PodRecord, PodId, NotUsed] =
    Flow[PodRecord].mapAsync(1)(store)

  override def delete(podId: PodId): Future[Unit] = synchronized {
    logger.info(s"Delete record $podId")
    data -= podId
    Future.unit
  }

  override def deleteFlow: Flow[PodId, Unit, NotUsed] =
    Flow[PodId].mapAsync(1)(delete)

  override def readAll(): Future[Map[PodId, PodRecord]] = {
    Future.successful(data.toMap)
  }

}
