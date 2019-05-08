package com.mesosphere.utils.persistence

import akka.Done
import com.mesosphere.usi.core.models.PodId
import com.mesosphere.usi.core.models.PodRecord
import com.mesosphere.usi.repository.PodRecordRepository
import com.typesafe.scalalogging.StrictLogging
import scala.collection.mutable
import scala.concurrent.Future

/**
  * A simple in memory implementation of the [[PodRecordRepository]]. It should not be used in production but merely
  * defines a common behavior to all CRUD repositories used by USI.
  */
case class InMemoryPodRecordRepository() extends PodRecordRepository with StrictLogging {
  val data = mutable.Map.empty[PodId, PodRecord]

  override def store(record: PodRecord): Future[Done] = synchronized {
    logger.info(s"Create record ${record.podId}")
    data += record.podId -> record
    Future.successful(Done)
  }

  override def delete(podId: PodId): Future[Done] = synchronized {
    logger.info(s"Delete record $podId")
    data -= podId
    Future.successful(Done)
  }

  override def readAll(): Future[Map[PodId, PodRecord]] = {
    Future.successful(data.toMap)
  }

}
