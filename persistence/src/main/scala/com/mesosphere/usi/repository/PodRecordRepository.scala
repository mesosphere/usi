package com.mesosphere.usi.repository

import com.mesosphere.usi.core.models.{PodId, PodRecord}

/**
  * Repository for [[PodRecord]].
  */
trait PodRecordRepository extends RecordRepository[PodRecord, PodId]
