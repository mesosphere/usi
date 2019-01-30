package com.mesosphere.usi.repository

import com.mesosphere.usi.core.models.{ReservationId, ReservationRecord}

/**
  * Repository for [[ReservationRecord]].
  */
trait ReservationRecordRepository extends RecordRepository {
  override type Record = ReservationRecord
  override type RecordId = ReservationId
}
