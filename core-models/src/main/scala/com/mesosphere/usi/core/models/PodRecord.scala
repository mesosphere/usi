package com.mesosphere.usi.core.models

import java.time.Instant


case class PodRecord(sequenceNr: Long,
                     podId: PodId,
                     launchedAt: Instant,
                     unserviceable: Option[UnserviceableDetails])

case class UnserviceableDetails(reason: UnserviceableReason, since: Instant)

sealed trait UnserviceableReason
case object Unhealthy extends UnserviceableReason
case object Unreachable extends UnserviceableReason

