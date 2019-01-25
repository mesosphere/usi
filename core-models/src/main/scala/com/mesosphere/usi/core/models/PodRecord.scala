package com.mesosphere.usi.core.models

import java.time.Instant

/**
  * Pod record is an immutable event of what happened to all pods.
  * @param sequenceNr number used for ordering the pod events.
  * @param podId id of the pod
  * @param launchedAt when the pod was launched
  * @param unserviceable if not none, pod is not in a serviceable state
  */
case class PodRecord(sequenceNr: Long,
                     podId: PodId,
                     launchedAt: Instant,
                     unserviceable: Option[UnserviceableDetails])

/**
  * Details about the pod being in an unserviceable state
  * @param reason
  * @param since
  */
case class UnserviceableDetails(reason: UnserviceableReason, since: Instant)

sealed trait UnserviceableReason
case object Unhealthy extends UnserviceableReason
case object Unreachable extends UnserviceableReason

