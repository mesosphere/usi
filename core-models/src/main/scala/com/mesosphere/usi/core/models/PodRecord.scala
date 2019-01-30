package com.mesosphere.usi.core.models

import java.time.Instant


/**
  * Snapshot of the [[PodSpec]] state that needs to be persisted and can't be obtained from mesos.
  *
  * @param podId id of the pod
  */
case class PodRecord(podId: PodId, launchedAt: Instant)