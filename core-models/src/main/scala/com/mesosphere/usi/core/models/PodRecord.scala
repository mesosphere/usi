package com.mesosphere.usi.core.models

import java.time.Instant

/**
  * Snapshot of the [[PodSpec]] state that needs to be persisted and can't be obtained from mesos.
  *
  * @param podId id of the pod
  * @param launchedAt time at which we accepted an offer and initiated launch of this pod
  * @param agentId id of Mesos agent from which we accepted offer to launch this pod
  */
case class PodRecord(podId: PodId, launchedAt: Instant, agentId: AgentId)
