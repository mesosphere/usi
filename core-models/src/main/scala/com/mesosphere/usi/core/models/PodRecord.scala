package com.mesosphere.usi.core.models

import java.time.Instant

/**
  * Describes facts about the result of a launched [[RunningPodSpec]] from the perspective of USI. Used to persist details and
  * can't be obtained from mesos:
  *
  * - Time at which a [[RunningPodSpec]] was launched. (covers the interim period between "we know we told Mesos to launch some
  * thing" and "we heard back the first task status for this launched pod)
  * - Time at which the Pod was first seen as unhealthy or unreachable
  * - etc
  *
  * @param podId id of the pod
  * @param launchedAt time at which we accepted an offer and initiated launch of this pod
  * @param agentId id of Mesos agent from which we accepted offer to launch this pod
  */
case class PodRecord(podId: PodId, launchedAt: Instant, agentId: AgentId)
