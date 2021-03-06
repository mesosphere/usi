package com.mesosphere.usi.core.models

import com.mesosphere.usi.core.models.constraints.AgentFilter
import com.mesosphere.usi.core.models.faultdomain.{DomainFilter, HomeRegionFilter}
import com.mesosphere.usi.core.models.template.RunTemplate

/**
  * Scheduler logic state indicating that some action still needs to be done some pod [[RunningPodSpec]] or
  * [[TerminalPodSpec]]
  */
sealed trait PodSpec {
  val id: PodId
  def shouldBeTerminal: Boolean
}

/**
  * Used by the scheduler to track that a pod needs to be killed
  * @param id
  */
case class TerminalPodSpec(id: PodId) extends PodSpec {
  override def shouldBeTerminal: Boolean = true
}

/**
  * Used by the scheduler to track that a pod pod should be launched (and, isn't yet.)
  *
  * Pods are launched at-most-once.
  *
  * @param id Id of the pod
  * @param runSpec WIP the thing to run, and resource requirements, etc.
  * @param domainFilter  Specifies on which [[http://mesos.apache.org/documentation/latest/fault-domains/ fault domains]]
  *                      the pod will be launched.
  */
case class RunningPodSpec(
    id: PodId,
    runSpec: RunTemplate,
    domainFilter: DomainFilter = HomeRegionFilter,
    agentFilters: Iterable[AgentFilter] = Nil
) extends PodSpec {
  override def shouldBeTerminal: Boolean = false
}
