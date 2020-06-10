package com.mesosphere.usi.core.models.commands

import com.mesosphere.usi.core.models.faultdomain.{DomainFilter, HomeRegionFilter}
import com.mesosphere.usi.core.models.template.RunTemplate
import com.mesosphere.usi.core.models.{PodId, ReservationId, ReservationSpec}
import com.mesosphere.usi.core.models.constraints.AgentFilter
import scala.collection.JavaConverters._

/**
  * Trait which includes all possible events that can describe the evolution of the framework implementation's
  * Specification state.
  *
  * Supported commands:
  * - [[LaunchPod]]
  * - [[KillPod]]
  * - [[ExpungePod]]
  * - [[CreateReservation]]
  */
sealed trait SchedulerCommand

/**
  * Launch the specified pod at-most-once. Frameworks *SHOULD NOT* reuse podIds; if restarting a pod, kill the old and
  * launch a new with a different id.
  *
  * Submitting a pod to the scheduler for launching will result the appropriate offer revive call. Once an offer is
  * received with suitable resources for the pod, a record of the matching pod is created, persisted, prior to the pod
  * being launched.
  *
  * Submitting a launch command for a pod that is already launched (has record of being launched), or is pending launch,
  * is a no-op.
  *
  * @param podId
  * @param runSpec
  * @param domainFilter A filter for default domains. See [[DomainFilter]] for details.
  */
class LaunchPod private (
    val podId: PodId,
    val runSpec: RunTemplate,
    val domainFilter: DomainFilter,
    val agentFilter: Iterable[AgentFilter]
) extends SchedulerCommand

object LaunchPod {

  /**
    * Factory method for instantiating a pod; Scala API
    */
  def apply(podId: PodId, runSpec: RunTemplate): LaunchPod =
    apply(podId, runSpec, HomeRegionFilter)

  /**
    * Factory method for instantiating a pod; Scala API
    */
  def apply(
      podId: PodId,
      runSpec: RunTemplate,
      domainFilter: DomainFilter,
      agentFilter: Iterable[AgentFilter]
  ): LaunchPod =
    new LaunchPod(podId, runSpec, domainFilter, agentFilter)

  /**
    * Factory method for instantiating a pod; Scala API
    */
  def apply(podId: PodId, runSpec: RunTemplate, domainFilter: DomainFilter): LaunchPod =
    apply(podId, runSpec, domainFilter, Nil)

  /**
    * Java constructor for LaunchPod
    */
  def create(
      podId: PodId,
      runSpec: RunTemplate,
      domainFilter: DomainFilter,
      agentFilter: java.lang.Iterable[AgentFilter]
  ): LaunchPod =
    apply(podId, runSpec, domainFilter, agentFilter.asScala)

  /**
    * Java constructor for LaunchPod
    */
  def create(podId: PodId, runSpec: RunTemplate, domainFilter: DomainFilter): LaunchPod =
    apply(podId, runSpec, domainFilter)

  /**
    * Java constructor for LaunchPod
    */
  def create(podId: PodId, runSpec: RunTemplate): LaunchPod =
    apply(podId, runSpec)
}

/**
  * Send a kill for tasks associated with the specified podId.
  *
  * If no task status is known for the specified podId, then the kill is a no-op.
  * @param podId
  */
case class KillPod(podId: PodId) extends SchedulerCommand

/**
  * Delete the pod record and any pending [[PodSpec]] ([[RunningPodSpec]] or [[TerminalPodSpec]]) for a given pod. Does
  * not cause the actual tasks in the pod to be killed.
  *
  * The associated [[PodStatus]], if it exists, will remain until the [[PodStatus]] is either terminal or unreachable.
  *
  * @param podId
  */
case class ExpungePod(podId: PodId) extends SchedulerCommand

/**
  * Not implemented yet; command will be used to make new reservations.
  *
  * @param id
  * @param newState
  */
case class CreateReservation(id: ReservationId, newState: Option[ReservationSpec]) extends SchedulerCommand
