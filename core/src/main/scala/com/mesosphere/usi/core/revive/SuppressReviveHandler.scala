package com.mesosphere.usi.core.revive

import akka.NotUsed
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Flow, Source}
import com.mesosphere.mesos.client.MesosCalls
import com.mesosphere.usi.core.models.{PodId, PodSpecUpdatedEvent, RunningPodSpec, TerminalPodSpec}
import com.mesosphere.usi.core.util.RateLimiterFlow
import com.mesosphere.usi.metrics.{Counter, Metrics}
import com.typesafe.scalalogging.StrictLogging
import org.apache.mesos.v1.scheduler.Protos.Call
import org.apache.mesos.v1.{Protos => Mesos}

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

class SuppressReviveHandler(
    initialFrameworkInfo: Mesos.FrameworkInfo,
    frameworkId: Mesos.FrameworkID,
    metrics: Metrics,
    mesosCallFactory: MesosCalls,
    debounceReviveInterval: FiniteDuration)
    extends StrictLogging {

  import SuppressReviveHandler._

  require(defaultRoles.nonEmpty, "initialFramework rolls must be non-empty!")

  private[this] val reviveCountMetric: Counter = metrics.counter("usi.mesos.calls.revive")
  private[this] val suppressCountMetric: Counter = metrics.counter("usi.mesos.calls.suppress")

  private def defaultRoles = initialFrameworkInfo.getRolesList.asScala

  /**
    * Given a stream of PodSpecUpdatedEvent (which gives us the signal of which pods want offers), we emit a snapshot
    * describing all roles to which the framework is subscribed, and the set of podIds which want offers for each role.
    */
  private[revive] def reviveStateFromPodSpecs: Flow[PodSpecUpdatedEvent, PodIdsWantingRoles, NotUsed] =
    Flow[PodSpecUpdatedEvent]
      .scan(ReviveOffersState.empty(defaultRoles)) {
        case (state, PodSpecUpdatedEvent(podId, Some(newPod: RunningPodSpec))) =>
          state.withRoleWanted(podId, newPod.runSpec.role)
        case (state, PodSpecUpdatedEvent(podId, Some(_: TerminalPodSpec))) =>
          state.withoutPodId(podId)
        case (state, PodSpecUpdatedEvent(podId, None)) =>
          state.withoutPodId(podId)
      }
      .map(_.offersWantedState)
      .named("reviveStateFromPodSpecs")

  /**
    * Takes two snapshot PodIdsWantingRoles and compares them, computing the appropriate role directive
    * (IssueUpdateFramework or IssueRevive). We use directives instead of Mesos calls directly so additional metadata
    * can be attached for metrics, and because it is easier to test.
    */
  private[revive] val reviveDirectiveFlow: Flow[PodIdsWantingRoles, RoleDirective, NotUsed] = {
    // By prepending an initial emptyRoles state, we enable the suppressRevive stream to send the initial suppress call,
    // effectively clearing the slate for whatever the revive state was for a previous instance of the Mesos framework
    Flow[Map[Role, Set[PodId]]]
      .prepend(Source.single[PodIdsWantingRoles](Map.empty).named("initialEmptyRolesWantedState"))
      .sliding(2)
      .mapConcat({
        case Seq(lastState, newState) =>
          directivesForDiff(lastState, newState)
        case _ =>
          logger.info(s"Revive stream is terminating")
          Nil
      })
      .named("reviveDirectiveFlow")
  }

  /**
    * Core logic for suppress and revive
    *
    * Revive rate is throttled and debounced using the minReviveOffersInterval as specified above
    *
    * @return
    */
  private[revive] val suppressAndReviveFlow: Flow[PodSpecUpdatedEvent, RoleDirective, NotUsed] = {

    val debouncedReviveState = Flow[PodIdsWantingRoles]
      .buffer(1, OverflowStrategy.dropHead) // While we are back-pressured, we drop older interim frames
      .via(RateLimiterFlow(debounceReviveInterval))
      .named("debouncedReviveState")

    reviveStateFromPodSpecs
      .via(debouncedReviveState)
      .buffer(1, OverflowStrategy.dropHead) // While we are back-pressured, we drop older interim frames
      .via(RateLimiterFlow(debounceReviveInterval))
      .via(reviveDirectiveFlow)
      .log("SuppressRevive handler directive")
  }

  private def frameworkInfoWithRoles(roles: Iterable[String]): Mesos.FrameworkInfo = {
    val b = initialFrameworkInfo.toBuilder
    b.clearRoles()
    b.addAllRoles(roles.asJava)
    b.setId(frameworkId)
    b.build
  }

  private def directiveToMesosCall(directive: RoleDirective): Call = {
    directive match {
      case IssueUpdateFramework(roleState, _, _) =>
        val newInfo = frameworkInfoWithRoles(roleState.keys)
        val suppressedRoles = offersNotWantedRoles(roleState)

        val updateFramework = Call.UpdateFramework
          .newBuilder()
          .setFrameworkInfo(newInfo)
          .addAllSuppressedRoles(suppressedRoles.asJava)
          .build
        mesosCallFactory.newUpdateFramework(updateFramework)

      case IssueRevive(roles) =>
        mesosCallFactory.newRevive(roles)
    }
  }

  private[revive] val reviveSuppressMetrics: Flow[RoleDirective, RoleDirective, NotUsed] =
    Flow[RoleDirective].map {
      case directive @ IssueUpdateFramework(_, newlyRevived, newlySuppressed) =>
        newlyRevived.foreach { _ =>
          reviveCountMetric.increment()
        }
        newlySuppressed.foreach { _ =>
          suppressCountMetric.increment()
        }
        directive

      case directive @ IssueRevive(roles) =>
        roles.foreach { _ =>
          reviveCountMetric.increment()
        }

        directive
    }

  /**
    * Flow which applies the RoleDirectives, recording the appropriate metrics and emitting the corresponding Mesos calls
    */
  val flow: Flow[PodSpecUpdatedEvent, Call, NotUsed] = {
    suppressAndReviveFlow.via(reviveSuppressMetrics).map(directiveToMesosCall).log("SuppressRevive Mesos call")
  }

  private def offersNotWantedRoles(state: PodIdsWantingRoles): Set[Role] =
    state.collect { case (role, podIds) if podIds.isEmpty => role }.toSet

  private def directivesForDiff(lastState: PodIdsWantingRoles, newState: PodIdsWantingRoles): List[RoleDirective] = {
    val directives = List.newBuilder[RoleDirective]

    val newWanted = newState.iterator.collect { case (role, podIds) if podIds.nonEmpty => role }.to[Set]
    val oldWanted = lastState.iterator.collect { case (role, podIds) if podIds.nonEmpty => role }.to[Set]
    val newlyWanted = newWanted -- oldWanted
    val newlyNotWanted = oldWanted -- newWanted
    val rolesChanged = lastState.keySet != newState.keySet

    if (newlyNotWanted.nonEmpty || rolesChanged) {
      directives += IssueUpdateFramework(newState, newlyRevived = newlyWanted, newlySuppressed = newlyNotWanted)
    }

    val rolesNeedingRevive = newState.iterator.collect {
      case (role, podIds) if (podIds -- lastState.getOrElse(role, Set.empty)).nonEmpty => role
    }.to[Set]

    if (rolesNeedingRevive.nonEmpty)
      directives += IssueRevive(rolesNeedingRevive)

    directives.result()
  }
}

object SuppressReviveHandler {
  private[SuppressReviveHandler] type Role = String
  private[SuppressReviveHandler] type PodIdsWantingRoles = Map[String, Set[PodId]]

  private[revive] sealed trait RoleDirective

  /**
    *
    * @param roleState       The data specifying to which roles we should be subscribed, and which should be suppressed
    * @param newlyRevived    Convenience metadata - Set of roles that were previously non-existent or suppressed
    * @param newlySuppressed Convenience metadata - Set of roles that were previously not suppressed
    */
  private[revive] case class IssueUpdateFramework(
      roleState: Map[String, Set[PodId]],
      newlyRevived: Set[String],
      newlySuppressed: Set[String])
      extends RoleDirective

  private[revive] case class IssueRevive(roles: Set[String]) extends RoleDirective
}
