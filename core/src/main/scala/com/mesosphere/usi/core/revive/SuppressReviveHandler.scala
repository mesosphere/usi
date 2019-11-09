package com.mesosphere.usi.core.revive

import akka.NotUsed
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Flow, Source}
import com.mesosphere.mesos.client.MesosCalls
import com.mesosphere.usi.core.models.{PodId, PodSpecUpdatedEvent, RunningPodSpec, TerminalPodSpec}
import com.mesosphere.usi.core.revive.SuppressReviveHandler.Role
import com.mesosphere.usi.core.util.RateLimiterFlow
import com.mesosphere.usi.metrics.{Counter, Metrics}
import com.typesafe.scalalogging.StrictLogging
import org.apache.mesos.v1.{Protos => Mesos}
import org.apache.mesos.v1.scheduler.Protos.Call

import scala.concurrent.duration.FiniteDuration
import scala.collection.JavaConverters._

class SuppressReviveHandler(initialFrameworkInfo: Mesos.FrameworkInfo, metrics: Metrics, mesosCallFactory: MesosCalls, defaultRole: String, minReviveOffersInterval: FiniteDuration) extends StrictLogging {
  import SuppressReviveHandler._

  private[this] val reviveCountMetric: Counter = metrics.counter("usi.mesos.calls.revive")
  private[this] val suppressCountMetric: Counter = metrics.counter("usi.mesos.calls.suppress")

  def reviveStateFromPodSpecs: Flow[PodSpecUpdatedEvent, Map[String, Set[PodId]], NotUsed] = Flow[PodSpecUpdatedEvent].scan(ReviveOffersState.empty(defaultRole)) {
    case (state, PodSpecUpdatedEvent(podId, Some(newPod: RunningPodSpec))) =>
      state.withRoleWanted(podId, newPod.runSpec.role)
    case (state, PodSpecUpdatedEvent(podId, Some(_: TerminalPodSpec))) =>
      state.withoutPodId(podId)
    case (state, PodSpecUpdatedEvent(podId, None)) =>
      state.withoutPodId(podId)
  }
    .map(_.offersWantedState)


  /**
    * Core logic for suppress and revive
    *
    * Receives either instance updates or delay updates; based on the state of those, issues a suppress or a revive call
    *
    * Revive rate is throttled and debounced using minReviveOffersInterval
    *
    * @return
    */
  private[revive] val suppressAndReviveFlow: Flow[PodSpecUpdatedEvent, RoleDirective, NotUsed] = {

    reviveStateFromPodSpecs
      .buffer(1, OverflowStrategy.dropHead) // While we are back-pressured, we drop older interim frames
      .via(RateLimiterFlow(minReviveOffersInterval))
      .via(reviveDirectiveFlow)
      .map(l => { logger.info(s"Issuing following suppress/revive directives: = ${l}"); l })
  }

  private[revive] val reviveDirectiveFlow: Flow[Map[Role, Set[PodId]], RoleDirective, NotUsed] = {
    Flow[Map[Role, Set[PodId]]]
      .prepend(Source.single(Map.empty[Role, Set[PodId]]))
      .sliding(2)
      .mapConcat({
        case Seq(lastState, newState) =>
          directivesForDiff(lastState, newState)
        case _ =>
          logger.info(s"Revive stream is terminating")
          Nil
      })
  }

  private def frameworkInfoWithRoles(roles: Iterable[String]): Mesos.FrameworkInfo = {
    val b = initialFrameworkInfo.toBuilder
    b.clearRoles()
    b.addAllRoles(roles.asJava)
    b.build
  }

  private def directiveToMesosCall(directive: RoleDirective): Call = {
    directive match {
      case UpdateFramework(roleState, _, _) =>
        val newInfo = frameworkInfoWithRoles(roleState.keys)
        val suppressedRoles = offersNotWantedRoles(roleState)

        val updateFramework = Call.UpdateFramework.newBuilder()
          .setFrameworkInfo(newInfo)
          .addAllSuppressedRoles(suppressedRoles.asJava)
          .build
        mesosCallFactory.newUpdateFramework(updateFramework)

      case IssueRevive(roles) =>
        mesosCallFactory.newRevive(roles)
    }
  }

  private [revive] val reviveSuppressMetrics: Flow[RoleDirective, RoleDirective, NotUsed] = Flow[RoleDirective].map {
    case directive @ UpdateFramework(_, newlyRevived, newlySuppressed) =>
      newlyRevived.foreach { _ => reviveCountMetric.increment() }
      newlySuppressed.foreach { _ => suppressCountMetric.increment() }
      directive

    case directive @ IssueRevive(roles) =>
      roles.foreach { _ => reviveCountMetric.increment() }

      directive
  }

  /**
    * Flow which applies the RoleDirectives, recording the appropriate metrics and emitting the corresponding Mesos calls
    */
  val flow: Flow[PodSpecUpdatedEvent, Call, NotUsed] = {
    suppressAndReviveFlow.via(reviveSuppressMetrics).map(directiveToMesosCall)
  }

  private def offersNotWantedRoles(state: Map[Role, Set[PodId]]): Set[Role] =
    state.collect { case (role, podIds) if podIds.isEmpty => role }.toSet

  private def directivesForDiff(lastState: Map[Role, Set[PodId]], newState: Map[Role, Set[PodId]]): List[RoleDirective] = {
    val directives = List.newBuilder[RoleDirective]

    val newWanted = newState.iterator.collect { case (role, podIds) if podIds.nonEmpty => role }.to[Set]
    val oldWanted = lastState.iterator.collect { case (role, podIds) if podIds.nonEmpty => role }.to[Set]
    val newlyWanted = newWanted -- oldWanted
    val newlyNotWanted = oldWanted -- newWanted
    val rolesChanged = lastState.keySet != newState.keySet

    if (newlyNotWanted.nonEmpty || rolesChanged) {
      directives += UpdateFramework(newState, newlyRevived = newlyWanted, newlySuppressed = newlyNotWanted)
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
  type Role = String

  private[revive] sealed trait RoleDirective

  /**
    *
    * @param roleState       The data specifying to which roles we should be subscribed, and which should be suppressed
    * @param newlyRevived    Convenience metadata - Set of roles that were previously non-existent or suppressed
    * @param newlySuppressed Convenience metadata - Set of roles that were previously not suppressed
    */
  private[revive] case class UpdateFramework(
      roleState: Map[String, Set[PodId]],
      newlyRevived: Set[String],
      newlySuppressed: Set[String])
      extends RoleDirective

  private[revive] case class IssueRevive(roles: Set[String]) extends RoleDirective
}
