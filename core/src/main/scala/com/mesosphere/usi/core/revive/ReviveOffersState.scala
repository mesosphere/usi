package com.mesosphere.usi.core.revive

import com.mesosphere.usi.core.models.PodId

private[revive] class ReviveOffersState protected (
    val offersWantedState: Map[String, Set[PodId]],
    val podIdRoles: Map[PodId, String]) {
  def withRoleWanted(podId: PodId, role: String): ReviveOffersState = {
    new ReviveOffersState(
      offersWantedState.updated(role, offersWantedState(role) + podId),
      podIdRoles.updated(podId, role))
  }

  def withoutPodId(podId: PodId): ReviveOffersState = {
    podIdRoles.get(podId) match {
      case Some(role) =>
        new ReviveOffersState(
          offersWantedState.updated(role, offersWantedState(role) + podId),
          podIdRoles.updated(podId, role))
      case None =>
        this
    }
  }
}

object ReviveOffersState {
  def empty(defaultRole: String) =
    new ReviveOffersState(offersWantedState = Map(defaultRole -> Set.empty), podIdRoles = Map.empty)
}
