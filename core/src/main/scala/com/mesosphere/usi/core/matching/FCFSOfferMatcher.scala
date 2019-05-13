package com.mesosphere.usi.core.matching
import com.mesosphere.usi.core.models.resources.{ResourceRequirement, ResourceType}
import com.mesosphere.usi.core.models.RunningPodSpec
import org.apache.mesos.v1.{Protos => Mesos}

import scala.annotation.tailrec
import scala.collection.JavaConverters._

/**
  * Simple first-come-first-served offer matcher implementation which tries to match [[RunningPodSpec]] one after the other,
  * consuming resources from the [[Mesos.Offer]] (should there be enough). Matcher goes over all passed specs *not*
  * breaking out on the first unmatched PodSpec.
  */
class FCFSOfferMatcher extends OfferMatcher {

  @tailrec private def maybeMatchPodSpec(
      remainingResources: Map[ResourceType, Seq[Mesos.Resource]],
      matchedResources: List[Mesos.Resource],
      resourceRequirements: List[ResourceRequirement])
    : Option[(List[Mesos.Resource], Map[ResourceType, Seq[Mesos.Resource]])] = {

    resourceRequirements match {
      case Nil =>
        Some((matchedResources, remainingResources))
      case req :: rest =>
        ResourceMatcher.matchAndConsume(req, remainingResources.getOrElse(req.resourceType, Nil)) match {
          case Some(matchResult) =>
            maybeMatchPodSpec(
              remainingResources.updated(req.resourceType, matchResult.remainingResource),
              matchResult.matchedResources.toList ++ matchedResources,
              rest)
          case None =>
            // we didn't match
            None
        }
    }
  }

  @tailrec private def matchPodSpecsTaskRecords(
      offer: Mesos.Offer,
      remainingResources: Map[ResourceType, Seq[Mesos.Resource]],
      result: Map[RunningPodSpec, List[Mesos.Resource]],
      pendingLaunchPodSpecs: List[RunningPodSpec]): Map[RunningPodSpec, List[Mesos.Resource]] = {

    pendingLaunchPodSpecs match {
      case Nil =>
        result

      case podSpec :: rest =>
        maybeMatchPodSpec(remainingResources, Nil, podSpec.runSpec.resourceRequirements.toList) match {
          case Some((matchedResources, newRemainingResources)) =>
            matchPodSpecsTaskRecords(offer, newRemainingResources, result.updated(podSpec, matchedResources), rest)
          case None =>
            matchPodSpecsTaskRecords(offer, remainingResources, result, rest)
        }
    }
  }

  override def matchOffer(
      offer: Mesos.Offer,
      podSpecs: Iterable[RunningPodSpec]): Map[RunningPodSpec, List[Mesos.Resource]] = {
    val groupedResources = offer.getResourcesList.asScala.groupBy(r => ResourceType.fromName(r.getName))
    matchPodSpecsTaskRecords(offer, groupedResources, Map.empty, podSpecs.toList)
  }
}
