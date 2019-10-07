package com.mesosphere.usi.core.matching
import com.mesosphere.usi.core.models.RunTemplateLike.KeyedResourceRequirement
import com.mesosphere.usi.core.models.resources.ResourceType
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
      matchedResources: List[OfferMatcher.ResourceMatch],
      resourceRequirements: List[KeyedResourceRequirement])
    : Option[(List[OfferMatcher.ResourceMatch], Map[ResourceType, Seq[Mesos.Resource]])] = {

    resourceRequirements match {
      case Nil =>
        Some((matchedResources, remainingResources))
      case (KeyedResourceRequirement(entityKey, req)) :: rest =>
        ResourceMatcher.matchAndConsume(req, remainingResources.getOrElse(req.resourceType, Nil)) match {
          case Some(matchResult) =>
            maybeMatchPodSpec(
              remainingResources.updated(req.resourceType, matchResult.remainingResource),
              matchResult.matchedResources.toList.map(OfferMatcher.ResourceMatch(entityKey, _)) ++ matchedResources,
              rest)
          case None =>
            // we didn't match
            None
        }
    }
  }

  @tailrec private def matchPodSpecsTaskRecords(
      remainingResources: Map[ResourceType, Seq[Mesos.Resource]],
      result: Map[RunningPodSpec, List[OfferMatcher.ResourceMatch]],
      pendingLaunchPodSpecs: List[RunningPodSpec]): Map[RunningPodSpec, List[OfferMatcher.ResourceMatch]] = {

    pendingLaunchPodSpecs match {
      case Nil =>
        result

      case podSpec :: rest =>
        maybeMatchPodSpec(remainingResources, Nil, podSpec.runSpec.allResourceRequirements) match {
          case Some((matchedResources, newRemainingResources)) =>
            matchPodSpecsTaskRecords(newRemainingResources, result.updated(podSpec, matchedResources), rest)
          case None =>
            matchPodSpecsTaskRecords(remainingResources, result, rest)
        }
    }
  }

  override def matchOffer(
      offer: Mesos.Offer,
      podSpecs: Iterable[RunningPodSpec]): Map[RunningPodSpec, List[OfferMatcher.ResourceMatch]] = {
    val groupedResources = offer.getResourcesList.asScala.groupBy(r => ResourceType.fromName(r.getName))
    matchPodSpecsTaskRecords(groupedResources, Map.empty, podSpecs.toList)
  }
}
