package com.mesosphere.usi.core.matching
import com.mesosphere.usi.core.models.template.RunTemplate.KeyedResourceRequirement
import com.mesosphere.usi.core.models.resources.ResourceType
import com.mesosphere.usi.core.models.{RunningPodSpec, TaskName}
import org.apache.mesos.v1.{Protos => Mesos}

import scala.annotation.tailrec
import scala.collection.JavaConverters._

class OfferMatcher(masterDomainInfo: Mesos.DomainInfo) {
  @tailrec private def maybeMatchResourceRequirements(
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
            maybeMatchResourceRequirements(
              remainingResources.updated(req.resourceType, matchResult.remainingResource),
              matchResult.matchedResources.toList.map(OfferMatcher.ResourceMatch(entityKey, _)) ++ matchedResources,
              rest
            )
          case None =>
            // we didn't match
            None
        }
    }
  }

  @tailrec private def matchPodSpecsTaskRecords(
      originalOffer: Mesos.Offer,
      remainingResources: Map[ResourceType, Seq[Mesos.Resource]],
      result: Map[RunningPodSpec, List[OfferMatcher.ResourceMatch]],
      pendingLaunchPodSpecs: List[RunningPodSpec]): Map[RunningPodSpec, List[OfferMatcher.ResourceMatch]] = {

    pendingLaunchPodSpecs match {
      case Nil =>
        result

      case podSpec :: rest =>
        Some(podSpec)
          .filter(_.domainFilter(masterDomainInfo, originalOffer.getDomain))
          .filter(_.runSpec.role == originalOffer.getAllocationInfo.getRole)
          .flatMap { ps =>
            maybeMatchResourceRequirements(remainingResources, Nil, ps.runSpec.allResourceRequirements)
          } match {
          case Some((matchedResources, newRemainingResources)) =>
            matchPodSpecsTaskRecords(
              originalOffer,
              newRemainingResources,
              result.updated(podSpec, matchedResources),
              rest)
          case None =>
            matchPodSpecsTaskRecords(originalOffer, remainingResources, result, rest)
        }
    }
  }

  def matchOffer(
      offer: Mesos.Offer,
      podSpecs: Iterable[RunningPodSpec]): Map[RunningPodSpec, List[OfferMatcher.ResourceMatch]] = {
    val groupedResources = offer.getResourcesList.asScala.groupBy(r => ResourceType.fromName(r.getName))
    matchPodSpecsTaskRecords(offer, groupedResources, Map.empty, podSpecs.toList)
  }
}

/**
  * Simple first-come-first-served offer matcher implementation which tries to match [[RunningPodSpec]] one after the other,
  * consuming resources from the [[Mesos.Offer]] (should there be enough). Matcher goes over all passed specs *not*
  * breaking out on the first unmatched PodSpec.
  */
object OfferMatcher {

  /**
    * Describes a resource match and tracks if a matched resource is for an executor or a task
    *
    * @param entityKey Optional taskId. If None, then this resource match is for the executor.
    * @param resource  The actual resource
    */
  case class ResourceMatch(entityKey: Option[TaskName], resource: Mesos.Resource)
}
