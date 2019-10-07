package com.mesosphere.usi.core.matching

import com.mesosphere.usi.core.models.{PartialTaskId, RunningPodSpec}
import org.apache.mesos.v1.{Protos => Mesos}

/**
  * Interface for the offer matcher implementations. Given a Mesos [[Mesos.Offer]] and a set of [[RunningPodSpec]]s
  * return a map of all matched PodSpecs with the list of matched Mesos resources.
  */
trait OfferMatcher {

  /**
    * Given a Mesos [[Mesos.Offer]] and a set of [[RunningPodSpec]]s return a map of all matched [[com.mesosphere.usi.core.models.PodId]]s
    * along with their corresponding resources.
    *
    * @param offer Mesos offer
    * @param podSpecs a collection of PodSpecs
    * @return a map of PodSpecs that matched the offer with the list of matched Mesos resouces
    */
  def matchOffer(offer: Mesos.Offer, podSpecs: Iterable[RunningPodSpec]): Map[RunningPodSpec, List[OfferMatcher.ResourceMatch]]
}

object OfferMatcher {

  /**
    * Describes a resource match and tracks if a matched resource is for an executor or a task
    * @param entityKey Optional taskId. If None, then this resource match is for the executor.
    * @param resource The actual resource
    */
  case class ResourceMatch(entityKey: Option[PartialTaskId], resource: Mesos.Resource)

}