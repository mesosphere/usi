package com.mesosphere.mesos.examples
import com.typesafe.scalalogging.StrictLogging
import org.apache.mesos.v1.Protos.Offer

import scala.collection.JavaConverters._

object OfferMatcher extends StrictLogging {

  /**
    * Simple FCFS offer matcher that looks for the first offer that satisfies task's resources and returns it.
    *
    * @param spec task's spec
    * @param offers list of offers
    * @return either [[Some]](offer) with the first matching offer or [[None]] if no match was found
    */
  def matchOffer(spec: Spec, offers: Seq[Offer]): Option[Offer] = {
    offers.find { offer =>
      val resourceList = offer.getResourcesList.asScala

      logger.debug(s""" Trying to match:
        | spec: $spec
        | offer Id: ${offer.getId.getValue}
        | scalar resources: ${resourceList.filter(_.hasScalar).map(r => s"${r.getName}:${r.getScalar.getValue}")}
        |
      """.stripMargin)

      resourceList.exists(resource => resource.getName == "cpus" && resource.getScalar.getValue >= spec.cpus) &&
      resourceList.exists(resource => resource.getName == "mem" && resource.getScalar.getValue >= spec.mem)
    }
  }
}
