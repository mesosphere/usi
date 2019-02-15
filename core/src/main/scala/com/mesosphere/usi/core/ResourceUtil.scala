package com.mesosphere.usi.core

import com.typesafe.scalalogging.StrictLogging
import org.apache.mesos.v1.Protos.Resource.DiskInfo.Source
import org.apache.mesos.v1.Protos.Resource.{DiskInfo, ReservationInfo}
import org.apache.mesos.v1.{Protos => Mesos}
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

object ResourceUtil extends StrictLogging {

  /**
    * The resources in launched tasks, should
    * be consumed from resources in the offer with the same [[ResourceMatchKey]].
    */
  private[this] case class ResourceMatchKey(
      name: String,
      reservations: Seq[ReservationInfo],
      disk: Option[DiskInfo])

  private[this] object ResourceMatchKey {
    def apply(resource: Mesos.Resource): ResourceMatchKey = {
      val reservations = resource.getReservationsList.asScala
      val disk = if (resource.hasDisk) Some(resource.getDisk) else None
      // role is included in the ResourceMatchKey by the reservation.
      ResourceMatchKey(resource.getName, reservations.toList, disk)
    }
  }

  /**
    * Decrements the scalar resource by amount
    *
    */
  def consumeScalarResource(resource: Mesos.Resource, amount: Double): Option[Mesos.Resource] = {
    require(resource.getType == Mesos.Value.Type.SCALAR)
    val isMountDiskResource =
      resource.hasDisk && resource.getDisk.hasSource &&
        (resource.getDisk.getSource.getType == Source.Type.MOUNT)

    // TODO(jdef) would be nice to use fixed precision like Mesos does for scalar math
    val leftOver: Double = resource.getScalar.getValue - amount
    if (leftOver <= 0 || isMountDiskResource) {
      None
    } else {
      Some(
        resource.toBuilder
          .setScalar(
            Mesos.Value.Scalar
              .newBuilder()
              .setValue(leftOver))
          .build())
    }
  }

  /**
    * Deduct usedResource from resource. If nothing is left, None is returned.
    */
  def consumeResource(resource: Mesos.Resource, usedResource: Mesos.Resource): Option[Mesos.Resource] = {
    require(resource.getType == usedResource.getType)

    def deductRange(baseRange: Mesos.Value.Range, usedRange: Mesos.Value.Range): Seq[Mesos.Value.Range] = {
      if (baseRange.getEnd < usedRange.getBegin || baseRange.getBegin > usedRange.getEnd) {
        // baseRange completely before or after usedRange
        Seq(baseRange)
      } else {
        val rangeBefore: Option[Mesos.Value.Range] =
          if (baseRange.getBegin < usedRange.getBegin)
            Some(baseRange.toBuilder.setEnd(usedRange.getBegin - 1).build())
          else
            None

        val rangeAfter: Option[Mesos.Value.Range] =
          if (baseRange.getEnd > usedRange.getEnd)
            Some(baseRange.toBuilder.setBegin(usedRange.getEnd + 1).build())
          else
            None

        Seq(rangeBefore, rangeAfter).flatten
      }
    }

    def consumeRangeResource: Option[Mesos.Resource] = {
      val usedRanges = usedResource.getRanges.getRangeList
      val baseRanges = resource.getRanges.getRangeList

      // FIXME: too expensive?
      val diminished = baseRanges.asScala.flatMap { baseRange =>
        usedRanges.asScala.foldLeft(Seq(baseRange)) {
          case (result, used) =>
            result.flatMap(deductRange(_, used))
        }
      }

      val rangesBuilder = Mesos.Value.Ranges.newBuilder()
      diminished.foreach(rangesBuilder.addRange)

      val result = resource.toBuilder
        .setRanges(rangesBuilder)
        .build()

      if (result.getRanges.getRangeCount > 0)
        Some(result)
      else
        None
    }

    def consumeSetResource: Option[Mesos.Resource] = {
      val baseSet: Set[String] = resource.getSet.getItemList.asScala.toSet
      val consumedSet: Set[String] = usedResource.getSet.getItemList.asScala.toSet
      require(consumedSet subsetOf baseSet, s"$consumedSet must be subset of $baseSet")

      val resultSet: Set[String] = baseSet -- consumedSet

      if (resultSet.nonEmpty)
        Some(
          resource.toBuilder
            .setSet(Mesos.Value.Set.newBuilder().addAllItem(resultSet.asJava))
            .build()
        )
      else
        None
    }

    resource.getType match {
      case Mesos.Value.Type.SCALAR => consumeScalarResource(resource, usedResource.getScalar.getValue)
      case Mesos.Value.Type.RANGES => consumeRangeResource
      case Mesos.Value.Type.SET => consumeSetResource

      case unexpectedResourceType: Mesos.Value.Type =>
        logger.warn("unexpected resourceType {} for resource {}", Seq(unexpectedResourceType, resource.getName): _*)
        // we don't know the resource, thus we consume it completely
        None
    }
  }

  /**
    * Deduct usedResources from resources by matching them by name and role.
    */
  def consumeResources(resources: Seq[Mesos.Resource], usedResources: Seq[Mesos.Resource]): Seq[Mesos.Resource] = {
    val usedResourceMap: Map[ResourceMatchKey, Seq[Mesos.Resource]] =
      usedResources.groupBy(ResourceMatchKey(_))

    resources.flatMap { resource: Mesos.Resource =>
      usedResourceMap.get(ResourceMatchKey(resource)) match {
        case Some(usedResources: Seq[Mesos.Resource]) =>
          usedResources.foldLeft(Some(resource): Option[Mesos.Resource]) {
            case (Some(resource), usedResource) =>
              if (resource.getType != usedResource.getType) {
                logger.warn(
                  "Different resource types for resource {}: {} and {}",
                  resource.getName,
                  resource.getType,
                  usedResource.getType)
                None
              } else
                try ResourceUtil.consumeResource(resource, usedResource)
                catch {
                  case NonFatal(e) =>
                    logger.warn("while consuming {} of type {}", resource.getName, resource.getType, e)
                    None
                }

            case (None, _) => None
          }
        case None => // if the resource isn't used, we keep it
          Some(resource)
      }
    }
  }

  /**
    * Deduct usedResources from resources in the offer.
    */
  def consumeResourcesFromOffer(offer: Mesos.Offer, usedResources: Seq[Mesos.Resource]): Mesos.Offer = {
    val offerResources: Seq[Mesos.Resource] = offer.getResourcesList.asScala
    val leftOverResources = ResourceUtil.consumeResources(offerResources, usedResources)
    offer.toBuilder.clearResources().addAllResources(leftOverResources.asJava).build()
  }

  def displayResource(resource: Mesos.Resource, maxRanges: Int): String = {
    def rangesToString(ranges: Seq[Mesos.Value.Range]): String = {
      ranges.map { range =>
        s"${range.getBegin}->${range.getEnd}"
      }.mkString(",")
    }

    val role = resource.getReservationsList().asScala.lastOption.map(_.getRole).getOrElse("*")
    val principal = resource.getReservationsList().asScala.lastOption.map(_.getPrincipal)

    lazy val resourceName = {
      val principalString = principal.map { p => s", RESERVED for ${p}" }.getOrElse("")
      val diskString =
        if (resource.hasDisk && resource.getDisk.hasPersistence)
          s", diskId ${resource.getDisk.getPersistence.getId}"
        else
          ""

      s"${resource.getName}(${role}$principalString$diskString)"
    }

    resource.getType match {
      case Mesos.Value.Type.SCALAR => s"$resourceName ${resource.getScalar.getValue}"
      case Mesos.Value.Type.RANGES =>
        s"$resourceName ${
          val ranges = resource.getRanges.getRangeList.asScala
          if (ranges.size > maxRanges)
            s"${rangesToString(ranges.take(maxRanges))} ... (${ranges.size - maxRanges} more)"
          else
            rangesToString(ranges)
        }"
      case _: Mesos.Value.Type => resource.toString
    }
  }

  def displayResources(resources: Seq[Mesos.Resource], maxRanges: Int): String = {
    resources.map(displayResource(_, maxRanges)).mkString("; ")
  }
}
