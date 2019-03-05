package com.mesosphere.usi.core.matching

import com.mesosphere.usi.core.ResourceUtil
import com.mesosphere.usi.core.matching.RangeResource._
import com.mesosphere.usi.core.models.{ResourceMatchResult, ResourceRequirement, ResourceType}
import com.mesosphere.usi.core.protos.ProtoBuilders
import org.apache.mesos.v1.Protos

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.Random

/**
  * Represents requirement for Mesos resource of type Range.
  * You can either request static values (by providing concrete value) or dynamic values (by providing 0).
  * Dynamic values are then selected from the offered ranges. If random implementation is passed in, these ranges are randomized.
  * For more information about randomization see [[lazyRandomValuesFromRanges()]]
  *
  * @param requestedValues values pod wants to consume on the given ranges
  * @param resourceType name of resource (e.g. ports)
  * @param random when requesting dynamic values, you can provide Random implementation if you want dynamic values to be randomized
  */
case class RangeResource(requestedValues: Seq[Int], resourceType: ResourceType, random: Random = Random)
    extends ResourceRequirement {
  override def description: String = s"$resourceType:[${requestedValues.mkString(",")}]"

  override def matchAndConsume(resources: Seq[Protos.Resource]): Option[ResourceMatchResult] = {
    matchAndConsumeIter(Nil, resources.toList)
  }

  /**
    * Iterates over given resources keeping in track unmatched resources.
    * In the end produces ResourceMatchResult if we were able to find a match on the given resources.
    *
    * @param unmatchedResources already processed resources that did not contain resources we are trying to match
    * @param remainingResources resources we still need to process when looking for resource match
    * @return None if no match was possible, ResourceMatchResult if we were able to consume requested resources
    */
  @tailrec private def matchAndConsumeIter(
      unmatchedResources: List[Protos.Resource],
      remainingResources: List[Protos.Resource]): Option[ResourceMatchResult] = {

    remainingResources match {
      case Nil =>
        None
      case next :: rest =>
        tryConsumeValuesFromResource(requestedValues, next) match {
          case Nil =>
            matchAndConsumeIter(next :: unmatchedResources, rest)
          case consumedResources =>
            Some(
              ResourceMatchResult(
                consumedResources,
                unmatchedResources ++ rest ++ ResourceUtil.consumeResources(Seq(next), consumedResources)))
        }
    }
  }

  /**
    * Tries to match and consume values from a given resource
    * @param values values we want to consume
    * @param resource mesos resource to match agains
    * @return final list of mesos resources created after consuming requested values, empty if match was not possible
    */
  private def tryConsumeValuesFromResource(values: Seq[Int], resource: Protos.Resource): Seq[Protos.Resource] = {
    val offeredRanges = parseResourceToRanges(resource)
    if (offeredRanges.isEmpty || values.isEmpty) {
      return Seq.empty
    }

    // non-dynamic values
    val staticRequestedValues = values.filter(v => v != 0).toSet
    val availableForDynamicAssignment: Iterator[Int] =
      lazyRandomValuesFromRanges(offeredRanges, random).filter(v => !staticRequestedValues(v))

    val matchResult = values.map {
      case v if v == RandomValue && !availableForDynamicAssignment.hasNext =>
        // need dynamic value but no more available
        ValueNotAvailable
      case v if v == RandomValue && availableForDynamicAssignment.hasNext =>
        // pick next available dynamic value
        ValueMatched(availableForDynamicAssignment.next())
      case v if offeredRanges.exists(_.contains(v)) =>
        // static value
        ValueMatched(v)
      case _ =>
        ValueNotAvailable
    }

    if (matchResult.contains(ValueNotAvailable)) {
      Seq.empty
    } else {
      createMesosResource(resource, matchResult.collect { case ValueMatched(v) => v }.toSeq, resourceType)
    }
  }

  /**
    * Converts given Resource to sequence of ranges
    * If given resource is not resource of given name and type range, it will return empty sequence
    * @param resource mesos resource definition
    * @return list of parsed ranges in that resource
    */
  private def parseResourceToRanges(resource: Protos.Resource): Seq[MesosRange] = {
    if (resource.getName != resourceType.name) {
      Seq.empty
    } else {
      val rangeInResource = resource.getRanges.getRangeList.asScala
      rangeInResource.map { range =>
        MesosRange(range.getBegin.toInt, range.getEnd.toInt)
      }
    }
  }

  /**
    * We want to make it less likely that we are reusing the same dynamic value for tasks of different pods.
    * This is important especially for ports.
    * This way we allow load balancers to reconfigure before reusing the same ports.
    *
    * Therefore we want to choose dynamic ports randomly from all the offered port ranges.
    * We want to use consecutive ports to avoid excessive range fragmentation.
    *
    * The implementation idea:
    *
    * * Randomize the order of the offered ranges.
    * * Now treat the ports contained in the ranges as one long sequence of ports.
    * * We randomly choose an index where we want to start assigning dynamic ports in that sequence. When
    *   we hit the last offered port with wrap around and start offering the ports at the beginning
    *   of the sequence up to (excluding) the port index we started at.
    * * The next range is determined on demand. That's why an iterator is returned.
    */
  private def lazyRandomValuesFromRanges(ranges: Seq[MesosRange], rand: Random): Iterator[Int] = {
    val numberOfOfferedValues = ranges.map(_.size).sum

    if (numberOfOfferedValues == 0) {
      return Iterator.empty
    }

    def findStartValue(shuffled: IndexedSeq[MesosRange], startValueIdx: Int): (Int, Int) = {
      var startValueIdxOfCurrentRange = 0
      val rangeIdx = shuffled.indexWhere {
        case range: MesosRange if startValueIdxOfCurrentRange + range.size > startValueIdx =>
          true
        case range: MesosRange =>
          startValueIdxOfCurrentRange += range.size
          false
      }

      (rangeIdx, startValueIdx - startValueIdxOfCurrentRange)
    }

    val shuffled = rand.shuffle(ranges).toIndexedSeq
    val startValueIdx = rand.nextInt(numberOfOfferedValues)
    val (rangeIdx, valueInRangeIdx) = findStartValue(shuffled, startValueIdx)
    val startRangeOrig = shuffled(rangeIdx)

    val startRange = startRangeOrig.drop(valueInRangeIdx)

    // These are created on demand if necessary
    def afterStartRange: Iterator[Int] =
      shuffled.slice(rangeIdx + 1, shuffled.length).iterator.flatMap(_.iterator)
    def beforeStartRange: Iterator[Int] =
      shuffled.slice(0, rangeIdx).iterator.flatMap(_.iterator)
    def endRange: Iterator[Int] = startRangeOrig.take(valueInRangeIdx)

    startRange ++ afterStartRange ++ beforeStartRange ++ endRange
  }
}

case class MesosRange(minValue: Int, maxValue: Int) {
  private[this] def range: Range.Inclusive = Range.inclusive(minValue, maxValue)
  def size: Int = range.size

  def iterator: Iterator[Int] = range.iterator.map(v => v)
  def drop(n: Int): Iterator[Int] = range.drop(n).iterator.map(v => v)
  def take(n: Int): Iterator[Int] = range.take(n).iterator.map(v => v)

  /*
   * Attention! range exports _two_ contains methods, a generic inefficient one and an efficient one
   * that only gets used with Int (and not java.lang.Integer and similar)
   */
  def contains(v: Int): Boolean = range.contains(v)
}

sealed trait ValueMatchResult
case object ValueNotAvailable extends ValueMatchResult
case class ValueMatched(port: Int) extends ValueMatchResult

object RangeResource {
  val RandomValue: Int = 0

  def ports(requestedPorts: Seq[Int], random: Random = Random): RangeResource = {
    new RangeResource(requestedPorts, ResourceType.PORTS, random)
  }

  /**
    * Return RangesResources covering all given values with the given roles.
    *
    * Creates as few RangesResources as possible while
    * preserving the order of the values.
    */
  def createMesosResource(
      resourceFromOffer: Protos.Resource,
      requestedValues: Seq[Int],
      resourceType: ResourceType): Seq[Protos.Resource] = {
    /*
     * Create as few ranges as possible from the given values while preserving the order of the values.
     *
     * It does not check if the given values have different roles.
     */
    def createRanges(ranges: Seq[Int]): Seq[Range] = {
      val builder = Seq.newBuilder[Range]

      @tailrec
      def process(lastRangeOpt: Option[Range], next: Seq[Int]): Unit = {
        (lastRangeOpt, next.headOption) match {
          case (None, _) =>
          case (Some(lastRange), None) =>
            builder += lastRange
          case (Some(lastRange), Some(nextVal)) if lastRange.end.toInt == nextVal - 1 =>
            process(Some(Range(lastRange.start, nextVal)), next.tail)
          case (Some(lastRange), Some(nextVal)) =>
            builder += lastRange
            process(Some(Range(nextVal, nextVal)), next.tail)
        }
      }
      process(ranges.headOption.map(v => Range(v, v)), ranges.tail)

      builder.result()
    }

    // TODO: we should handle roles and reservations here
    val rangesProto = Protos.Value.Ranges.newBuilder
      .addAllRange(createRanges(requestedValues).map(rangeToProto).asJava)
      .build

    val resource = ProtoBuilders.newResource(
      resourceType.name,
      Protos.Value.Type.RANGES,
      resourceFromOffer.getAllocationInfo,
      ranges = rangesProto)

    Seq(resource)
  }

  def rangeToProto(range: Range): Protos.Value.Range = {
    Protos.Value.Range.newBuilder
      .setBegin(range.start)
      .setEnd(range.end)
      .build
  }
}
