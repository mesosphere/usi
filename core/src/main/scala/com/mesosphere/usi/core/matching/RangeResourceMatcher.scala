package com.mesosphere.usi.core.matching

import com.mesosphere.usi.core.ResourceUtil
import com.mesosphere.usi.core.models.resources.{
  ExactValue,
  OrderedSelection,
  RandomSelection,
  RandomValue,
  RangeRequirement,
  ResourceType
}
import com.mesosphere.usi.core.protos.ProtoBuilders
import org.apache.mesos.v1.Protos

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.Random

case class MesosRange(minValue: Int, maxValue: Int) {
  private[this] def range: Range.Inclusive = Range.inclusive(minValue, maxValue)
  def size: Int = range.size

  def iterator: Iterator[Int] = range.iterator
  def drop(n: Int): Iterator[Int] = range.drop(n).iterator
  def take(n: Int): Iterator[Int] = range.take(n).iterator

  /*
   * Attention! range exports _two_ contains methods, a generic inefficient one and an efficient one
   * that only gets used with Int (and not java.lang.Integer and similar)
   */
  def contains(v: Int): Boolean = range.contains(v)
}

sealed trait ValueMatchResult
case object ValueNotAvailable extends ValueMatchResult
case class ValueMatched(port: Int) extends ValueMatchResult

object RangeResourceMatcher {

  def matchAndConsume(
      rangeRequirment: RangeRequirement,
      resources: Seq[Protos.Resource]): Option[ResourceMatchResult] = {
    matchAndConsumeIter(rangeRequirment, Nil, resources.toList)
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
      rangeRequirment: RangeRequirement,
      unmatchedResources: List[Protos.Resource],
      remainingResources: List[Protos.Resource]): Option[ResourceMatchResult] = {

    remainingResources match {
      case Nil =>
        None
      case next :: rest =>
        tryConsumeValuesFromResource(rangeRequirment, next) match {
          case Nil =>
            matchAndConsumeIter(rangeRequirment, next :: unmatchedResources, rest)
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
    * @param requestedValues values we want to consume
    * @param resource mesos resource to match agains
    * @return final list of mesos resources created after consuming requested values, empty if match was not possible
    */
  private def tryConsumeValuesFromResource(
      rangeRequirment: RangeRequirement,
      resource: Protos.Resource): Seq[Protos.Resource] = {
    val offeredRanges = parseResourceToRanges(rangeRequirment, resource)
    if (offeredRanges.isEmpty || rangeRequirment.requestedValues.isEmpty) {
      return Seq.empty
    }

    // non-dynamic values
    val staticRequestedValues = rangeRequirment.requestedValues.collect { case ExactValue(v) => v }.toSet
    val availableForDynamicAssignment: Iterator[Int] = rangeRequirment.valueSelectionPolicy match {
      case RandomSelection(r) =>
        lazyRandomValuesFromRanges(offeredRanges, r)
          .filter(v => !staticRequestedValues(v))
      case OrderedSelection =>
        offeredRanges.iterator
          .flatMap(_.iterator)
          .filterNot(staticRequestedValues)
    }

    val matchResult = rangeRequirment.requestedValues.map {
      case RandomValue if !availableForDynamicAssignment.hasNext =>
        // need dynamic value but no more available
        ValueNotAvailable
      case RandomValue if availableForDynamicAssignment.hasNext =>
        // pick next available dynamic value
        ValueMatched(availableForDynamicAssignment.next())
      case _ @ExactValue(v) if offeredRanges.exists(_.contains(v)) =>
        // static value
        ValueMatched(v)
      case _ =>
        ValueNotAvailable
    }

    if (matchResult.contains(ValueNotAvailable)) {
      Seq.empty
    } else {
      createMesosResource(
        resource,
        matchResult.collect { case ValueMatched(v) => v }.toSeq,
        rangeRequirment.resourceType)
    }
  }

  /**
    * Converts given Resource to sequence of ranges
    * If given resource is not resource of given name and type range, it will return empty sequence
    * @param resource mesos resource definition
    * @return list of parsed ranges in that resource
    */
  private def parseResourceToRanges(rangeResource: RangeRequirement, resource: Protos.Resource): Seq[MesosRange] = {
    if (resource.getName != rangeResource.resourceType.name) {
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

    def overlaps(range: Range, nextVal: Int): Boolean = range.end.toInt == nextVal - 1
    /*
     * Create as few ranges as possible from the given values while preserving the order of the values.
     *
     * It does not check if the given values have different roles.
     */
    def createRanges(values: Seq[Int]): Seq[Range] = {
      val builder = Vector.newBuilder[Range]

      if (values.nonEmpty) {
        var currentRange = Range(values.head, values.head)
        values.tail.foreach { v =>
          if (overlaps(currentRange, v)) {
            currentRange = Range(currentRange.start, v)
          } else {
            builder += currentRange
            currentRange = Range(v, v)
          }
        }
        builder += currentRange // append last range.
      }

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
