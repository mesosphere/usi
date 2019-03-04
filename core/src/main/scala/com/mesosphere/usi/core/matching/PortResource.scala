package com.mesosphere.usi.core.matching

import com.mesosphere.usi.core.ResourceUtil
import com.mesosphere.usi.core.matching.PortResource._
import com.mesosphere.usi.core.models.{ResourceMatchResult, ResourceRequirement, ResourceType}
import com.mesosphere.usi.core.protos.ProtoBuilders
import org.apache.mesos.v1.{Protos}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.Random

case class PortResource(ports: Seq[Int], random: Random = Random) extends ResourceRequirement {
  override def description: String = s"$resourceType:[${ports.mkString(",")}]"

  override def resourceType: ResourceType = ResourceType.PORTS

  @tailrec private def findResourceToMatch(
      unmatchedResources: List[Protos.Resource],
      remainingResources: List[Protos.Resource]): Option[ResourceMatchResult] = {

    remainingResources match {
      case Nil =>
        None
      case next :: rest =>
        matchPortResource(next) match {
          case Nil =>
            findResourceToMatch(next :: unmatchedResources, rest)
          case consumedResources =>
            Some(
              ResourceMatchResult(
                consumedResources,
                unmatchedResources ++ rest ++ ResourceUtil.consumeResources(Seq(next), consumedResources)))
        }
    }
  }

  private def matchPortResource(resource: Protos.Resource): Seq[Protos.Resource] = {
    val offeredPorts = resourceToPortRange(resource)
    if (offeredPorts.isEmpty || ports.isEmpty) {
      return Seq.empty
    }

    // non-dynamic hostPorts
    val staticPorts = ports.collect { case port if port != 0 => port }.toSet
    val availablePortsWithoutStaticPorts: Iterator[Int] =
      lazyRandomPortsFromRanges(offeredPorts, random).filter(port => !staticPorts(port))

    val portMatchResults = ports.map {
      case port if port == RandomPort && !availablePortsWithoutStaticPorts.hasNext =>
        // need dynamic port but no more available
        PortNotAvailable
      case port if port == RandomPort && availablePortsWithoutStaticPorts.hasNext =>
        // pick next available dynamic port
        PortMatched(availablePortsWithoutStaticPorts.next())
      case port if offeredPorts.exists(_.contains(port)) =>
        // static port
        PortMatched(port)
      case _ =>
        PortNotAvailable
    }

    if (portMatchResults.contains(PortNotAvailable)) {
      Seq.empty
    } else {
      createPortsResources(resource, portMatchResults.collect { case PortMatched(port) => port }.toSeq, resourceType)
    }
  }

  override def matchAndConsume(resources: Seq[Protos.Resource]): Option[ResourceMatchResult] = {
    findResourceToMatch(Nil, resources.toList)
  }

  /**
    * Converts given Resource to sequence of port ranges
    * If given resource is not ports resource of type range, it will return empty sequence
    * @param resource mesos resource definition
    * @return list of port ranges in that resource
    */
  private def resourceToPortRange(resource: Protos.Resource): Seq[PortRange] = {
    if (resource.getName != resourceType.name) {
      Seq.empty
    } else {
      val rangeInResource = resource.getRanges.getRangeList.asScala
      rangeInResource.map { range =>
        PortRange(range.getBegin.toInt, range.getEnd.toInt)
      }
    }
  }

  /**
    * We want to make it less likely that we are reusing the same dynamic port for tasks of different pods.
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
  private def lazyRandomPortsFromRanges(offeredPortRanges: Seq[PortRange], rand: Random): Iterator[Int] = {
    val numberOfOfferedPorts = offeredPortRanges.map(_.size).sum

    if (numberOfOfferedPorts == 0) {
      return Iterator.empty
    }

    def findStartPort(shuffled: IndexedSeq[PortRange], startPortIdx: Int): (Int, Int) = {
      var startPortIdxOfCurrentRange = 0
      val rangeIdx = shuffled.indexWhere {
        case range: PortRange if startPortIdxOfCurrentRange + range.size > startPortIdx =>
          true
        case range: PortRange =>
          startPortIdxOfCurrentRange += range.size
          false
      }

      (rangeIdx, startPortIdx - startPortIdxOfCurrentRange)
    }

    val shuffled = rand.shuffle(offeredPortRanges).toIndexedSeq
    val startPortIdx = rand.nextInt(numberOfOfferedPorts)
    val (rangeIdx, portInRangeIdx) = findStartPort(shuffled, startPortIdx)
    val startRangeOrig = shuffled(rangeIdx)

    val startRange = startRangeOrig.drop(portInRangeIdx)

    // These are created on demand if necessary
    def afterStartRange: Iterator[Int] =
      shuffled.slice(rangeIdx + 1, shuffled.length).iterator.flatMap(_.iterator)
    def beforeStartRange: Iterator[Int] =
      shuffled.slice(0, rangeIdx).iterator.flatMap(_.iterator)
    def endRange: Iterator[Int] = startRangeOrig.take(portInRangeIdx)

    startRange ++ afterStartRange ++ beforeStartRange ++ endRange
  }
}

case class PortRange(minPort: Int, maxPort: Int) {
  private[this] def range: Range.Inclusive = Range.inclusive(minPort, maxPort)
  def size: Int = range.size

  def iterator: Iterator[Int] = range.iterator.map(port => port)
  def drop(n: Int): Iterator[Int] = range.drop(n).iterator.map(port => port)
  def take(n: Int): Iterator[Int] = range.take(n).iterator.map(port => port)

  /*
   * Attention! range exports _two_ contains methods, a generic inefficient one and an efficient one
   * that only gets used with Int (and not java.lang.Integer and similar)
   */
  def contains(port: Int): Boolean = range.contains(port)
}

sealed trait PortMatchResult
case object PortNotAvailable extends PortMatchResult
case class PortMatched(port: Int) extends PortMatchResult

object PortResource {
  val RandomPort: Int = 0

  /**
    * Return RangesResources covering all given ports with the given roles.
    *
    * Creates as few RangesResources as possible while
    * preserving the order of the ports.
    */
  def createPortsResources(
      originalResource: Protos.Resource,
      requestedPorts: Seq[Int],
      resourceType: ResourceType): Seq[Protos.Resource] = {
    /*
     * Create as few ranges as possible from the given ports while preserving the order of the ports.
     *
     * It does not check if the given ports have different roles.
     */
    def createRanges(ranges: Seq[Int]): Seq[Range] = {
      val builder = Seq.newBuilder[Range]

      @tailrec
      def process(lastRangeOpt: Option[Range], next: Seq[Int]): Unit = {
        (lastRangeOpt, next.headOption) match {
          case (None, _) =>
          case (Some(lastRange), None) =>
            builder += lastRange
          case (Some(lastRange), Some(nextPort)) if lastRange.end.toInt == nextPort - 1 =>
            process(Some(Range(lastRange.start, nextPort)), next.tail)
          case (Some(lastRange), Some(nextPort)) =>
            builder += lastRange
            process(Some(Range(nextPort, nextPort)), next.tail)
        }
      }
      process(ranges.headOption.map(port => Range(port, port)), ranges.tail)

      builder.result()
    }

    // TODO: we should handle roles and reservations here
    val rangesProto = Protos.Value.Ranges.newBuilder
      .addAllRange(createRanges(requestedPorts).map(rangeToProto).asJava)
      .build

    val resource = ProtoBuilders.newResource(
      resourceType.name,
      Protos.Value.Type.RANGES,
      originalResource.getAllocationInfo,
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
