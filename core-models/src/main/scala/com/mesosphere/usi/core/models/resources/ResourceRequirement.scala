package com.mesosphere.usi.core.models.resources

import scala.util.Random

/**
  * Used for resource matching.
  */
sealed trait ResourceRequirement {

  /**
    * A human readable description for this resource requirement
    */
  def description: String

  /**
    * The ResourceType this requirement describes
    */
  def resourceType: ResourceType
}

/**
  * Represents requirement for Mesos resource of type Range. http://mesos.apache.org/documentation/attributes-resources/
  *
  * You can either request static values (by providing concrete value) or dynamic values (by providing 0).
  * Dynamic values are then selected from the offered ranges. If random implementation is passed in, these ranges are randomized.
  *
  * @param requestedValues values pod wants to consume on the given ranges
  * @param resourceType name of resource (e.g. ports)
  * @param valueSelectionPolicy when requesting dynamic values, you can provide Random implementation if you want dynamic values to be randomized
  */
case class RangeRequirement(
    requestedValues: Seq[RequestedValue],
    resourceType: ResourceType,
    valueSelectionPolicy: ValueSelectionPolicy = RandomSelection(Random))
    extends ResourceRequirement {
  override def description: String = s"$resourceType:[${requestedValues.mkString(",")}]"
}

sealed trait ValueSelectionPolicy
case object OrderedSelection extends ValueSelectionPolicy
case class RandomSelection(randomGenerator: Random) extends ValueSelectionPolicy

sealed trait RequestedValue
case class ExactValue(value: Int) extends RequestedValue
case object RandomValue extends RequestedValue

object RangeRequirement {
  val RandomPort: Int = 0

  def ports(
      requestedPorts: Seq[Int],
      valueSelectionPolicy: ValueSelectionPolicy = RandomSelection(Random)): RangeRequirement = {
    new RangeRequirement(
      requestedPorts.map(p => if (p == RandomPort) RandomValue else ExactValue(p)),
      ResourceType.PORTS,
      valueSelectionPolicy)
  }
}

case class ScalarRequirement(resourceType: ResourceType, amount: Double) extends ResourceRequirement {
  override def description: String = s"$resourceType:$amount"
}

object ScalarRequirement {
  def cpus(amount: Double): ScalarRequirement = ScalarRequirement(ResourceType.CPUS, amount)
  def memory(amount: Double): ScalarRequirement = ScalarRequirement(ResourceType.MEM, amount)
  def disk(amount: Double): ScalarRequirement = ScalarRequirement(ResourceType.DISK, amount)
  def gpus(amount: Double): ScalarRequirement = ScalarRequirement(ResourceType.GPUS, amount)
}
