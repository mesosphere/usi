package com.mesosphere.usi.core.models

import com.mesosphere.usi.core.models.resources.{ExactValue, RangeRequirement, ResourceRequirement, ScalarRequirement}

/**
  * Scheduler logic state indicating that some action still needs to be done some pod [[RunningPodSpec]] or
  * [[TerminalPodSpec]]
  */
sealed trait PodSpec {
  val id: PodId
  def shouldBeTerminal: Boolean
}

/**
  * Used by the scheduler to track that a pod needs to be killed
  * @param id
  */
case class TerminalPodSpec(id: PodId) extends PodSpec {
  override def shouldBeTerminal: Boolean = true
}

/**
  * Used by the scheduler to track that a pod pod should be launched (and, isn't yet.)
  *
  * Pods are launched at-most-once.
  *
  * @param id Id of the pod
  * @param runSpec WIP the thing to run, and resource requirements, etc.
  */
case class RunningPodSpec(id: PodId, runSpec: RunTemplate) extends PodSpec {
  override def shouldBeTerminal: Boolean = false
}

object RunningPodSpec {
  type ValidationMessage = String
  val Valid = Seq.empty

  /**
    * Verifies that every value in range is requested only once. Requesting same value multiple times would yield pod that will never be scheduled.
    * E.g. requesting two ports 80 on one machine is just not possible to satisfy
    *
    * @param runSpec runSpec we are validating
    * @return true if provided range requirements are valid
    */
  private def validateStaticRangeRequirementsUnique(runSpec: RunTemplate): Seq[ValidationMessage] = {
    val staticPorts = runSpec.resourceRequirements.collect {
      case RangeRequirement(requestedValues, _, _) => requestedValues
    }.flatten.collect { case ExactValue(value) => value }

    if (staticPorts.distinct.length != staticPorts.length) {
      Seq(s"Every value inside RangeResource can be requested only once. Requirement: ${staticPorts.mkString(",")}")
    } else {
      Valid
    }
  }

  private def validateScalarRequirements(resourceRequirements: Seq[ResourceRequirement]): Seq[ValidationMessage] = {
    val invalidScalar = resourceRequirements.collect {
      case s: ScalarRequirement => s
    }.filter(s => s.amount < 0)
    if (invalidScalar.nonEmpty) {
      Seq(s"Scalar values cannot be smaller than 0. Invalid requirements: ${invalidScalar
        .map(s => s"${s.resourceType}:${s.amount}")
        .mkString(",")}")
    } else {
      Valid
    }
  }

  def isValid(runSpec: RunTemplate): Seq[ValidationMessage] = {
    val uniqueRangeRequirements = validateStaticRangeRequirementsUnique(runSpec)
    val scalarRequirements = validateScalarRequirements(runSpec.resourceRequirements)
    uniqueRangeRequirements ++ scalarRequirements
  }
}
