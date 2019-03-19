package com.mesosphere.usi.core.models

import com.mesosphere.usi.core.models.resources.{ExactValue, RangeRequirement, ResourceRequirement, ScalarRequirement}

/**
  * Framework implementation owned specification of some Pod that should be launched.
  *
  * Pods are launched at-most-once. It is illegal to transition of [[PodSpec]] from goal terminal to goal running.
  *
  * The deletion of a pod for which a known non-terminal task status exists will result in a spurious pod. Spurious pods
  * can be killed by specifying a [[PodSpec]] for said spurious pod with [[Goal]] terminal.
  *
  * @param id Id of the pod
  * @param goal target goal of this pod. See [[Goal]] for more details
  * @param runSpec WIP the thing to run, and resource requirements, etc.
  */
case class PodSpec(id: PodId, goal: Goal, runSpec: RunSpec)

object PodSpec {
  type ValidationMessage = String

  /**
    * Verifies that every value in range is requested only once. Requesting same value multiple times would yield pod that will never be scheduled.
    * E.g. requesting two ports 80 on one machine is just not possible to satisfy
    * @param runSpec runSpec we are validating
    * @return true if provided range requirements are valid
    */
  private def validateStaticRangeRequirementsUnique(runSpec: RunSpec): Seq[ValidationMessage] = {
    val staticPorts = runSpec.resourceRequirements.collect {
      case RangeRequirement(requestedValues, _, _) => requestedValues
    }.flatten.collect { case ExactValue(value) => value }

    if (staticPorts.distinct.length != staticPorts.length) {
      Seq(s"Every value inside RangeResource can be requested only once. Requirement: ${staticPorts.mkString(",")}")
    } else {
      Seq.empty
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
      Seq.empty
    }
  }

  def isValid(podSpec: PodSpec): Seq[ValidationMessage] = {
    val uniqueRangeRequirements = validateStaticRangeRequirementsUnique(podSpec.runSpec)
    val scalarRequirements = validateScalarRequirements(podSpec.runSpec.resourceRequirements)

    uniqueRangeRequirements ++ scalarRequirements
  }
}
