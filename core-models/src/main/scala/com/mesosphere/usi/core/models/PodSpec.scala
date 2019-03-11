package com.mesosphere.usi.core.models

import com.mesosphere.usi.core.models.resources.{ExactValue, RangeRequirement, ResourceRequirement, ScalarRequirement}
import com.mesosphere.usi.core.models.validation.{ErrorMessage, ValidationError}
import org.scalactic.Accumulation._
import org.scalactic.{Bad, Every, Good, One, Or}

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
trait PodSpec {
  def id: PodId
  def goal: Goal
  def runSpec: RunSpec
}

object PodSpec {

  /**
    * Verifies that every value in range is requested only once. Requesting same value multiple times would yield pod that will never be scheduled.
    * E.g. requesting two ports 80 on one machine is just not possible to satisfy
    * @param resourceRequirements resources we are validating
    * @return true if provided range requirements are valid
    */
  private def validateRangeRequirements(resourceRequirements: Seq[ResourceRequirement]): Unit Or One[ErrorMessage] = {
    val staticPorts = resourceRequirements.collect {
      case RangeRequirement(requestedValues, _, _) => requestedValues
    }.flatten.collect { case ExactValue(value) => value }

    if (staticPorts.distinct.length != staticPorts.length) {
      Bad(
        One(
          ValidationError(
            s"Every value inside RangeResource can be requested only once. Requirement: ${staticPorts.mkString(",")}")))
    } else {
      Good(())
    }
  }

  private def validateScalarRequirements(resourceRequirements: Seq[ResourceRequirement]): Unit Or One[ErrorMessage] = {
    val invalidScalar = resourceRequirements.collect {
      case s: ScalarRequirement => s
    }.filter(s => s.amount < 0)

    invalidScalar match {
      case Nil => Good(())
      case invalid =>
        Bad(One(ValidationError(s"Scalar values cannot be smaller than 0. Invalid requirements: ${invalid
          .map(s => s"${s.resourceType}:${s.amount}")
          .mkString(",")}")))
    }
  }

  /**
    * Groups together validation rules for resource requirements
    */
  private def validateResources(resourceRequirements: Seq[ResourceRequirement]): Unit Or Every[ErrorMessage] = {
    // this is here just to document how multiple validation methods can be grouped together
    withGood(validateRangeRequirements(resourceRequirements), validateScalarRequirements(resourceRequirements))(
      (_, _) => ())
  }

  def apply(id: PodId, goal: Goal, runSpec: RunSpec): PodSpec Or Every[ErrorMessage] = {
    val resourceValidation = validateResources(runSpec.resourceRequirements)

    resourceValidation.map(_ => PodSpecImpl(id, goal, runSpec))
  }

  private case class PodSpecImpl(id: PodId, goal: Goal, runSpec: RunSpec) extends PodSpec
}
