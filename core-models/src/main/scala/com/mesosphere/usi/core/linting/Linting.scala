package com.mesosphere.usi.core.linting

import com.mesosphere.usi.core.models.resources.{ExactValue, RangeRequirement, ResourceRequirement, ScalarRequirement}
import com.mesosphere.usi.core.models.template.RunTemplate
import com.mesosphere.usi.core.models.template.RunTemplate.KeyedResourceRequirement

class Linting {
  type LintingWarning = String

  /**
    * Verifies that every value in range is requested only once. Requesting same value multiple times would yield pod that will never be scheduled.
    * E.g. requesting two ports 80 on one machine is just not possible to satisfy
    *
    * @param runTemplate runTemplate we are linting
    * @return List of linting errors; empty if none
    */
  private def validateStaticRangeRequirementsUnique(runTemplate: RunTemplate): Seq[LintingWarning] = {
    val staticPorts = runTemplate.allResourceRequirements.collect {
      case KeyedResourceRequirement(_, RangeRequirement(requestedValues, _, _)) => requestedValues
    }.flatten.collect { case ExactValue(value) => value }

    if (staticPorts.distinct.length != staticPorts.length) {
      Seq(s"Every value inside RangeResource can be requested only once. Requirement: ${staticPorts.mkString(",")}")
    } else {
      Nil
    }
  }

  private def validateScalarRequirements(resourceRequirements: Seq[ResourceRequirement]): Seq[LintingWarning] = {
    val invalidScalar = resourceRequirements.collect {
      case s: ScalarRequirement => s
    }.filter(s => s.amount < 0)
    if (invalidScalar.nonEmpty) {
      Seq(s"Scalar values cannot be smaller than 0. Invalid requirements: ${invalidScalar
        .map(s => s"${s.resourceType}:${s.amount}")
        .mkString(",")}")
    } else {
      Nil
    }
  }

  def isValid(runTemplate: RunTemplate): Seq[LintingWarning] = {
    val uniqueRangeRequirements = validateStaticRangeRequirementsUnique(runTemplate)
    val scalarRequirements = validateScalarRequirements(runTemplate.allResourceRequirements.map(_.requirement))
    uniqueRangeRequirements ++ scalarRequirements
  }
}
