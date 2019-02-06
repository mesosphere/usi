package com.mesosphere.usi.core
import com.mesosphere.usi.core.models.{PodId, PodSpec}

case class SpecificationState(podSpecs: Map[PodId, PodSpec])
object SpecificationState {
  val empty = SpecificationState(Map.empty)
}

