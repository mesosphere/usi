package com.mesosphere.usi.core
import com.mesosphere.usi.core.models.{PodId, PodSpec}

case class SpecState(podSpecs: Map[PodId, PodSpec])
object SpecState {
  val empty = SpecState(Map.empty)
}
