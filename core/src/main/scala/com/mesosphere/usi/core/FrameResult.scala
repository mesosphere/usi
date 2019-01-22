package com.mesosphere.usi.core

import com.mesosphere.usi.models.{Mesos, USIStateEvent}

sealed trait FrameResult {
  def usiStateEvents: List[USIStateEvent]

  def mesosCalls: List[Mesos.Call]
}

case class FrameResultWithMessages(usiStateEvents: List[USIStateEvent] = Nil, mesosCalls: List[Mesos.Call] = Nil) extends FrameResult

case class FrameResultBuilder(reverseUSIStateEvents: List[USIStateEvent] = Nil, reverseMesosCalls: List[Mesos.Call] = Nil) extends FrameResult {
  lazy val usiStateEvents = reverseUSIStateEvents.reverse
  lazy val mesosCalls = reverseMesosCalls.reverse

  def ++(other: FrameResultBuilder): FrameResultBuilder = {
    if (other == FrameResultBuilder.empty)
      this
    else if (this == FrameResultBuilder.empty)
      other
    else
      FrameResultBuilder(other.reverseUSIStateEvents ++ reverseUSIStateEvents, other.reverseMesosCalls ++ reverseMesosCalls)
  }

  def withChangeMessage(message: USIStateEvent): FrameResultBuilder = copy(reverseUSIStateEvents = message :: reverseUSIStateEvents)

  def withMesosCall(call: Mesos.Call): FrameResultBuilder = copy(reverseMesosCalls = call :: reverseMesosCalls)
}

object FrameResultBuilder {
  val empty = FrameResultBuilder()
}
