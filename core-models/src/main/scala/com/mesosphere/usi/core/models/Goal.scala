package com.mesosphere.usi.core.models

sealed trait Goal

object Goal {
  case object Running extends Goal
  case object Terminal extends Goal
}
