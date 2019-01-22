package com.mesosphere.usi.core.models

/**
  * Target state of PodSpec. Scheduler will work towards achieving this goal state.
  */
sealed trait Goal

object Goal {

  /**
    * [[PodSpec]] with this goal should be started if is not running
    */
  case object Running extends Goal

  /**
    * [[PodSpec]] with this goal should be killed if it is running.
    */
  case object Terminal extends Goal
}
