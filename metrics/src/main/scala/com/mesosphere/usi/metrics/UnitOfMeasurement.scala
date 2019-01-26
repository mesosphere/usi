package com.mesosphere.usi.metrics

sealed trait UnitOfMeasurement

object UnitOfMeasurement {

  /**
    * Default unit of measurement. Not suffix is appended to metric names.
    */
  case object None extends UnitOfMeasurement

  /**
    * Memory is measure in bytes. ".bytes" is appended to metric names.
    */
  case object Memory extends UnitOfMeasurement

  /**
    * Time is measured in seconds. ".seconds" is appended to timer names.
    */
  case object Time extends UnitOfMeasurement
}
