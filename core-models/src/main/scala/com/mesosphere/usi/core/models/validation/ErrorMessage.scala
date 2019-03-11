package com.mesosphere.usi.core.models.validation

sealed trait ErrorMessage {
  def message: String
}
case class ValidationError(message: String) extends ErrorMessage
