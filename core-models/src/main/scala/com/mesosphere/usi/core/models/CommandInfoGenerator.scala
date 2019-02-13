package com.mesosphere.usi.core.models

import org.apache.mesos.v1.{Protos => Mesos}

trait CommandInfoGenerator {
  def buildCommandInfo(): Mesos.CommandInfo
}
