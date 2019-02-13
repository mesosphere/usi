package com.mesosphere.usi.core.launching

import com.mesosphere.usi.core.models.CommandInfoGenerator
import org.apache.mesos.v1.Protos

case class SimpleShellCommandInfoGenerator(command: String) extends CommandInfoGenerator {
  override def buildCommandInfo(): Protos.CommandInfo = {
    Protos.CommandInfo.newBuilder()
      .setShell(true).setValue(command).build
  }
}
