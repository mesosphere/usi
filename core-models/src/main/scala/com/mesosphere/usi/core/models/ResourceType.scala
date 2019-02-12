package com.mesosphere.usi.core.models

sealed trait ResourceType {
  def name: String
  override def toString(): String = name
}
object ResourceType {
  case object CPUS extends ResourceType { val name = "cpus" }
  case object MEM extends ResourceType { val name = "mem" }
  case object DISK extends ResourceType { val name = "disk" }
  case object PORTS extends ResourceType { val name = "ports" }
  case object GPUS extends ResourceType { val name = "gpus" }
  private val all = Seq(CPUS, MEM, DISK, PORTS, GPUS)

  def fromName(name: String): Option[ResourceType] = {
    all.find(_.name == name)
  }
}