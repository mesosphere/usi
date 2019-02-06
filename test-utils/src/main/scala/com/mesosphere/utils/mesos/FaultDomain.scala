package com.mesosphere.utils.mesos

case class FaultDomain(region: Region, zone: Zone)

case class Region(value: String) extends AnyVal {
  override def toString = value
}

case class Zone(value: String) extends AnyVal {
  override def toString = value
}
