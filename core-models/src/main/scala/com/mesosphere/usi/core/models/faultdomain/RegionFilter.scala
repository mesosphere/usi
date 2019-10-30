package com.mesosphere.usi.core.models.faultdomain
import org.apache.mesos.v1.Protos

case class RegionFilter(region: String) extends DomainFilter {
  override def apply(masterDomain: Protos.DomainInfo, nodeDomain: Protos.DomainInfo): Boolean = {
    region == nodeDomain.getFaultDomain.getRegion.getName
  }
}
