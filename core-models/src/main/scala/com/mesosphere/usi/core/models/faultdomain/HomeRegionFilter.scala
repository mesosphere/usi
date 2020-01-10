package com.mesosphere.usi.core.models.faultdomain
import org.apache.mesos.v1.Protos.DomainInfo

case object HomeRegionFilter extends DomainFilter {
  override def apply(masterDomain: DomainInfo, nodeDomain: DomainInfo): Boolean = {
    masterDomain.getFaultDomain.getRegion == nodeDomain.getFaultDomain.getRegion
  }

  override def description: String = "expect home region."
}
