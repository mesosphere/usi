package com.mesosphere.usi.core.models.faultdomain
import org.apache.mesos.v1.Protos.DomainInfo

case object HomeRegionSelector extends DomainSelector {
  override def apply(masterDomain: DomainInfo, nodeDomain: DomainInfo): Boolean = {
    masterDomain.getFaultDomain.getRegion == nodeDomain.getFaultDomain.getRegion
  }
}
