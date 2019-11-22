package com.mesosphere.usi.core.models.faultdomain

import org.apache.mesos.v1.Protos.DomainInfo

/**
  * A filter for Mesos [[http://mesos.apache.org/documentation/latest/fault-domains/ fault domains]] used during offer
  * matching. See [[HomeRegionFilter]] for the fault filter used.
  */
trait DomainFilter {
  def apply(masterDomain: DomainInfo, nodeDomain: DomainInfo): Boolean
}
