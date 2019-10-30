package com.mesosphere.usi.core.models.faultdomain

import org.apache.mesos.v1.Protos.DomainInfo

trait DomainFilter {
  def apply(masterDomain: DomainInfo, nodeDomain: DomainInfo): Boolean
}
