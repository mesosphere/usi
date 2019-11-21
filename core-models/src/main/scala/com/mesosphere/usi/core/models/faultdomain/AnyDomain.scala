package com.mesosphere.usi.core.models.faultdomain
import org.apache.mesos.v1.Protos

object AnyDomain extends DomainFilter {

  override def apply(masterDomain: Protos.DomainInfo, nodeDomain: Protos.DomainInfo): Boolean = true
}
