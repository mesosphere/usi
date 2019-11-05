package com.mesosphere.usi.core.models.constraints

import org.apache.mesos.v1.Protos

// Default passthrough filter.
case object DefaultAgentFilter extends AgentFilter {
  override def apply(offer: Protos.Offer): Boolean = {
    true
  }
}
