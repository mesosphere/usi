package com.mesosphere.usi.core.models.constraints

import org.apache.mesos.v1.Protos

// Filter on resources or attributes specific to agents.
// Mostly used for attribute filtering.
trait AgentFilter {
  def apply(offer: Protos.Offer): Boolean

  /**
    * Return a text description of the matcher
    * @return
    */
  def description: String
}
