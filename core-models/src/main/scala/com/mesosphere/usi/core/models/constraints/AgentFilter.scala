package com.mesosphere.usi.core.models.constraints

import org.apache.mesos.v1.Protos

/**
  * Filter on resources or attributes specific to agents.
  * Mostly used for attribute filtering.
  */
trait AgentFilter {
  def apply(offer: Protos.Offer): Boolean

  /** @return a text description of the matcher */
  def description: String
}
