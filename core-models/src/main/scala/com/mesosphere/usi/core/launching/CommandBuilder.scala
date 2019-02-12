package com.mesosphere.usi.core.launching

import org.apache.mesos.v1.{Protos => Mesos}

trait CommandBuilder {
  def buildCommandInfo(offer: Mesos.Offer, matchedResources: Seq[Mesos.Resource]) = {
    
  }
}
