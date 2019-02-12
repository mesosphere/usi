package com.mesosphere.usi.core.matching

import org.apache.mesos.v1.{Protos => Mesos}

case class ResourceMatchResult(matchedResources: Seq[Mesos.Resource], remainingResource: Seq[Mesos.Resource])
