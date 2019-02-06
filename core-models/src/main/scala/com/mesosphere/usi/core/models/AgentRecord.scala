package com.mesosphere.usi.core.models

/**
  * Persistent record of agent attributes and other pertinent non-recoverable facts that are inputs for launching pods
  */
case class AgentRecord(id: AgentId, hostname: String)
