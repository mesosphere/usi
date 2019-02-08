package com.mesosphere.mesos.examples

/**
  * A minimal version of a task's specification. Contains the command and the necessary resources. Note that we only
  * consider memory and cpus for now, ignoring disk, gpus, ports etc.
  *
  * @param name task's name
  * @param cmd command to execute
  * @param cpus amount of CPU resources
  * @param mem amount of memory (in Megabytes)
  */
case class Spec(name: String, cmd: String, cpus: Double = 0.1, mem: Double = 32.0) {
  require(name.matches("""^[a-z-]+"""), "Task name can not be empty and can only contain lowercase letters and -")
}
