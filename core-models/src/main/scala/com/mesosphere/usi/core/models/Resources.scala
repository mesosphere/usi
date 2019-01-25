package com.mesosphere.usi.core.models

/**
  * Resources that were reserved with a reservation
  * @param cpus number of CPUs
  * @param mem amount of memory
  * @param disk reserved disk space
  * @param gpus reserved GPUs
  */
case class Resources(cpus: Double = 0.0, mem: Double = 0.0, disk: Double = 0.0, gpus: Int = 0)