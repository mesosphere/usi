package com.mesosphere.usi.helloworld.http

/**
  * Set of classes used to marshall and unmarshall JSON entities.
  *
  * It has a Json prefix to avoid collisions with other classes.
  *
  */
object DTO {

  case class ResourceRequirement(
      resource: String,
      amount: Double
  )

  case class ServiceSpec(
      resourceRequirements: Seq[ResourceRequirement],
      shellCommand: String
  )

  case class Service(id: String, serviceSpec: ServiceSpec)

  case class ServiceSpecDefinition(id: String, cpus: Double, mem: Double, disk: Double, command: String)

  case class ServiceInfo(id: String, serviceSpec: ServiceSpec, status: String)

  case class DeploymentResult(deploymentId: String)
}
