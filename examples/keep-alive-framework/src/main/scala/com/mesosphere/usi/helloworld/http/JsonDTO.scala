package com.mesosphere.usi.helloworld.http


/**
  * Set of classes used to marshall and unmarshall JSON entities.
  *
  * It has a Json prefix to avoid collisions with other classes.
  *
  */
object JsonDTO {

  case class JsonResourceRequirement(
      resource: String,
      amount: Double
  )

  case class JsonRunSpec(
      resourceRequirements: Seq[JsonResourceRequirement],
      shellCommand: String
  )

  case class JsonApp(id: String, runSpec: JsonRunSpec)

  case class JsonRunSpecDefinition(id: String,
                                   cpus: Double,
                                   mem: Double,
                                   disk: Double,
                                   command: String
                        )

  case class JsonAppInfo(id: String, runSpec: JsonRunSpec, status: String)

  case class DeploymentResult(deploymentId: String)
}
