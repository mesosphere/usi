package com.mesosphere.mesos.client

import akka.http.scaladsl.model.headers
import akka.http.scaladsl.model.headers.{BasicHttpCredentials, HttpCredentials}

trait Authorization{

  def credentials(): HttpCredentials
  def header(): headers.Authorization = headers.Authorization(credentials())
}

case class JwtHttpCredentials(token: String) extends HttpCredentials {

  override def scheme: String = "token"
  override def params: Map[String, String] = Map.empty
}

case class Jwt(privateKey: String) extends Authorization {

  val token: String = "incorrect"

  override def credentials(): HttpCredentials = JwtHttpCredentials(token)
}

case class Basic(user: String, password: String) extends Authorization {

  override def credentials(): HttpCredentials = BasicHttpCredentials(user, password)
}
