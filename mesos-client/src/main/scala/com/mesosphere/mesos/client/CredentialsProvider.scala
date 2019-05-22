package com.mesosphere.mesos.client

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{BasicHttpCredentials, HttpCredentials}
import org.json4s.native.JsonMethods
import pdi.jwt.JwtJson4s
import pdi.jwt.JwtAlgorithm.RS256

trait CredentialsProvider{

  def credentials(): HttpCredentials
  def header(): headers.Authorization = headers.Authorization(credentials())
  def refresh(): Unit = ???
}

case class JwtHttpCredentials(token: String) extends HttpCredentials {

  override def scheme: String = "token"
  override def params: Map[String, String] = Map.empty
}

case class JwtProvider(privateKey: String) extends CredentialsProvider {

  val root: String = "http://dcos.io"
  val token: String = "incorrect"

  // Should probably be a flow at some point
  def getToken()(implicit system: ActorSystem): Unit = {
    import org.json4s._
    import org.json4s.JsonDSL._
    import JsonMethods.{render, compact}
    val claim = JObject(("uid", 1), ("exp", 1431520421))
    val token = JwtJson4s.encode(claim, privateKey, RS256)
    val data: String = compact(render(JObject(("uid", 1), ("token", token))))
    
    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = Uri(s"$root/acs/api/v1/auth/login"),
      entity = HttpEntity(ContentTypes.`application/json`, data)
    )
    Http().singleRequest(request)
  }

  override def credentials(): HttpCredentials = JwtHttpCredentials(token)
}

case class BasicAuthenticationProvider(user: String, password: String) extends CredentialsProvider {

  override def credentials(): HttpCredentials = BasicHttpCredentials(user, password)
}
