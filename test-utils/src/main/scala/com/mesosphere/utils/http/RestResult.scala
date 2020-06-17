package com.mesosphere.utils.http

import akka.http.scaladsl.model.HttpResponse
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.matchers.{BeMatcher, MatchResult}
import play.api.libs.json.{JsValue, Json}

import scala.reflect.ClassTag

/**
  * Result of an REST call.
  */
case class RestResult[+T](valueGetter: () => T, originalResponse: HttpResponse, entityString: String)(
    implicit ct: ClassTag[T])
    extends StrictLogging {
  def code: Int = originalResponse.status.intValue
  def success: Boolean = code >= 200 && code < 300
  lazy val value: T = try {
    valueGetter()
  } catch {
    case ex: Throwable =>
      import scala.collection.JavaConverters._
      val headersStr = originalResponse.getHeaders.asScala.map { h =>
        h.name() + "=" + h.value()
      }.mkString("; ")
      logger.error(s"""Error parsing RestResult of type ${ct.runtimeClass}
                      |Request entity is '${entityString}'
                      |Headers: ${headersStr}.
                      |Status: ${originalResponse.status}
                      |ContentType: ${originalResponse.entity.contentType}
                      |ContentLengthOption: ${originalResponse.entity.getContentLengthOption}
                      |""".stripMargin)
      throw ex
  }

  /** Transform the value of this result. */
  def map[R](change: T => R)(implicit ct: ClassTag[R]): RestResult[R] = {
    RestResult(() => change(valueGetter()), originalResponse, entityString)
  }

  /** Parse the original response entity (=body) as json. */
  lazy val entityJson: JsValue = Json.parse(entityString)

  /** Pretty print the original response entity (=body) as json. */
  lazy val entityPrettyJsonString: String = Json.prettyPrint(entityJson)
}

object RestResult {
  def apply(response: HttpResponse, entityString: String): RestResult[HttpResponse] = {
    new RestResult[HttpResponse](() => response, response, entityString)
  }
}

/**
  * Custom matcher for HTTP responses that print response body.
  * @param status The expected status code.
  */
class RestResultMatcher(status: Int) extends BeMatcher[RestResult[_]] {
  def apply(left: RestResult[_]) =
    MatchResult(
      left.code == status,
      s"Response code was not $status but ${left.code} with body '${left.entityString}'",
      s"Response code was $status with body '${left.entityString}'"
    )
}

trait RestResultMatchers {
  val OK = new RestResultMatcher(200)
  val Created = new RestResultMatcher(201)
  val Accepted = new RestResultMatcher(202)
  val NoContent = new RestResultMatcher(204)
  val Redirect = new RestResultMatcher(302)
  val NotFound = new RestResultMatcher(404)
  val Conflict = new RestResultMatcher(409)
  val UnprocessableEntity = new RestResultMatcher(422)
  val ServerError = new RestResultMatcher(500)
  val BadGateway = new RestResultMatcher(502)
}
