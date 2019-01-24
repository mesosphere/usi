package mesosphere.gradle.github

import org.gradle.api.DefaultTask
import org.gradle.api.tasks.TaskAction
import scalaj.http._

import scala.beans.BeanProperty

object GithubStatus {
  sealed trait State {
    def value: String = toString.toLowerCase
  }
  case object Error extends State
  case object Failure extends State
  case object Pending extends State
  case object Success extends State
}

class GithubStatus extends DefaultTask {
  @BeanProperty var targetUrl: String = ""

  @BeanProperty var statusDescription: String = ""

  @BeanProperty var repoSlug = ""

  @BeanProperty var commit: String = ""

  @BeanProperty var context: String = ""

  @BeanProperty var commitState: GithubStatus.State = GithubStatus.Success

  @TaskAction
  def run(): Unit = {
    val path = s"repos/$repoSlug/statuses/$commit"
    val body =
      s"""
        |{
        |  "state": "${commitState.value}",
        |  "target_url": "$targetUrl",
        |  "description": "$statusDescription",
        |  "context": "$context"
        |}
      """.stripMargin
    execute(path, body)
  }


  /**
    * Makes a POST request to GitHub's API with path and body.
    * E.g. "repos/mesosphere/marathon/pulls/5513/reviews" would post the body as a
    * comment.
    *
    * @param path The API path. See path in
    *   https://developer.github.com/v3/pulls/reviews/#create-a-pull-request-review
    *   for an example.
    * @param body The body of the post request.
    */
  def execute(path:String, body: String): Unit = {
    val GITHUB_API_TOKEN =
      sys.env.getOrElse("GIT_PASSWORD", throw new IllegalArgumentException("GIT_PASSWORD environment variable was not set."))
    val GITHUB_API_USER =
      sys.env.getOrElse("GIT_USER", throw new IllegalArgumentException("GIT_USER environment variable was not set."))

    Http(s"https://api.github.com/$path")
      .auth(GITHUB_API_USER, GITHUB_API_TOKEN)
      .timeout(connTimeoutMs = 5000, readTimeoutMs = 100000)
      .postData(body)
      .asString
      .throwError
  }
}
