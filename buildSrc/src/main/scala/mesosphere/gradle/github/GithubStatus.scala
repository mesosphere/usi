package mesosphere.gradle.github

import org.gradle.api.DefaultTask
import org.gradle.api.tasks.TaskAction
import scalaj.http._

class GithubStatus extends DefaultTask {

  @TaskAction
  def run(): Unit = {
    val commit = sys.env.getOrElse("TRAVIS_COMMIT", throw new IllegalArgumentException("TRAVIS_COMMIT not set. Probably not running on Travis CI."))
    val path = s"repos/mesosphere/usi/status/$commit"
    val body =
      """
        |{
        |  "state": "success",
        |  "target_url": "https://examplebucket.s3-website-us-west-2.amazonaws.com/docs/doc1.html",
        |  "description": "The build succeeded!",
        |  "context": "mesosphere/tests"
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
