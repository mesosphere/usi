package mesosphere.gradle.travis

import scala.util.Properties

/**
  * Exposes the Travis CI build environment according to o
  *
  * @see [[https://docs.travis-ci.com/user/environment-variables/#default-environment-variables]]
  */
object TravisEnvironment {

  /**
    * @return the repo slug, eg mesosphere/usi, of this build.
    */
  def repoSlug(): String = Properties.envOrElse("TRAVIS_REPO_SLUG", "unknown")

  /*
   * @return the name of the branch that is build.
   */
  def branch(): String = {
    if (isPullRequestBuild()) sys.env("TRAVIS_PULL_REQUEST_BRANCH")
    else Properties.envOrElse("TRAVIS_BRANCH", "unknown")
  }

  /**
    * @return the commit SHA of this build.
    */
  def commit(): String = Properties.envOrElse("TRAVIS_COMMIT", "unknown")

  /**
    * @return the number of the build.
    */
  def buildNumber(): String = Properties.envOrElse("TRAVIS_BUILD_NUMBER", "unknown")

  /**
    * @return true if this is a build of a pull request.
    */
  def isPullRequestBuild(): Boolean = Properties.envOrElse("TRAVIS_PULL_REQUEST", "false") != "false"

  /**
    * @return true if this is a build of the master branch.
    */
  def isMasterBuild(): Boolean = !isPullRequestBuild() && branch() == "master"

  /**
    * For Travis CI this build means that it's not a build of a fork. Thus credentials are exposed in variables.
    *
    * @return true if the build is for trusted source.
    */
  def isSecureBuild(): Boolean = Properties.envOrElse("TRAVIS_SECURE_ENV_VARS", "false") == "true"
}
