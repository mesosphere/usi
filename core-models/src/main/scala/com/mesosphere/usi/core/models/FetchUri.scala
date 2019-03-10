package com.mesosphere.usi.core.models

import java.net.URI

/**
  * Specification used by the [Mesos fetcher](http://mesos.apache.org/documentation/latest/fetcher/) to download
  * resources into the sandbox directory of a task in preparation of running the task. Defaults taken from Mesos
  * proto files.
  *
  * @param uri artifact URI to download
  * @param extract if true, the artifact will be treated as an archive an extracted to the sandbox. See the list
  *                of the supported archive types below
  * @param executable if true, the fetch result will be changed to be executable (by “chmod”) for every user
  * @param cache if true, the fetcher cache is to be used for the URI
  * @param outputFile if set, the fetcher will use that name for the copy stored in the sandbox directory
  */
case class FetchUri(
    uri: URI,
    extract: Boolean = true,
    executable: Boolean = false,
    cache: Boolean = false,
    outputFile: Option[String] = None) {
  if (extract) require(FetchUri.extractable(uri), s"$uri is not a supported extractable archive type")
}

object FetchUri {

  // Official extractable extensions of mesos fetcher: http://mesos.apache.org/documentation/latest/fetcher/
  val extractableTypes = Seq(".tar", ".tar.gz", ".tar.bz2", ".tar.xz", ".gz", ".tgz", ".tbz2", ".txz", ".zip")

  def extractable(uri: URI): Boolean = {
    extractableTypes.exists { fileType =>
      uri.getPath.endsWith(fileType)
    }
  }
}
