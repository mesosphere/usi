package mesosphere.gradle.aws

import java.util.concurrent.CompletableFuture

import org.apache.commons.io.FilenameUtils
import com.typesafe.scalalogging.StrictLogging
import org.gradle.api.DefaultTask
import org.gradle.api.file.FileTree
import org.gradle.api.tasks.TaskAction
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{PutObjectRequest, PutObjectResponse}

import scala.beans.BeanProperty

/**
  * The S3Upload task defines a basic uploader for AWS S3.
  */
class S3Upload extends DefaultTask with StrictLogging {

  lazy val s3: S3AsyncClient = S3AsyncClient.builder().region(Region.of(region)).build()

  /**
    * Defines the AWS region. Defaults to us-west-2.
    */
  @BeanProperty var region: String = "us-west-2"

  /**
    * The bucket name for all uploaded objects.
    */
  @BeanProperty var bucket: String = ""

  /**
    * The prefix used for all files to create the key. The S3 object key will be "$prefix/$filePath"
    *
    * Example:
    *
    * Given the prefix is "foo/bar"
    * And we have a file "build/tests/failed.xml"
    * And source points to "build/"
    * Then the object key for "failed.xml" will be "foo/bar/tests/failed.xml"
    */
  @BeanProperty var prefix: String = ""

  /**
    * Defines the root with filters for all files that should be uploaded to [[bucket]].
    */
  @BeanProperty var source: FileTree = null

  @TaskAction
  def run(): Unit = {
    logger.info(s"Uploading files to $bucket with prefix $prefix")

    var pendingUploads = Vector.empty[CompletableFuture[PutObjectResponse]]
    source.visit { fileDetails =>
      if (!fileDetails.isDirectory) {
        val file = fileDetails.getRelativePath()
        val key = s"$prefix/$file"
        val request = PutObjectRequest.builder()
          .bucket(bucket)
          .key(key)
          .contentType(ContentType.of(fileDetails.getName))
          .build()
        val body = AsyncRequestBody.fromFile(fileDetails.getFile)
        pendingUploads = pendingUploads :+ s3.putObject(request, body)
      }
    }

    logger.info(s"Waiting for all ${pendingUploads.size} to finish.")
    // Any exception is propagated.
    pendingUploads.foreach(_.join())

    logger.info("Done.")
  }

}

/**
  * Defines a mapping from file extensions to MIME types, ie Content-type metadata.
  */
object ContentType {
  val contentTypeMap = Map(
    "html" -> "text/html",
    "css" -> "text/css",
    "js" -> "application/x-javascript",
    "xml" -> "text/xml"
  )

  def of(fileName: String): String = contentTypeMap.getOrElse(FilenameUtils.getExtension(fileName), "text/plain")
}
