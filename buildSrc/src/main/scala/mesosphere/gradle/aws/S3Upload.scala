package mesosphere.gradle.aws

import java.io.File

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.transfer.Transfer.TransferState
import com.amazonaws.services.s3.transfer.TransferManagerBuilder
import com.typesafe.scalalogging.StrictLogging
import org.gradle.api.DefaultTask
import org.gradle.api.tasks.TaskAction

import scala.beans.BeanProperty

class S3Upload extends DefaultTask with StrictLogging {

  val credentialProviderChain = new DefaultAWSCredentialsProviderChain()

  @BeanProperty var region: String = "us-west-2"

  @BeanProperty var bucket: String = ""

  @BeanProperty var prefix: String = ""

  @BeanProperty var source: File = null

  @TaskAction
  def run(): Unit = {
    logger.info(s"Uploading files from $source to $bucket with prefix $prefix")

    val tx = TransferManagerBuilder.standard().withS3Client(createS3Client()).build()
    val upload = tx.uploadDirectory(bucket, prefix, source, true)

    while(!upload.isDone()) {
      val progress = upload.getProgress()
      logger.info(s"${progress.getPercentTransferred()} % ${upload.getState()}")
      Thread.sleep(2000)
    }

    tx.shutdownNow(true)
    assert(upload.getState() == TransferState.Completed, s"Upload finished with ${upload.getState()}")

    tx.shutdownNow(true)
    logger.info("Done.")
  }

  /**
    *  Returns AWS S3 client.
    */
  def createS3Client() = {
    AmazonS3ClientBuilder.standard().withCredentials(credentialProviderChain).withRegion(region).build()
  }

}
