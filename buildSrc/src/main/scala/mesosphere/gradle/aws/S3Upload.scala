package mesosphere.gradle.aws

import java.io.File

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.transfer.Transfer.TransferState
import com.amazonaws.services.s3.transfer.{TransferManager, TransferManagerBuilder}
import org.gradle.api.DefaultTask
import org.gradle.api.tasks.TaskAction

import scala.beans.BeanProperty

class S3Upload extends DefaultTask {

  val credentialProviderChain = new DefaultAWSCredentialsProviderChain()

  @BeanProperty
  var bucket: String = ""

  @BeanProperty
  var prefix: String = ""

  @BeanProperty
  var source: File = null

  @TaskAction
  def run(): Unit = {
    val tx = TransferManagerBuilder.standard().withS3Client(createS3Client()).build()
    val upload = tx.uploadDirectory(bucket, prefix, source, true)

    while(!upload.isDone()) {
      val progress = upload.getProgress()
      println(s"${progress.getPercentTransferred()} % ${upload.getState()}")
      Thread.sleep(2000)
    }

    tx.shutdownNow(true)
    assert(upload.getState() == TransferState.Completed, s"Upload finished with ${upload.getState()}")

    tx.shutdownNow(true)
  }

  /**
    *  Returns AWS S3 client.
    */
  def createS3Client(): AmazonS3Client = {
    new AmazonS3Client(new DefaultAWSCredentialsProviderChain())
  }

}
