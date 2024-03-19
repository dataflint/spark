package org.apache.spark.dataflint.saas

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.ObjectMetadata
import org.apache.spark.internal.Logging

import java.io.ByteArrayInputStream

class S3Uploader(accessKeyId: String, secretAccessKey: String, mode: String) extends Logging {
  val credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey)
  private val bucketName = "dataflint-upload-" + mode

  val s3client: AmazonS3 = {
    var builder = AmazonS3ClientBuilder.standard()
      .withCredentials(new AWSStaticCredentialsProvider(credentials))

    if(mode == "local") {
      logInfo(s"Uploading to S3 with localstack")
      builder = builder.withEndpointConfiguration(new EndpointConfiguration("s3.localhost.localstack.cloud:4566", Regions.US_EAST_1.getName))
    } else {
      builder = builder.enableAccelerateMode()

    }

    builder.build()
  }

  def uploadToS3(jsonContent: String, fileKey: String, shouldGzip: Boolean): Unit = {
    try {
      val startTimeMillis = System.currentTimeMillis()

      val metadata = new ObjectMetadata()
      val jsonToSend = if(shouldGzip) GZipUtils.compressString(jsonContent) else jsonContent.getBytes("UTF-8")
      if(shouldGzip) {
        metadata.setContentType("application/x-gzip")
      } else {
        metadata.setContentType("application/json")
      }
      metadata.setContentLength(jsonToSend.length)

      val inputStream = new ByteArrayInputStream(jsonToSend)

      s3client.putObject(bucketName, fileKey, inputStream, metadata)
      val endTimeMillis = System.currentTimeMillis()
      val durationMs = endTimeMillis - startTimeMillis
      logDebug(s"Upload file $fileKey took ${durationMs}ms")
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}
