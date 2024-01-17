package org.apache.spark.dataflint

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ObjectMetadata
import org.apache.spark.internal.Logging

import java.io.ByteArrayInputStream

class S3Uploader(accessKeyId: String, secretAccessKey: String) extends Logging {
  val credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey)

  val s3client: AmazonS3 = AmazonS3ClientBuilder.standard()
    .withCredentials(new AWSStaticCredentialsProvider(credentials))
    .enableAccelerateMode()
    .build()

  def uploadToS3(jsonContent: String, bucketName: String, fileKey: String, shouldGzip: Boolean): Unit = {
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
