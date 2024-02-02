package org.apache.spark.dataflint

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerApplicationStart}
import org.apache.spark.sql.execution.ui.SQLAppStatusStore
import org.apache.spark.status.ElementTrackingStore

import java.nio.file.{Files, Paths}
import java.util.concurrent.atomic.AtomicBoolean

class DataflintListener(context: SparkContext) extends SparkListener with Logging {
  private val applicationEnded = new AtomicBoolean(false);
  private val dataflintBucketName = "dataflint-uploads-test"

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    if (applicationEnded.getAndSet(true)) {
      // onApplicationEnd could be called multiple times, and this is a defence against that case
      return;
    }
    try {
      logInfo("DataFlint run exporter started")
      val startTimeMillis = System.currentTimeMillis()

      def runId = context.conf.get("spark.dataflint.runId")

      val isLocalMode = context.getConf.get("spark.dataflint.localMode", "false") == "true"

      val tokenParts = context.conf.get("spark.dataflint.token").split("-")
      val accessKey = tokenParts(0)
      val secretAccessKey = tokenParts(1)

      val baseFilePath = s"$accessKey/$runId"

      val sqlStore = new SQLAppStatusStore(context.statusStore.store, None)

      val s3Uploader = new S3Uploader(accessKey, secretAccessKey, isLocalMode)
      val data = new StoreDataExtractor(context.statusStore).extract()
      val metadata = new StoreMetadataExtractor(context.statusStore, sqlStore, context.getConf).extract(runId, accessKey, applicationEnd.time)
      // local mode is for local development and testing purposes
      if (!doesAWSCredentialsClassExist()) {
        logError("Failed to export run to dataflint SaaS, please make sure you have the aws-java-sdk-s3 dependency installed in your project")
        return;
      }

      val dataJson = SparkRunSerializer.serialize(data)
      s3Uploader.uploadToS3(dataJson, dataflintBucketName, baseFilePath + ".data.json.gz", shouldGzip = true)

      val metaJson = SparkMetadataSerializer.serialize(metadata)
      s3Uploader.uploadToS3(metaJson, dataflintBucketName, baseFilePath + ".meta.json", shouldGzip = false)

      val endTimeMillis = System.currentTimeMillis()
      val durationMs = endTimeMillis - startTimeMillis
      if (isLocalMode) {
        logInfo(s"Exported run to dataflint SaaS successfully! exporting took ${durationMs}ms, link to job: http://localhost:8000/dataflint-spark-ui/history/${accessKey}-${runId}/dataflint/")
      } else {
        logInfo(s"Exported run to dataflint SaaS successfully! exporting took ${durationMs}ms, link to job: http://sparkui.dataflint,io/history/${accessKey}-${runId}/dataflint/")
      }
    } catch {
      case exception: Throwable => logError("Failed to export run to dataflint SaaS", exception)
        return;
    }

    def doesAWSCredentialsClassExist(): Boolean = {
      try {
        Class.forName("com.amazonaws.auth.AWSCredentials")
        true
      } catch {
        case _: ClassNotFoundException => false
      }
    }
  }
}
