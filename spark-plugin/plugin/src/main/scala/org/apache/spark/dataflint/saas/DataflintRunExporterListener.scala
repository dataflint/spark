package org.apache.spark.dataflint.saas

import org.apache.spark.SparkContext
import org.apache.spark.dataflint.jobgroup.JobGroupExtractor
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.execution.ui.SQLAppStatusStore
import org.apache.spark.status.AppStatusStore

import java.util.concurrent.atomic.AtomicBoolean

class DataflintRunExporterListener(context: SparkContext) extends SparkListener with Logging {
  private val applicationEnded = new AtomicBoolean(false);

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    if (applicationEnded.getAndSet(true)) {
      // onApplicationEnd could be called multiple times, and this is a defence against that case
      return;
    }
    try {
      logInfo("DataFlint run exporter started")
      val startTimeMillis = System.currentTimeMillis()

      val sqlStore = new SQLAppStatusStore(context.statusStore.store, None)
      val groupExtractor = new JobGroupExtractor(context.statusStore, sqlStore)
      val groupList = groupExtractor.getGroupList()

      if (!doesAWSCredentialsClassExist()) {
        logError("Failed to export run to dataflint SaaS, please make sure you have the aws-java-sdk-s3 dependency installed in your project")
        return
      }

      val mode = context.getConf.get("spark.dataflint.mode", "prod")
      if (mode != "prod" && mode != "staging" && mode != "dev" && mode != "local") {
        logInfo("DataFlint run exporter is disabled in unknown mode")
        return
      }

      if(groupList.isEmpty) {
        processSparkJob(applicationEnd.time, mode, startTimeMillis, context.statusStore, sqlStore)
      }
      else {
        logInfo(s"DataFLint run exporter found ${groupList.length} groups")
        groupList.foreach(group => {
          val extractedGroup = groupExtractor.extract(group)
          logInfo(s"DataFlint run exporter processing group ${group}")
          processSparkJob(extractedGroup._3, mode, startTimeMillis, extractedGroup._1, extractedGroup._2)
        })
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

  private def processSparkJob(appEndTime: Long, mode :String, startTimeMillis: Long, statusStore: AppStatusStore, sqlStore: SQLAppStatusStore): Unit = {
    context.conf.set("spark.dataflint.runId", java.util.UUID.randomUUID.toString.replaceAll("-", ""))
    def runId = context.conf.get("spark.dataflint.runId")

    val tokenParts = context.conf.get("spark.dataflint.token").split("-")
    val accessKey = tokenParts(0)
    val secretAccessKey = tokenParts(1)

    val baseFilePath = s"$accessKey/$runId"


    val s3Uploader = new S3Uploader(accessKey, secretAccessKey, mode)
    val data = new StoreDataExtractor(statusStore).extract()
    val metadata = new StoreMetadataExtractor(statusStore, sqlStore, context.getConf).extract(runId, accessKey, appEndTime)
    // local mode is for local development and testing purposes

    val dataJson = SparkRunSerializer.serialize(data)
    s3Uploader.uploadToS3(dataJson, baseFilePath + ".data.json.gz", shouldGzip = true)

    val metaJson = SparkMetadataSerializer.serialize(metadata)
    s3Uploader.uploadToS3(metaJson, baseFilePath + ".meta.json", shouldGzip = false)

    val endTimeMillis = System.currentTimeMillis()
    val durationMs = endTimeMillis - startTimeMillis
    val urlPrefix = mode match {
      case "local" => "http://localhost:8000"
      case "staging" => "https://sparkui.staging.dataflint.io"
      case "dev" => "https://sparkui.dev.dataflint.io"
      case _ => "https://sparkui.dataflint.io"
    }
    logInfo(s"Exported run to dataflint SaaS successfully! exporting took ${durationMs}ms, link to job: ${urlPrefix}/dataflint-spark-ui/history/${accessKey}-${runId}/dataflint/")
  }
}
