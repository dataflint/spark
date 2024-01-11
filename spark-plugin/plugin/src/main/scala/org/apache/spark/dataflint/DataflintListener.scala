package org.apache.spark.dataflint

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.status.ElementTrackingStore

import java.util.concurrent.atomic.AtomicBoolean

class DataflintListener(context: SparkContext) extends SparkListener with Logging {
  private val applicationEnded = new AtomicBoolean(false);
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    if(applicationEnded.getAndSet(true)) {
      // onApplicationEnd could be called multiple times, and this is a defence against that case
      return;
    }
    logInfo("DataFlint run exporter started")
    val startTimeMillis = System.currentTimeMillis()
    try {
      def runId = context.conf.get("spark.dataflint.runId")

      val data = new StoreDataExtractor(context.statusStore).extract()
      val metadata = new StoreMetadataExtractor(context.statusStore, context.getConf).extract(runId, applicationEnd.time)
      // local mode is for local development and testing purposes
        if(context.getConf.get("spark.dataflint.localMode", "false") == "true") {
          SparkRunSerializer.serializeAndSave(data, s"/tmp/dataflint-export/${runId}.data.json")
          SparkMetadataSerializer.serializeAndSave(metadata, s"/tmp/dataflint-export/${runId}.meta.json")
        } else {
          // TODO: add SaaS S3 export code
        }
     } catch {
       case exception: Throwable => logError("Failed to export run to dataflint SaaS", exception)
       return;
     }

    val endTimeMillis = System.currentTimeMillis()
    val durationMs = endTimeMillis - startTimeMillis
    logInfo(s"Exported run to dataflint SaaS successfully! exporting took ${durationMs}ms")
  }
}
