package org.apache.spark.dataflint

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.status.ElementTrackingStore

class DataflintListener(context: SparkContext) extends SparkListener with Logging {
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    logInfo("DataFlint run exporter started")
    val startTimeMillis = System.currentTimeMillis()
    try {
        val kvStore = context.statusStore.store.asInstanceOf[ElementTrackingStore]
        val data = new StoreDataExtractor(kvStore).extractAllData()
        // local mode is for local development and testing purposes
        if(context.getConf.get("spark.dataflint.localMode", "false") == "true") {
          SparkRunSerializer.serializeAndSave(data, "/tmp/dataflint-export/sparkjob.json")
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
