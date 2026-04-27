package org.apache.spark.dataflint.executor

import org.apache.spark.SparkContext
import org.apache.spark.dataflint.listener.{DataflintExecutorMetadataEvent, DataflintExecutorMetadataInfo}

object DriverMetadataHelper {

  def isExecutorMetadataEnabled(sc: SparkContext): Boolean = {
    sc.conf.getBoolean("spark.dataflint.experimental.executor.metadata.enabled", defaultValue = false)
  }

  def postExecutorMetadataEvent(sc: SparkContext, info: DataflintExecutorMetadataInfo): Unit = {
    sc.listenerBus.post(DataflintExecutorMetadataEvent(info))
  }
}
