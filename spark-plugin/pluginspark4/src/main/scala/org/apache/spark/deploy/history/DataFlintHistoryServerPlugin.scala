package org.apache.spark.deploy.history

import org.apache.spark.SparkConf
import org.apache.spark.dataflint.DataflintSparkUILoader
import org.apache.spark.dataflint.SqlMetricsPatch
import org.apache.spark.dataflint.listener.DataflintListener
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.status.{AppHistoryServerPlugin, ElementTrackingStore, LiveRDDsListener}
import org.apache.spark.ui.SparkUI

class DataFlintHistoryServerPlugin extends AppHistoryServerPlugin {
  // Reference shared patch object to trigger patch on plugin instantiation
  private val _ = SqlMetricsPatch.sqlMetricsPatchApplied

  override def createListeners(conf: SparkConf, store: ElementTrackingStore): Seq[SparkListener] = {
    Seq(new DataflintListener(store), new LiveRDDsListener(store))
  }

  override def setupUI(ui: SparkUI): Unit = {
    DataflintSparkUILoader.loadUI(ui)
  }
}