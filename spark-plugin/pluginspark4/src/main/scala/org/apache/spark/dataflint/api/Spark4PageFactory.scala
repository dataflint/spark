package org.apache.spark.dataflint.api

import org.apache.spark.dataflint.listener.DataflintStore
import org.apache.spark.sql.execution.ui.SQLAppStatusListener
import org.apache.spark.ui.{SparkUI, WebUIPage, WebUITab}

/**
 * Spark 4.x implementation of DataflintPageFactory using jakarta.servlet API
 */
class Spark4PageFactory extends DataflintPageFactory {
  
  override def createDataFlintTab(ui: SparkUI): WebUITab = {
    new DataFlintTab(ui)
  }
  
  override def createApplicationInfoPage(ui: SparkUI, dataflintStore: DataflintStore): WebUIPage = {
    new DataflintApplicationInfoPage(ui, dataflintStore)
  }
  
  override def createCachedStoragePage(ui: SparkUI, dataflintStore: DataflintStore): WebUIPage = {
    new DataflintCachedStoragePage(ui, dataflintStore)
  }
  
  override def createIcebergPage(ui: SparkUI, dataflintStore: DataflintStore): WebUIPage = {
    new DataflintIcebergPage(ui, dataflintStore)
  }
  
  override def createDeltaLakeScanPage(ui: SparkUI, dataflintStore: DataflintStore): WebUIPage = {
    new DataflintDeltaLakeScanPage(ui, dataflintStore)
  }
  
  override def createSQLMetricsPage(ui: SparkUI, sqlListener: () => Option[SQLAppStatusListener]): WebUIPage = {
    new DataflintSQLMetricsPage(ui, sqlListener)
  }
  
  override def createSQLPlanPage(ui: SparkUI, dataflintStore: DataflintStore, sqlListener: () => Option[SQLAppStatusListener]): WebUIPage = {
    new DataflintSQLPlanPage(ui, dataflintStore, sqlListener)
  }
  
  override def createSQLStagesRddPage(ui: SparkUI): WebUIPage = {
    new DataflintSQLStagesRddPage(ui)
  }
  
  override def addStaticHandler(ui: SparkUI, resourceBase: String, contextPath: String): Unit = {
    DataflintJettyUtils.addStaticHandler(ui, resourceBase, contextPath)
  }
  
  override def getTabs(ui: SparkUI): Seq[WebUITab] = {
    ui.getTabs.toSeq
  }
}
