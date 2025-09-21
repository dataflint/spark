package org.apache.spark.dataflint.api

import org.apache.spark.dataflint.listener.DataflintStore
import org.apache.spark.sql.execution.ui.SQLAppStatusListener
import org.apache.spark.ui.{SparkUI, WebUIPage, WebUITab}

/**
 * Abstract factory for creating Dataflint UI components.
 * This allows version-specific implementations for different Spark versions.
 */
abstract class DataflintPageFactory {
  
  def createDataFlintTab(ui: SparkUI): WebUITab
  
  def createApplicationInfoPage(ui: SparkUI, dataflintStore: DataflintStore): WebUIPage
  
  def createCachedStoragePage(ui: SparkUI, dataflintStore: DataflintStore): WebUIPage
  
  def createIcebergPage(ui: SparkUI, dataflintStore: DataflintStore): WebUIPage
  
  def createSQLMetricsPage(ui: SparkUI, sqlListener: () => Option[SQLAppStatusListener]): WebUIPage
  
  def createSQLPlanPage(ui: SparkUI, dataflintStore: DataflintStore, sqlListener: () => Option[SQLAppStatusListener]): WebUIPage
  
  def createSQLStagesRddPage(ui: SparkUI): WebUIPage
  
  def addStaticHandler(ui: SparkUI, resourceBase: String, contextPath: String): Unit
  
  def getTabs(ui: SparkUI): Seq[WebUITab]
}
