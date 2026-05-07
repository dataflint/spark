package org.apache.spark.dataflint.api

import org.apache.spark.dataflint.listener.DataflintStore
import org.apache.spark.sql.execution.ui.SQLAppStatusListener
import org.apache.spark.ui.{SparkUI, WebUIPage, WebUITab}

/**
 * Abstract factory for creating Dataflint UI components.
 * This allows version-specific implementations for different Spark versions.
 *
 * Spark 3 implementations build a `DataFlintTab` and attach `WebUIPage` instances
 * to it (see [[createApplicationInfoPage]] etc.). Spark 4 cannot use that flow
 * because some target runtimes (e.g. Databricks Runtime 17.3) ship javax.servlet
 * instead of jakarta.servlet, which breaks JVM verification of any class that
 * extends WebUIPage with jakarta-typed `render`/`renderJson`. The Spark 4 path
 * therefore overrides [[usesReflectiveEndpoints]] to `true` and supplies all
 * endpoints through [[attachReflectiveEndpoints]] instead.
 */
abstract class DataflintPageFactory {

  def createDataFlintTab(ui: SparkUI): WebUITab

  def createApplicationInfoPage(ui: SparkUI, dataflintStore: DataflintStore): WebUIPage

  def createCachedStoragePage(ui: SparkUI, dataflintStore: DataflintStore): WebUIPage

  def createIcebergPage(ui: SparkUI, dataflintStore: DataflintStore): WebUIPage

  def createDeltaLakeScanPage(ui: SparkUI, dataflintStore: DataflintStore): WebUIPage

  def createSQLMetricsPage(ui: SparkUI, sqlListener: () => Option[SQLAppStatusListener]): WebUIPage

  def createSQLPlanPage(ui: SparkUI, dataflintStore: DataflintStore, sqlListener: () => Option[SQLAppStatusListener]): WebUIPage

  def createSQLStagesRddPage(ui: SparkUI): WebUIPage

  def addStaticHandler(ui: SparkUI, resourceBase: String, contextPath: String): Unit

  def getTabs(ui: SparkUI): Seq[WebUITab]

  def isUISupported(ui: SparkUI): Boolean = true

  /**
   * If true, the common loader will skip the per-page `attachPage` calls and
   * rely entirely on [[attachReflectiveEndpoints]] to wire the JSON API. Used
   * by Spark 4 to bypass `WebUIPage` (which is jakarta.servlet-typed and breaks
   * on javax-only runtimes like Databricks Runtime 17.3).
   */
  def usesReflectiveEndpoints: Boolean = false

  /**
   * Attach DataFlint JSON endpoints reflectively. Default no-op; overridden by
   * Spark 4 to install a Proxy-backed servlet for each endpoint.
   */
  def attachReflectiveEndpoints(ui: SparkUI,
                                dataflintStore: DataflintStore,
                                sqlListener: () => Option[SQLAppStatusListener]): Unit = ()
}