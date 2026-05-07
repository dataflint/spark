package org.apache.spark.dataflint.api

import org.apache.spark.dataflint.listener.DataflintStore
import org.apache.spark.sql.execution.ui.SQLAppStatusListener
import org.apache.spark.ui.{SparkUI, WebUIPage, WebUITab}

/**
 * Spark 4.x implementation of DataflintPageFactory.
 *
 * Unlike the Spark 3 factory, this implementation does NOT serve JSON via
 * `WebUIPage` subclasses — those would tie the bytecode to either
 * jakarta.servlet or javax.servlet at compile time, but the same artifact
 * needs to load on both stock Spark 4 (jakarta) and Databricks Runtime 17.3
 * (javax). Instead, the factory advertises [[usesReflectiveEndpoints]] and
 * wires every endpoint through a reflective Jetty handler in
 * [[DataflintReflectiveServletBuilder]]. The `createXxxPage` methods are
 * therefore never called on the Spark 4 path and throw if invoked.
 */
class Spark4PageFactory extends DataflintPageFactory {

  override def createDataFlintTab(ui: SparkUI): WebUITab = {
    new DataFlintTab(ui)
  }

  // The Spark 4 loader path skips WebUIPage instantiation entirely (see
  // usesReflectiveEndpoints below). These methods exist only because the trait
  // is shared with Spark 3; calling them would mean the loader took the wrong
  // branch.
  private def webUIPageNotUsed: Nothing =
    throw new UnsupportedOperationException(
      "WebUIPage is not used on Spark 4 — endpoints are attached reflectively via " +
        "DataflintReflectiveServletBuilder.attachAllEndpoints"
    )

  override def createApplicationInfoPage(ui: SparkUI, dataflintStore: DataflintStore): WebUIPage = webUIPageNotUsed

  override def createCachedStoragePage(ui: SparkUI, dataflintStore: DataflintStore): WebUIPage = webUIPageNotUsed

  override def createIcebergPage(ui: SparkUI, dataflintStore: DataflintStore): WebUIPage = webUIPageNotUsed

  override def createDeltaLakeScanPage(ui: SparkUI, dataflintStore: DataflintStore): WebUIPage = webUIPageNotUsed

  override def createSQLMetricsPage(ui: SparkUI, sqlListener: () => Option[SQLAppStatusListener]): WebUIPage = webUIPageNotUsed

  override def createSQLPlanPage(ui: SparkUI, dataflintStore: DataflintStore, sqlListener: () => Option[SQLAppStatusListener]): WebUIPage = webUIPageNotUsed

  override def createSQLStagesRddPage(ui: SparkUI): WebUIPage = webUIPageNotUsed

  override def addStaticHandler(ui: SparkUI, resourceBase: String, contextPath: String): Unit = {
    DataflintJettyUtils.addStaticHandler(ui, resourceBase, contextPath)
  }

  override def getTabs(ui: SparkUI): Seq[WebUITab] = {
    ui.getTabs.toSeq
  }

  override def usesReflectiveEndpoints: Boolean = true

  override def attachReflectiveEndpoints(ui: SparkUI,
                                         dataflintStore: DataflintStore,
                                         sqlListener: () => Option[SQLAppStatusListener]): Unit = {
    DataflintReflectiveServletBuilder.attachAllEndpoints(ui, dataflintStore, sqlListener, ui.basePath)
  }
}