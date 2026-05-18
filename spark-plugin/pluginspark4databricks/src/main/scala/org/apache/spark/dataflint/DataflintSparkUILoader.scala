package org.apache.spark.dataflint

import org.apache.spark.SparkContext
import org.apache.spark.dataflint.api.Spark4DatabricksPageFactory
import org.apache.spark.ui.SparkUI

/**
 * Databricks variant of the Spark 4 loader. Identical to the pluginspark4
 * loader except it instantiates `Spark4DatabricksPageFactory`, which
 * inverts the Databricks UI gate so the shaded jar serves UI only on DBR.
 * Same FQN as the upstream loader so the shared `SparkDataflintPlugin`
 * entrypoint resolves it without any per-flavor wiring.
 */
object DataflintSparkUILoader {
  private val pageFactory = new Spark4DatabricksPageFactory()

  def install(context: SparkContext): String =
    new org.apache.spark.dataflint.DataflintSparkUICommonInstaller().install(context, pageFactory)

  def loadUI(ui: SparkUI): String =
    new org.apache.spark.dataflint.DataflintSparkUICommonInstaller().loadUI(ui, pageFactory)
}
