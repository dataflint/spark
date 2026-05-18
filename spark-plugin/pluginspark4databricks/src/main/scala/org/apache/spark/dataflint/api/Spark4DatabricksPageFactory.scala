package org.apache.spark.dataflint.api

import org.apache.spark.ui.SparkUI

/**
 * Databricks variant of [[Spark4PageFactory]]. The parent class skips the
 * DataFlint UI on any Databricks runtime to avoid the jakarta.servlet
 * NoClassDefFoundError on DBR 17.3 (see issue #47). This subclass inverts
 * the check: enable UI ONLY on Databricks (where the javax-shaded bytecode
 * matches the runtime). If this jar is installed on stock Spark 4 by
 * mistake, the UI is silently skipped instead of crashing.
 */
class Spark4DatabricksPageFactory extends Spark4PageFactory {
  override def isUISupported(ui: SparkUI): Boolean =
    ui.conf.getOption("spark.databricks.clusterUsageTags.cloudProvider").isDefined
}
