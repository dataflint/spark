package org.apache.spark.dataflint

import org.apache.spark.SparkContext
import org.apache.spark.dataflint.api.Spark3PageFactory
import org.apache.spark.ui.SparkUI

/**
 * Spark 3.x specific implementation of DataflintSparkUILoader that provides backward compatibility
 */
object DataflintSparkUILoader {
  
  private val pageFactory = new Spark3PageFactory()
  
  def install(context: SparkContext): String = {
    // Call the common implementation with Spark 3 factory
    new org.apache.spark.dataflint.DataflintSparkUICommonInstaller().install(context, pageFactory)
  }

  def loadUI(ui: SparkUI): String = {
    // Call the common implementation with Spark 3 factory
    new org.apache.spark.dataflint.DataflintSparkUICommonInstaller().loadUI(ui, pageFactory)
  }
}
