package io.dataflint.spark

import org.apache.spark.SparkContext
import org.apache.spark.dataflint.DataflintSparkUILoader

object SparkDataflint {
  def install(context: SparkContext): Unit = {
      DataflintSparkUILoader.install(context)
  }
}
