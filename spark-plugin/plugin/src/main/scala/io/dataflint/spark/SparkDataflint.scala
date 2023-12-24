package io.dataflint.spark

import org.apache.spark.{SparkContext, DataflintSparkUILoader}

object SparkDataflint {
  def install(context: SparkContext): Unit = {
      DataflintSparkUILoader.install(context)
  }
}
