package org.apache.spark.dataflint

import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.metric.SQLMetric

object MetricsUtils {
  def getTimingMetric(name: String)(implicit sparkContext:SparkContext): (String, SQLMetric) = {
    name -> {
      val metric = new SQLMetric("timing", -1L)
      metric.register(sparkContext, Some(name), countFailedValues = false)
      metric
    }
  }
}
