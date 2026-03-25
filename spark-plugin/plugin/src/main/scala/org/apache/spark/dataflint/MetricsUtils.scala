package org.apache.spark.dataflint

import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

object MetricsUtils {
  def getTimingMetric(name: String)(implicit sparkContext: SparkContext): (String, SQLMetric) = {
    name -> {
      try {
        SQLMetrics.createTimingMetric(sparkContext, name)
      } catch {
        case _: NoSuchMethodError =>
          val metric = new SQLMetric("timing", -1L)
          metric.register(sparkContext, Some(name), countFailedValues = false)
          metric
      }
    }
  }
}
