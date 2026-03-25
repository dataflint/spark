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
          try {
            val metric = new SQLMetric("timing", -1L)
            metric.register(sparkContext, Some(name), countFailedValues = false)
            metric
          } catch {
            case _: NoSuchMethodError =>
              // Databricks custom runtime removed the 2-arg SQLMetric constructor;
              // fall back to a sum metric (functional, just displays as sum not timing)
              SQLMetrics.createMetric(sparkContext, name)
          }
      }
    }
  }
}
