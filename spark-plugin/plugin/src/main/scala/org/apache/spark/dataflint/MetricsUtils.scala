package org.apache.spark.dataflint

import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

/**
 * Utilities for creating and posting SQL metrics across different Spark versions.
 *
 * Spark's SQLMetrics API changed across versions (3.0 → 3.5 → 4.x) and Databricks
 * runtime. Each factory method tries the standard API first, then falls back to
 * reflection-based alternatives for compatibility.
 */
object MetricsUtils {

  /**
   * Post driver-side metric updates to the SQL listener.
   * Used for metrics computed on the driver (e.g., rddId) rather than accumulated
   * across tasks. Only posts if there's an active SQL execution context.
   */
  def postDriverMetrics(sparkContext: SparkContext, metrics: SQLMetric*): Unit = {
    try {
      val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
      if (executionId != null) {
        SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics)
      }
    } catch {
      case _: Exception => // Spark 3.1 SqlResource may NPE on unposted metrics; swallow to avoid breaking the query
    }
  }

  /**
   * Create a "size" metric (displayed as bytes in Spark UI).
   * Falls back across Spark versions:
   *   1. SQLMetrics.createSizeMetric (standard API)
   *   2. new SQLMetric("size", 0L) + register (older Spark)
   *   3. SQLMetrics.createMetric (Databricks runtime)
   */
  def getSizeMetric(name: String)(implicit sparkContext: SparkContext): (String, SQLMetric) = {
    name -> {
      try {
        SQLMetrics.createSizeMetric(sparkContext, name)
      } catch {
        case _: NoSuchMethodError =>
          try {
            val metric = new SQLMetric("size", 0L)
            metric.register(sparkContext, Some(name), countFailedValues = false)
            metric
          } catch {
            case _: NoSuchMethodError =>
              SQLMetrics.createMetric(sparkContext, name)
          }
      }
    }
  }

  /**
   * Create an "average" metric (displayed as average value in Spark UI).
   * Same fallback chain as getSizeMetric for cross-version compatibility.
   */
  def getAverageMetric(name: String)(implicit sparkContext: SparkContext): (String, SQLMetric) = {
    name -> {
      try {
        SQLMetrics.createAverageMetric(sparkContext, name)
      } catch {
        case _: NoSuchMethodError =>
          try {
            val metric = new SQLMetric("average", 0L)
            metric.register(sparkContext, Some(name), countFailedValues = false)
            metric
          } catch {
            case _: NoSuchMethodError =>
              SQLMetrics.createMetric(sparkContext, name)
          }
      }
    }
  }

  /**
   * Create a "timing" metric (displayed as milliseconds with total/min/med/max in Spark UI).
   * Used by TimedExec for the "duration" metric.
   * Same fallback chain as getSizeMetric for cross-version compatibility.
   */
  def getTimingMetric(name: String)(implicit sparkContext: SparkContext): (String, SQLMetric) = {
    name -> {
      try {
        SQLMetrics.createTimingMetric(sparkContext, name)
      } catch {
        case _: NoSuchMethodError =>
          try {
            val metric = new SQLMetric("timing", 0L)
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