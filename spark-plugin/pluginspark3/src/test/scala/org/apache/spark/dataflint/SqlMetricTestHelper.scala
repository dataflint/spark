package org.apache.spark.dataflint

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.metric.SQLMetric
import org.scalatest.Assertions

case class MetricStats(total: Long, min: Long, med: Long, max: Long)

trait SqlMetricTestHelper extends Assertions {

  // Reads per-task total/min/med/max from the SQL status store for the given SQLMetric.
  // createTimingMetric records each task's elapsed time individually; the store formats them as:
  // "total (min, med, max (stageId: taskId))\nX ms (Y ms, Z ms, W ms (stage A.B: task C))"
  def metricMinMax(metric: SQLMetric)(implicit spark: SparkSession): MetricStats = {
    val sqlStore = spark.sharedState.statusStore
    val execData = sqlStore.executionsList().maxBy(_.executionId)
    val metricStr = sqlStore.executionMetrics(execData.executionId).getOrElse(metric.id, "")
    val pattern = """(\d+) ms \((\d+) ms, (\d+) ms, (\d+) ms""".r
    pattern.findFirstMatchIn(metricStr) match {
      case Some(m) => MetricStats(m.group(1).toLong, m.group(2).toLong, m.group(3).toLong, m.group(4).toLong)
      case None    => fail(s"Expected per-task timing breakdown but got: '$metricStr'")
    }
  }
}