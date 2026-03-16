/*
 * DataFlint instrumented MapInBatchExec for Spark 3.5.x
 *
 * Spark 3.5 characteristics:
 *   - Major refactor: MapInBatchEvaluatorFactory introduced (SPARK-44361)
 *   - PartitionEvaluator API support
 *   - Barrier mode support (SPARK-42896) — isBarrier field on MapInPandasExec
 *   - JobArtifactSet for Spark Connect artifact management
 *   - arrowUseLargeVarTypes config (SPARK-39979)
 *   - ArrowPythonRunner.getPythonRunnerConfMap replaces ArrowUtils.getPythonRunnerConfMap (SPARK-44532)
 *   - MapInPandasExec fields: (func, output, child, isBarrier)
 *   - PythonMapInArrowExec fields: (func, output, child, isBarrier)
 *
 * Both classes extend the original exec nodes directly and wrap doExecute() with
 * DataFlintRDDUtils.withDurationMetric, delegating all execution logic to Spark.
 * This is safe on 3.5 since the constructor signatures match the compile-time artifacts.
 * On older Spark versions these classes are never instantiated (version-guarded by the extension).
 */
package org.apache.spark.sql.execution.python

import org.apache.spark.dataflint.DataFlintRDDUtils
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

/**
 * DataFlint instrumented MapInPandasExec for Spark 3.5.x.
 * Replaces the original MapInPandasExec in the physical plan.
 */
class DataFlintMapInPandasExec_3_5 private (
    func: Expression,
    output: Seq[Attribute],
    child: SparkPlan,
    isBarrier: Boolean)
  extends MapInPandasExec(func, output, child, isBarrier) with Logging {

  override def nodeName: String = "DataFlintMapInPandas"

  logInfo("DataFlint MapInPandas (Spark 3.5) is connected")

  override lazy val metrics: Map[String, SQLMetric] = pythonMetrics ++ Map(
    "duration" -> SQLMetrics.createTimingMetric(sparkContext, "duration")
  )

  override protected def doExecute(): RDD[InternalRow] =
    DataFlintRDDUtils.withDurationMetric(super.doExecute(), longMetric("duration"))

  override protected def withNewChildInternal(newChild: SparkPlan): DataFlintMapInPandasExec_3_5 =
    DataFlintMapInPandasExec_3_5(func, output, newChild, isBarrier)

  override def canEqual(other: Any): Boolean = other.isInstanceOf[DataFlintMapInPandasExec_3_5]
  override def equals(other: Any): Boolean = other.isInstanceOf[DataFlintMapInPandasExec_3_5] && super.equals(other)
  override def hashCode: Int = super.hashCode
}

object DataFlintMapInPandasExec_3_5 {
  def apply(
      func: Expression,
      output: Seq[Attribute],
      child: SparkPlan,
      isBarrier: Boolean): DataFlintMapInPandasExec_3_5 =
    new DataFlintMapInPandasExec_3_5(func, output, child, isBarrier)
}
