package org.apache.spark.sql.execution.python

import org.apache.spark.dataflint.DataFlintRDDUtils
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

/**
 * DataFlint instrumented version of BatchEvalPythonExec for Spark 4.x.
 *
 * Instruments regular Python UDF (@udf / SQL_BATCHED_UDF) operations with a
 * duration metric by wrapping the parent's doExecute() RDD.
 * Constructor (udfs, resultAttrs, child) is stable across Spark 4.0–4.1.
 */
class DataFlintBatchEvalPythonExec private (
    udfs: Seq[PythonUDF],
    resultAttrs: Seq[Attribute],
    child: SparkPlan)
  extends BatchEvalPythonExec(udfs, resultAttrs, child) with Logging {

  override def nodeName: String = "DataFlintBatchEvalPython"

  override lazy val metrics: Map[String, SQLMetric] = pythonMetrics ++ Map(
    "duration" -> SQLMetrics.createTimingMetric(sparkContext, "duration")
  )

  override protected def doExecute(): RDD[InternalRow] =
    DataFlintRDDUtils.withDurationMetric(super.doExecute(), longMetric("duration"))

  override protected def withNewChildInternal(newChild: SparkPlan): DataFlintBatchEvalPythonExec =
    DataFlintBatchEvalPythonExec(udfs, resultAttrs, newChild)

  override def canEqual(other: Any): Boolean = other.isInstanceOf[DataFlintBatchEvalPythonExec]

  override def equals(other: Any): Boolean =
    other.isInstanceOf[DataFlintBatchEvalPythonExec] && super.equals(other)

  override def hashCode: Int = super.hashCode
}

object DataFlintBatchEvalPythonExec {
  def apply(
      udfs: Seq[PythonUDF],
      resultAttrs: Seq[Attribute],
      child: SparkPlan): DataFlintBatchEvalPythonExec =
    new DataFlintBatchEvalPythonExec(udfs, resultAttrs, child)
}