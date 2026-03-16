package org.apache.spark.sql.execution.python

import org.apache.spark.dataflint.DataFlintRDDUtils
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

/**
 * DataFlint instrumented PythonMapInArrowExec for Spark 3.5.x.
 * Replaces the original PythonMapInArrowExec in the physical plan.
 *
 * Note: PythonMapInArrowExec was introduced in Spark 3.3 (SPARK-37227).
 * This class is only loaded/instantiated when the Spark version is 3.5+,
 * guarded by the try-catch(NoClassDefFoundError) in DataFlintInstrumentationExtension.
 */
class DataFlintPythonMapInArrowExec_3_5 private(
                                                 func: Expression,
                                                 output: Seq[Attribute],
                                                 child: SparkPlan,
                                                 isBarrier: Boolean)
  extends PythonMapInArrowExec(func, output, child, isBarrier) with Logging {

  override def nodeName: String = "DataFlintMapInArrow"

  logInfo("DataFlint MapInArrow (Spark 3.5) is connected")

  override lazy val metrics: Map[String, SQLMetric] = pythonMetrics ++ Map(
    "duration" -> SQLMetrics.createTimingMetric(sparkContext, "duration")
  )

  override protected def doExecute(): RDD[InternalRow] =
    DataFlintRDDUtils.withDurationMetric(super.doExecute(), longMetric("duration"))

  override protected def withNewChildInternal(newChild: SparkPlan): DataFlintPythonMapInArrowExec_3_5 =
    DataFlintPythonMapInArrowExec_3_5(func, output, newChild, isBarrier)

  override def canEqual(other: Any): Boolean = other.isInstanceOf[DataFlintPythonMapInArrowExec_3_5]

  override def equals(other: Any): Boolean = other.isInstanceOf[DataFlintPythonMapInArrowExec_3_5] && super.equals(other)

  override def hashCode: Int = super.hashCode
}

object DataFlintPythonMapInArrowExec_3_5 {
  def apply(
             func: Expression,
             output: Seq[Attribute],
             child: SparkPlan,
             isBarrier: Boolean): DataFlintPythonMapInArrowExec_3_5 =
    new DataFlintPythonMapInArrowExec_3_5(func, output, child, isBarrier)
}