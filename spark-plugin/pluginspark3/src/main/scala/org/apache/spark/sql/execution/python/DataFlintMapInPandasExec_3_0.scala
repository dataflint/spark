/*
 * DataFlint instrumented MapInBatchExec for Spark 3.0.x / 3.1.x / 3.2.x
 *
 * Spark 3.0–3.2 characteristics:
 *   - MapInPandasExec(func, output, child) — 3-arg constructor
 *   - No MapInBatchExec trait (introduced in 3.3)
 *   - No PythonMapInArrowExec (introduced in 3.3)
 *   - No PythonSQLMetrics (introduced in 3.4)
 *
 * Uses the DataFlintArrowWindowPythonExec_4_1 delegation pattern:
 * creates the original MapInPandasExec at runtime via reflection, delegates
 * execution to it, and wraps the result RDD with a duration metric.
 * This avoids copying any Spark execution logic.
 */
package org.apache.spark.sql.execution.python

import org.apache.spark.dataflint.{DataFlintRDDUtils, MetricsUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

/**
 * DataFlint instrumented MapInPandasExec for Spark 3.0.x / 3.1.x / 3.2.x.
 * Replaces the original MapInPandasExec in the physical plan.
 */
class DataFlintMapInPandasExec_3_0 private (
    val func: Expression,
    override val output: Seq[Attribute],
    val child: SparkPlan)
  extends UnaryExecNode with Logging {

  override def nodeName: String = "DataFlintMapInPandas"

  logInfo("DataFlint MapInPandas (Spark 3.0) is connected")

  override def producedAttributes: AttributeSet = AttributeSet(output)
  override def outputPartitioning: Partitioning = child.outputPartitioning

  // Cannot use SQLMetrics.createTimingMetric() — it gained a default parameter in 3.5
  // which generates a $default$3() call that doesn't exist in 3.0–3.4 at runtime.
  override lazy val metrics: Map[String, SQLMetric] = Map(
    MetricsUtils.getTimingMetric("duration")(sparkContext)
  )

  override protected def doExecute(): RDD[InternalRow] = {
    logInfo("DataFlint MapInPandas (Spark 3.0) doExecute is running")

    val innerRDD: RDD[InternalRow] = try {
      val companionClass = Class.forName(
        "org.apache.spark.sql.execution.python.MapInPandasExec$")
      val companion = companionClass.getField("MODULE$").get(null)
      val applyMethod = companion.getClass.getMethods
        .find(m => m.getName == "apply" && m.getParameterCount == 3)
        .getOrElse(throw new RuntimeException(
          "MapInPandasExec$.apply(3) not found — Spark 3.0.x required"))
      val innerExec = applyMethod.invoke(companion, func, output, child).asInstanceOf[SparkPlan]
      innerExec.execute()
    } catch {
      case e: Exception =>
        logWarning(s"DataFlint: failed to create MapInPandasExec via reflection: ${e.getMessage}")
        throw e
    }

    DataFlintRDDUtils.withDurationMetric(innerRDD, longMetric("duration"))
  }

  override def canEqual(that: Any): Boolean = that.isInstanceOf[DataFlintMapInPandasExec_3_0]
  override def productArity: Int = 3
  override def productElement(n: Int): Any = n match {
    case 0 => func
    case 1 => output
    case 2 => child
    case _ => throw new IndexOutOfBoundsException(s"$n")
  }

  override protected def withNewChildInternal(newChild: SparkPlan): DataFlintMapInPandasExec_3_0 =
    new DataFlintMapInPandasExec_3_0(func, output, newChild)
}

object DataFlintMapInPandasExec_3_0 {
  def apply(
      func: Expression,
      output: Seq[Attribute],
      child: SparkPlan): DataFlintMapInPandasExec_3_0 =
    new DataFlintMapInPandasExec_3_0(func, output, child)
}