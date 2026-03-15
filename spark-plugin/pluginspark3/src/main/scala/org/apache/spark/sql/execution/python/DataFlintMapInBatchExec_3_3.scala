/*
 * DataFlint instrumented PythonMapInArrowExec for Spark 3.3.x – 3.4.x
 *
 * PythonMapInArrowExec was introduced in Spark 3.3 (SPARK-37227).
 * Both 3.3 and 3.4 share the same constructor: PythonMapInArrowExec(func, output, child) — 3 args.
 * (isBarrier was added in 3.5)
 *
 * Uses the delegation-via-reflection pattern — see DataFlintMapInBatchExec_3_0.scala.
 * This single class handles 3.3 and 3.4 without version-specific subclasses.
 *
 * For MapInPandasExec (all 3.0–3.4), see DataFlintMapInBatchExec_3_0.scala.
 */
package org.apache.spark.sql.execution.python

import org.apache.spark.dataflint.DataFlintRDDUtils
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

/**
 * DataFlint instrumented PythonMapInArrowExec for Spark 3.3.x – 3.4.x.
 * Replaces the original PythonMapInArrowExec in the physical plan.
 */
class DataFlintPythonMapInArrowExec_3_3 private (
    val func: Expression,
    override val output: Seq[Attribute],
    val child: SparkPlan)
  extends UnaryExecNode with Logging {

  override def nodeName: String = "DataFlintMapInArrow"

  logInfo("DataFlint MapInArrow (Spark 3.3–3.4) is connected")

  override def producedAttributes: AttributeSet = AttributeSet(output)
  override def outputPartitioning: Partitioning = child.outputPartitioning

  // Cannot use SQLMetrics.createTimingMetric() — it gained a default parameter in 3.5
  // which generates a $default$3() call that doesn't exist in 3.3–3.4 at runtime.
  override lazy val metrics: Map[String, SQLMetric] = Map(
    "duration" -> {
      val metric = new SQLMetric("timing", -1L)
      metric.register(sparkContext, Some("duration"), false)
      metric
    }
  )

  override protected def doExecute(): RDD[InternalRow] = {
    logInfo("DataFlint MapInArrow (Spark 3.3–3.4) doExecute is running")

    val innerRDD: RDD[InternalRow] = try {
      val companionClass = Class.forName(
        "org.apache.spark.sql.execution.python.PythonMapInArrowExec$")
      val companion = companionClass.getField("MODULE$").get(null)
      val applyMethod = companion.getClass.getMethods
        .find(m => m.getName == "apply" && m.getParameterCount == 3)
        .getOrElse(throw new RuntimeException(
          "PythonMapInArrowExec$.apply(3) not found — Spark 3.3–3.4 required"))
      val innerExec = applyMethod.invoke(companion, func, output, child).asInstanceOf[SparkPlan]
      innerExec.execute()
    } catch {
      case e: Exception =>
        logWarning(s"DataFlint: failed to create PythonMapInArrowExec via reflection: ${e.getMessage}")
        throw e
    }

    DataFlintRDDUtils.withDurationMetric(innerRDD, longMetric("duration"))
  }

  override def canEqual(that: Any): Boolean = that.isInstanceOf[DataFlintPythonMapInArrowExec_3_3]
  override def productArity: Int = 3
  override def productElement(n: Int): Any = n match {
    case 0 => func
    case 1 => output
    case 2 => child
    case _ => throw new IndexOutOfBoundsException(s"$n")
  }

  override protected def withNewChildInternal(newChild: SparkPlan): DataFlintPythonMapInArrowExec_3_3 =
    new DataFlintPythonMapInArrowExec_3_3(func, output, newChild)
}

object DataFlintPythonMapInArrowExec_3_3 {
  def apply(
      func: Expression,
      output: Seq[Attribute],
      child: SparkPlan): DataFlintPythonMapInArrowExec_3_3 =
    new DataFlintPythonMapInArrowExec_3_3(func, output, child)
}