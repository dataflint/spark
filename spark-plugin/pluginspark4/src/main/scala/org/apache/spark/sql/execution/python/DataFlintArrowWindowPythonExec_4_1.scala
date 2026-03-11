/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * DataFlint instrumented version for Python UDF window functions on Spark 4.1.x.
 *
 * In Spark 4.1, WindowInPandasExec was replaced by ArrowWindowPythonExec.
 * Since pluginspark4 compiles against Spark 4.0.1, we cannot extend ArrowWindowPythonExec
 * directly. Instead, we use reflection to create an ArrowWindowPythonExec at runtime
 * and delegate execution to it, wrapping the result with a duration metric.
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

class DataFlintArrowWindowPythonExec_4_1 private (
    val windowExpression: Seq[NamedExpression],
    val partitionSpec: Seq[Expression],
    val orderSpec: Seq[SortOrder],
    val child: SparkPlan)
  extends UnaryExecNode with PythonSQLMetrics with Logging {

  override def nodeName: String = "DataFlintArrowWindowPython"

  override def canEqual(that: Any): Boolean = that.isInstanceOf[DataFlintArrowWindowPythonExec_4_1]
  override def productArity: Int = 4
  override def productElement(n: Int): Any = n match {
    case 0 => windowExpression
    case 1 => partitionSpec
    case 2 => orderSpec
    case 3 => child
    case _ => throw new IndexOutOfBoundsException(s"$n")
  }

  override def output: Seq[Attribute] = child.output ++ windowExpression.map(_.toAttribute)

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override lazy val metrics: Map[String, SQLMetric] = pythonMetrics ++ Map(
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"),
    "duration" -> SQLMetrics.createTimingMetric(sparkContext, "duration")
  )

  override protected def doExecute(): RDD[InternalRow] = {
    val durationMetric = longMetric("duration")

    // Create ArrowWindowPythonExec at runtime via reflection (Spark 4.1-only class).
    // We use the companion object's apply method which deduces evalType from window expressions.
    val innerRDD: RDD[InternalRow] = try {
      val companionClass = Class.forName(
        "org.apache.spark.sql.execution.python.ArrowWindowPythonExec$")
      val companion = companionClass.getField("MODULE$").get(null)
      val applyMethod = companion.getClass.getMethods
        .find(m => m.getName == "apply" && m.getParameterCount == 4)
        .getOrElse(throw new RuntimeException(
          "ArrowWindowPythonExec$.apply(4) not found — Spark 4.1.x required"))
      val innerExec = applyMethod.invoke(companion,
        windowExpression, partitionSpec, orderSpec, child).asInstanceOf[SparkPlan]
      innerExec.execute()
    } catch {
      case e: Exception =>
        logWarning(s"DataFlint: failed to create ArrowWindowPythonExec via reflection: ${e.getMessage}")
        throw e
    }

    DataFlintRDDUtils.withDurationMetric(innerRDD, durationMetric)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): DataFlintArrowWindowPythonExec_4_1 =
    new DataFlintArrowWindowPythonExec_4_1(windowExpression, partitionSpec, orderSpec, newChild)
}

object DataFlintArrowWindowPythonExec_4_1 {
  def apply(
      windowExpression: Seq[NamedExpression],
      partitionSpec: Seq[Expression],
      orderSpec: Seq[SortOrder],
      child: SparkPlan): DataFlintArrowWindowPythonExec_4_1 =
    new DataFlintArrowWindowPythonExec_4_1(windowExpression, partitionSpec, orderSpec, child)
}