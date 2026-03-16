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
package org.apache.spark.sql.execution.python

import org.apache.spark.TaskContext
import org.apache.spark.api.python.ChainedPythonFunctions
import org.apache.spark.dataflint.{DataFlintRDDUtils, MetricsUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.StructType

import java.util.concurrent.TimeUnit.NANOSECONDS

/**
 * DataFlint instrumented version of FlatMapCoGroupsInPandasExec for Spark 3.x.
 *
 * Instruments PandasCogroupedOps.applyInPandas() / df.cogroup(df2) operations
 * with a duration metric by wrapping the parent's doExecute() RDD.
 * The constructor (leftGroup, rightGroup, func, output, left, right) is stable across 3.0–3.5.
 */
class DataFlintFlatMapCoGroupsInPandasExec private (
    leftGroup: Seq[Attribute],
    rightGroup: Seq[Attribute],
    func: Expression,
    output: Seq[Attribute],
    left: SparkPlan,
    right: SparkPlan)
  extends FlatMapCoGroupsInPandasExec(leftGroup, rightGroup, func, output, left, right) with Logging {

  override def nodeName: String = "DataFlintFlatMapCoGroupsInPandas"

  // Cannot use super.metrics in a lazy val override — Scala 2 does not generate super
  // accessors for trait lazy vals (PythonSQLMetrics). Use a sibling instance instead.
  // Cannot use SQLMetrics.createTimingMetric() — it gained a default parameter in 3.5
  // which generates a $default$3() call that doesn't exist in 3.0–3.4 at runtime.
  private val internal = FlatMapCoGroupsInPandasExec(leftGroup, rightGroup, func, output, left, right)

  override lazy val metrics: Map[String, SQLMetric] = internal.metrics ++ Map(
    MetricsUtils.getTimingMetric("duration")(sparkContext)
  )

  override protected def doExecute(): RDD[InternalRow] =
    DataFlintRDDUtils.withDurationMetric(super.doExecute(), longMetric("duration"))

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan, newRight: SparkPlan): DataFlintFlatMapCoGroupsInPandasExec =
    DataFlintFlatMapCoGroupsInPandasExec(leftGroup, rightGroup, func, output, newLeft, newRight)

  override def canEqual(other: Any): Boolean = other.isInstanceOf[DataFlintFlatMapCoGroupsInPandasExec]

  override def equals(other: Any): Boolean =
    other.isInstanceOf[DataFlintFlatMapCoGroupsInPandasExec] && super.equals(other)

  override def hashCode: Int = super.hashCode
}

object DataFlintFlatMapCoGroupsInPandasExec {
  def apply(
      leftGroup: Seq[Attribute],
      rightGroup: Seq[Attribute],
      func: Expression,
      output: Seq[Attribute],
      left: SparkPlan,
      right: SparkPlan): DataFlintFlatMapCoGroupsInPandasExec =
    new DataFlintFlatMapCoGroupsInPandasExec(leftGroup, rightGroup, func, output, left, right)
}
