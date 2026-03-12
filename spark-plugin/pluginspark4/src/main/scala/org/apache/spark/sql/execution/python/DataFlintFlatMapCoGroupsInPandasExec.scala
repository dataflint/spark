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

import org.apache.spark.dataflint.DataFlintRDDUtils
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

/**
 * DataFlint instrumented version of FlatMapCoGroupsInPandasExec for Spark 4.x.
 *
 * Instruments PandasCogroupedOps.applyInPandas() / df.cogroup(df2) operations
 * with a duration metric by wrapping the parent's doExecute() RDD.
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

  override lazy val metrics: Map[String, SQLMetric] = pythonMetrics ++ Map(
    "duration" -> SQLMetrics.createTimingMetric(sparkContext, "duration")
  )

  override protected def doExecute(): RDD[InternalRow] =
    DataFlintRDDUtils.withDurationMetric(super.doExecute(), longMetric("duration"))

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan, newRight: SparkPlan): DataFlintFlatMapCoGroupsInPandasExec =
    DataFlintFlatMapCoGroupsInPandasExec(leftGroup, rightGroup, func, output, newLeft, newRight)
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
