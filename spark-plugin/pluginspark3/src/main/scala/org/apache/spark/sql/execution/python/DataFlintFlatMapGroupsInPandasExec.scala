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
 * DataFlint instrumented version of FlatMapGroupsInPandasExec for Spark 3.x.
 *
 * Instruments GroupedData.applyInPandas() / pandas_udf(GROUPED_MAP) operations
 * with a duration metric by wrapping the parent's doExecute() RDD.
 * The constructor (groupingAttributes, func, output, child) is stable across 3.0–3.5.
 */
class DataFlintFlatMapGroupsInPandasExec private (
    groupingAttributes: Seq[Attribute],
    func: Expression,
    output: Seq[Attribute],
    child: SparkPlan)
  extends FlatMapGroupsInPandasExec(groupingAttributes, func, output, child) with Logging {

  override def nodeName: String = "DataFlintFlatMapGroupsInPandas"

  override lazy val metrics: Map[String, SQLMetric] = pythonMetrics ++ Map(
    "duration" -> SQLMetrics.createTimingMetric(sparkContext, "duration")
  )

  override protected def doExecute(): RDD[InternalRow] =
    DataFlintRDDUtils.withDurationMetric(super.doExecute(), longMetric("duration"))

  override protected def withNewChildInternal(newChild: SparkPlan): DataFlintFlatMapGroupsInPandasExec =
    DataFlintFlatMapGroupsInPandasExec(groupingAttributes, func, output, newChild)
}

object DataFlintFlatMapGroupsInPandasExec {
  def apply(
      groupingAttributes: Seq[Attribute],
      func: Expression,
      output: Seq[Attribute],
      child: SparkPlan): DataFlintFlatMapGroupsInPandasExec =
    new DataFlintFlatMapGroupsInPandasExec(groupingAttributes, func, output, child)
}
