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
 *
 *  What's currently included in duration

  The timer wraps iter from super.doExecute(). When you call iter.hasNext, the parent WindowExec iterator internally:
  1. Pulls rows from its child (e.g., a SortExec)
  2. Accumulates a full partition group
  3. Computes window function values
  4. Emits output rows

  Steps 1–3 all happen during the first iter.hasNext call. So the timer captures child-fetch time + window computation time combined.

  How to isolate just the window computation

  You can't easily do this with the current wrapping approach because WindowExec uses an internal buffer pattern — it reads all input for a partition group eagerly on the first hasNext. To truly isolate window computation you'd
   need to:

  1. Override doExecute more deeply — instrument inside WindowFunctionFrame.prepare() and write() calls, which are the actual window computation steps. This requires overriding private Spark internals.
  2. Subtract child time — measure the child RDD's execution time separately and subtract it. Fragile and inaccurate.
  3. Accept the current scope as "window stage time" — which is the pragmatic choice. The metric measures the time from when the partition iterator is first pulled until it's exhausted. This is a reasonable proxy for window
  operator cost in a query plan.

 */
package org.apache.spark.sql.execution.window

import org.apache.spark.dataflint.DataFlintRDDUtils
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

class DataFlintWindowExec private (
    windowExpression: Seq[NamedExpression],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    child: SparkPlan)
  extends WindowExec(windowExpression, partitionSpec, orderSpec, child) with Logging {

  override def nodeName: String = "DataFlintWindow"

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"),
    "duration" -> SQLMetrics.createTimingMetric(sparkContext, "duration")
  )

  override protected def doExecute(): RDD[InternalRow] =
    DataFlintRDDUtils.withDurationMetric(super.doExecute(), longMetric("duration"))

  override protected def withNewChildInternal(newChild: SparkPlan): DataFlintWindowExec =
    DataFlintWindowExec(windowExpression, partitionSpec, orderSpec, newChild)
}

object DataFlintWindowExec {
  def apply(
      windowExpression: Seq[NamedExpression],
      partitionSpec: Seq[Expression],
      orderSpec: Seq[SortOrder],
      child: SparkPlan): DataFlintWindowExec =
    new DataFlintWindowExec(windowExpression, partitionSpec, orderSpec, child)
}