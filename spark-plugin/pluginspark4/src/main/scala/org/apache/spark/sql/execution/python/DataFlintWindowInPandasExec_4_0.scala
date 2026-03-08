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

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

import java.util.concurrent.TimeUnit.NANOSECONDS

class DataFlintWindowInPandasExec_4_0 private (
    windowExpression: Seq[NamedExpression],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    child: SparkPlan)
  extends WindowInPandasExec(windowExpression, partitionSpec, orderSpec, child) with Logging {

  override def nodeName: String = "DataFlintWindowInPandas"

  override lazy val metrics: Map[String, SQLMetric] = pythonMetrics ++ Map(
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"),
    "duration" -> SQLMetrics.createTimingMetric(sparkContext, "duration")
  )

  override protected def doExecute(): RDD[InternalRow] = {
    val durationMetric = longMetric("duration")
    val innerRDD = super.doExecute()
    innerRDD.mapPartitions { iter =>
      val startTime = System.nanoTime()
      var done = false
      new Iterator[InternalRow] {
        override def hasNext: Boolean = {
          val r = iter.hasNext
          if (!r && !done) {
            durationMetric += NANOSECONDS.toMillis(System.nanoTime() - startTime)
            done = true
          }
          r
        }
        override def next(): InternalRow = iter.next()
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): DataFlintWindowInPandasExec_4_0 =
    DataFlintWindowInPandasExec_4_0(windowExpression, partitionSpec, orderSpec, newChild)
}

object DataFlintWindowInPandasExec_4_0 {
  def apply(
      windowExpression: Seq[NamedExpression],
      partitionSpec: Seq[Expression],
      orderSpec: Seq[SortOrder],
      child: SparkPlan): DataFlintWindowInPandasExec_4_0 =
    new DataFlintWindowInPandasExec_4_0(windowExpression, partitionSpec, orderSpec, child)
}