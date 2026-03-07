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
package org.apache.spark.sql.execution.window

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

import java.util.concurrent.TimeUnit.NANOSECONDS

/**
 * DataFlint instrumented version of WindowExec for Spark 4.x.
 *
 * Spark 4.0 uses WindowEvaluatorFactory whose eval() returns a lazy iterator,
 * so wrapping super.doExecute() is correct: startTime is set before any rows
 * are consumed and the duration covers the full window computation.
 */
class DataFlintWindowExec private (
    windowExpression: Seq[NamedExpression],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    child: SparkPlan)
  extends WindowExec(windowExpression, partitionSpec, orderSpec, child) with Logging {

  logInfo("DataFlint WindowExec is connected")

  override def nodeName: String = "DataFlintWindow"

  override lazy val metrics: Map[String, SQLMetric] = Map(
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
