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
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

/**
 * DataFlint instrumented version of MapInArrowExec for Spark 4.x.
 *
 * Instruments map_in_arrow UDF operations with a duration metric by wrapping the
 * parent's doExecute() RDD. Works for both Spark 4.0 and 4.1 — super.doExecute()
 * dispatches to the runtime jar's implementation via JVM invokespecial semantics,
 * so no version-specific subclasses are needed.
 */
class DataFlintMapInArrowExec private (
    func: Expression,
    output: Seq[Attribute],
    child: SparkPlan,
    isBarrier: Boolean,
    profile: Option[ResourceProfile])
  extends MapInArrowExec(func, output, child, isBarrier, profile) with Logging {

  override def nodeName: String = "DataFlintMapInArrow"

  logInfo("DataFlint MapInArrow is connected")

  override lazy val metrics: Map[String, SQLMetric] = pythonMetrics ++ Map(
    "duration" -> SQLMetrics.createTimingMetric(sparkContext, "duration")
  )

  override protected def doExecute(): RDD[InternalRow] =
    DataFlintRDDUtils.withDurationMetric(super.doExecute(), longMetric("duration"))

  override protected def withNewChildInternal(newChild: SparkPlan): DataFlintMapInArrowExec =
    DataFlintMapInArrowExec(func, output, newChild, isBarrier, profile)

  override def canEqual(other: Any): Boolean = other.isInstanceOf[DataFlintMapInArrowExec]
  override def equals(other: Any): Boolean = other.isInstanceOf[DataFlintMapInArrowExec] && super.equals(other)
  override def hashCode: Int = super.hashCode
}

object DataFlintMapInArrowExec {
  def apply(
      func: Expression,
      output: Seq[Attribute],
      child: SparkPlan,
      isBarrier: Boolean,
      profile: Option[ResourceProfile]): DataFlintMapInArrowExec =
    new DataFlintMapInArrowExec(func, output, child, isBarrier, profile)
}
