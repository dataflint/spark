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

import org.apache.spark.JobArtifactSet
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{PythonUDF, _}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

import java.util.concurrent.TimeUnit.NANOSECONDS

/**
 * DataFlint instrumented version of MapInPandasExec for Spark 3.x.
 *
 * A relation produced by applying a function that takes an iterator of pandas DataFrames
 * and outputs an iterator of pandas DataFrames. Adds a duration metric.
 */
case class DataFlintMapInPandasExec(
    func: Expression,
    output: Seq[Attribute],
    child: SparkPlan,
    override val isBarrier: Boolean)
  extends DataFlintMapInBatchExec with Logging {

  logInfo("DataFlint MapInPandas is connected")

  override protected val pythonEvalType: Int = PythonEvalType.SQL_MAP_PANDAS_ITER_UDF

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan =
    copy(child = newChildren.head)
}

/**
 * DataFlint instrumented version of MapInBatchExec trait for Spark 3.x.
 *
 * Adds a duration metric to measure the time spent in Python UDF execution.
 */
trait DataFlintMapInBatchExec extends SparkPlan with PythonSQLMetrics with Logging {
  protected val func: Expression
  protected val pythonEvalType: Int
  protected val isBarrier: Boolean

  def child: SparkPlan

  override def children: Seq[SparkPlan] = Seq(child)

  override def producedAttributes: AttributeSet = AttributeSet(output)

  private[this] val jobArtifactUUID = JobArtifactSet.getCurrentJobArtifactState.map(_.uuid)

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override lazy val metrics: Map[String, SQLMetric] = pythonMetrics ++ Map(
    "duration" -> SQLMetrics.createTimingMetric(sparkContext, "duration")
  )

  override protected def doExecute(): RDD[InternalRow] = {
    logInfo("DataFlint MapInBatchExec doExecute is running")

    val pythonRunnerConf = ArrowPythonRunner.getPythonRunnerConfMap(conf)
    val pythonFunction = func.asInstanceOf[PythonUDF].func
    val chainedFunc = Seq(ChainedPythonFunctions(Seq(pythonFunction)))

    val evaluatorFactory = new MapInBatchEvaluatorFactory(
      output,
      chainedFunc,
      child.schema,
      conf.arrowMaxRecordsPerBatch,
      pythonEvalType,
      conf.sessionLocalTimeZone,
      conf.arrowUseLargeVarTypes,
      pythonRunnerConf,
      pythonMetrics,
      jobArtifactUUID)

    val durationMetric = longMetric("duration")

    if (isBarrier) {
      val rddBarrier = child.execute().barrier()
      if (conf.usePartitionEvaluator) {
        rddBarrier.mapPartitionsWithEvaluator(evaluatorFactory)
      } else {
        rddBarrier.mapPartitionsWithIndex { (index, iter) =>
          val startTime = System.nanoTime()
          val result = evaluatorFactory.createEvaluator().eval(index, iter).toList
          durationMetric += NANOSECONDS.toMillis(System.nanoTime() - startTime)
          result.iterator
        }
      }
    } else {
      val inputRdd = child.execute()
      if (conf.usePartitionEvaluator) {
        inputRdd.mapPartitionsWithEvaluator(evaluatorFactory)
      } else {
        inputRdd.mapPartitionsWithIndexInternal { (index, iter) =>
          val startTime = System.nanoTime()
          val result = evaluatorFactory.createEvaluator().eval(index, iter).toList
          durationMetric += NANOSECONDS.toMillis(System.nanoTime() - startTime)
          result.iterator
        }
      }
    }
  }
}

object DataFlintMapInPandasExec {
  def apply(
      func: Expression,
      output: Seq[Attribute],
      child: SparkPlan,
      isBarrier: Boolean): DataFlintMapInPandasExec = {
    new DataFlintMapInPandasExec(func, output, child, isBarrier)
  }
}
