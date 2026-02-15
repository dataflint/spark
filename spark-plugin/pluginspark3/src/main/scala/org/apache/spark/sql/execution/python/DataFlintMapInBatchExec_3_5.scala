/*
 * DataFlint instrumented MapInBatchExec for Spark 3.5.x
 *
 * Spark 3.5 characteristics:
 *   - Major refactor: MapInBatchEvaluatorFactory introduced (SPARK-44361)
 *   - PartitionEvaluator API support
 *   - Barrier mode support (SPARK-42896) — isBarrier field on MapInPandasExec
 *   - JobArtifactSet for Spark Connect artifact management
 *   - arrowUseLargeVarTypes config (SPARK-39979)
 *   - ArrowPythonRunner.getPythonRunnerConfMap replaces ArrowUtils.getPythonRunnerConfMap (SPARK-44532)
 *   - ArrowPythonRunner constructor: 9-arg (adds largeVarTypes, pythonMetrics, jobArtifactUUID)
 *   - MapInPandasExec fields: (func, output, child, isBarrier)
 */
package org.apache.spark.sql.execution.python

import java.util.concurrent.TimeUnit.NANOSECONDS

import org.apache.spark.JobArtifactSet
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{PythonUDF, _}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

/**
 * DataFlint instrumented version of MapInBatchExec for Spark 3.5.x.
 * Adds a duration metric alongside PythonSQLMetrics.
 *
 * Uses MapInBatchEvaluatorFactory directly (introduced in 3.5).
 */
trait DataFlintMapInBatchExec_3_5 extends SparkPlan with PythonSQLMetrics with Logging {
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
    logInfo("DataFlint MapInBatchExec (Spark 3.5) doExecute is running")

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

/**
 * DataFlint instrumented MapInPandasExec for Spark 3.5.x.
 * Replaces the original MapInPandasExec in the physical plan.
 */
case class DataFlintMapInPandasExec_3_5(
    func: Expression,
    output: Seq[Attribute],
    child: SparkPlan,
    override val isBarrier: Boolean)
  extends DataFlintMapInBatchExec_3_5 with Logging {

  override def nodeName: String = "DataFlintMapInPandas"

  logInfo("DataFlint MapInPandas (Spark 3.5) is connected")

  override protected val pythonEvalType: Int = PythonEvalType.SQL_MAP_PANDAS_ITER_UDF

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan =
    copy(child = newChildren.head)
}

/**
 * DataFlint instrumented PythonMapInArrowExec for Spark 3.5.x.
 * Replaces the original PythonMapInArrowExec in the physical plan.
 */
case class DataFlintPythonMapInArrowExec_3_5(
    func: Expression,
    output: Seq[Attribute],
    child: SparkPlan,
    override val isBarrier: Boolean)
  extends DataFlintMapInBatchExec_3_5 with Logging {

  override def nodeName: String = "DataFlintMapInArrow"

  logInfo("DataFlint MapInArrow (Spark 3.5) is connected")

  override protected val pythonEvalType: Int = PythonEvalType.SQL_MAP_ARROW_ITER_UDF

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan =
    copy(child = newChildren.head)
}
