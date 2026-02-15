/*
 * DataFlint instrumented MapInBatchExec for Spark 4.0.x
 *
 * Spark 4.0 characteristics:
 *   - ResourceProfile support (SPARK-46812) — profile field on MapInPandasExec
 *   - resultId tracking: chainedFunc becomes Seq[(ChainedPythonFunctions, Long)]
 *   - ArrowPythonRunner now 10-arg constructor (adds profiler: Option[String])
 *   - MapInBatchEvaluatorFactory chainedFunc type changed to Seq[(ChainedPythonFunctions, Long)]
 *   - MapInPandasExec fields: (func, output, child, isBarrier, profile)
 */
package org.apache.spark.sql.execution.python

import java.util.concurrent.TimeUnit.NANOSECONDS

import org.apache.spark.JobArtifactSet
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{PythonUDF, _}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

/**
 * DataFlint instrumented version of MapInBatchExec for Spark 4.0.x.
 * Adds a duration metric alongside PythonSQLMetrics.
 *
 * Key 4.0 change: chainedFunc includes resultId for metrics/profiling correlation,
 * and ResourceProfile support allows custom resource allocation for Python UDFs.
 */
trait DataFlintMapInBatchExec_4_0 extends SparkPlan with PythonSQLMetrics with Logging {
  protected val func: Expression
  protected val pythonEvalType: Int
  protected val isBarrier: Boolean
  protected val profile: Option[ResourceProfile]

  def child: SparkPlan

  override def children: Seq[SparkPlan] = Seq(child)

  override def producedAttributes: AttributeSet = AttributeSet(output)

  private[this] val jobArtifactUUID = JobArtifactSet.getCurrentJobArtifactState.map(_.uuid)

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override lazy val metrics: Map[String, SQLMetric] = pythonMetrics ++ Map(
    "duration" -> SQLMetrics.createTimingMetric(sparkContext, "duration")
  )

  override protected def doExecute(): RDD[InternalRow] = {
    logInfo("DataFlint MapInBatchExec (Spark 4.0) doExecute is running")

    val pythonRunnerConf = ArrowPythonRunner.getPythonRunnerConfMap(conf)
    val pythonUDF = func.asInstanceOf[PythonUDF]
    val pythonFunction = pythonUDF.func
    // 4.0 change: chainedFunc includes resultId for profiling correlation
    val chainedFunc = Seq((ChainedPythonFunctions(Seq(pythonFunction)), pythonUDF.resultId.id))

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

    val rdd = if (isBarrier) {
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
    // 4.0 change: apply ResourceProfile if specified
    profile.map(rp => rdd.withResources(rp)).getOrElse(rdd)
  }
}

/**
 * DataFlint instrumented MapInPandasExec for Spark 4.0.x.
 * Replaces the original MapInPandasExec in the physical plan.
 */
case class DataFlintMapInPandasExec_4_0(
    func: Expression,
    output: Seq[Attribute],
    child: SparkPlan,
    override val isBarrier: Boolean,
    override val profile: Option[ResourceProfile])
  extends DataFlintMapInBatchExec_4_0 with Logging {

  override def nodeName: String = "DataFlintMapInPandas"

  logInfo("DataFlint MapInPandas (Spark 4.0) is connected")

  override protected val pythonEvalType: Int = PythonEvalType.SQL_MAP_PANDAS_ITER_UDF

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan =
    copy(child = newChildren.head)
}

/**
 * DataFlint instrumented PythonMapInArrowExec for Spark 4.0.x.
 * Replaces the original PythonMapInArrowExec in the physical plan.
 */
case class DataFlintPythonMapInArrowExec_4_0(
    func: Expression,
    output: Seq[Attribute],
    child: SparkPlan,
    override val isBarrier: Boolean,
    override val profile: Option[ResourceProfile])
  extends DataFlintMapInBatchExec_4_0 with Logging {

  override def nodeName: String = "DataFlintMapInArrow"

  logInfo("DataFlint MapInArrow (Spark 4.0) is connected")

  override protected val pythonEvalType: Int = PythonEvalType.SQL_MAP_ARROW_ITER_UDF

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan =
    copy(child = newChildren.head)
}
