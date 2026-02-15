/*
 * DataFlint instrumented MapInBatchExec for Spark 3.3.x
 *
 * Spark 3.3 characteristics:
 *   - MapInBatchExec trait was introduced (extracted from MapInPandasExec)
 *   - No PythonSQLMetrics (added in 3.4 via SPARK-34265)
 *   - No barrier mode support (added in 3.5 via SPARK-42896)
 *   - No MapInBatchEvaluatorFactory (added in 3.5 via SPARK-44361)
 *   - Inline execution with ArrowPythonRunner (6-arg constructor)
 *   - ArrowPythonRunner constructor: (funcs, evalType, argOffsets, schema, timeZoneId, confMap)
 *   - MapInPandasExec fields: (func, output, child) — no isBarrier, no profile
 */
package org.apache.spark.sql.execution.python

import scala.collection.JavaConverters._

import java.util.concurrent.TimeUnit.NANOSECONDS

import org.apache.spark.{ContextAwareIterator, TaskContext}
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.api.python.BasePythonRunner
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{PythonUDF, _}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

/**
 * DataFlint instrumented version of MapInBatchExec for Spark 3.3.x.
 * Adds a duration metric to measure time spent in Python UDF execution.
 *
 * Uses reflection to construct ArrowPythonRunner since the 3.3 constructor
 * (6-arg) differs from the 3.5 constructor (9-arg) that this code compiles against.
 */
trait DataFlintMapInBatchExec_3_3 extends SparkPlan with Logging {
  protected val func: Expression
  protected val pythonEvalType: Int

  def child: SparkPlan

  override def children: Seq[SparkPlan] = Seq(child)

  override def producedAttributes: AttributeSet = AttributeSet(output)

  override def outputPartitioning: Partitioning = child.outputPartitioning

  // No PythonSQLMetrics in 3.3 — only our custom duration metric
  // Note: Cannot use SQLMetrics.createTimingMetric() because it gained a default parameter
  // in 3.5 (size: Long = -1L), and the Scala compiler generates a call to the synthetic
  // $default$3() method which doesn't exist in 3.3/3.4 at runtime.
  override lazy val metrics: Map[String, SQLMetric] = Map(
    "duration" -> {
      val metric = new SQLMetric("timing", -1L)
      metric.register(sparkContext, Some("duration"), false)
      metric
    }
  )

  override protected def doExecute(): RDD[InternalRow] = {
    logInfo("DataFlint MapInBatchExec (Spark 3.3) doExecute is running")

    val durationMetric = longMetric("duration")

    child.execute().mapPartitionsInternal { inputIter =>
      val startTime = System.nanoTime()

      val pythonFunction = func.asInstanceOf[PythonUDF].func
      val argOffsets = Array(Array(0))
      val chainedFunc = Seq(ChainedPythonFunctions(Seq(pythonFunction)))
      val sessionLocalTimeZone = conf.sessionLocalTimeZone
      val outputTypes = child.schema
      val batchSize = conf.arrowMaxRecordsPerBatch

      // Construct confMap manually (replicates ArrowUtils.getPythonRunnerConfMap for 3.3)
      val pythonRunnerConf = Map(
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> sessionLocalTimeZone,
        SQLConf.PANDAS_GROUPED_MAP_ASSIGN_COLUMNS_BY_NAME.key ->
          conf.pandasGroupedMapAssignColumnsByName.toString
      )

      val context = TaskContext.get()
      val contextAwareIterator = new ContextAwareIterator(context, inputIter)
      val wrappedIter = contextAwareIterator.map(InternalRow(_))
      val batchIter =
        if (batchSize > 0) new BatchIterator(wrappedIter, batchSize) else Iterator(wrappedIter)

      // Use reflection: Spark 3.3 ArrowPythonRunner has a 6-arg constructor
      val runnerClass = classOf[ArrowPythonRunner]
      val ctor = runnerClass.getConstructors
        .find(_.getParameterCount == 6)
        .getOrElse(throw new RuntimeException(
          "Cannot find ArrowPythonRunner 6-arg constructor. Expected Spark 3.3.x runtime."))
      val runner = ctor.newInstance(
        chainedFunc.asInstanceOf[AnyRef],
        Int.box(pythonEvalType),
        argOffsets.asInstanceOf[AnyRef],
        StructType(StructField("struct", outputTypes) :: Nil).asInstanceOf[AnyRef],
        sessionLocalTimeZone.asInstanceOf[AnyRef],
        pythonRunnerConf.asInstanceOf[AnyRef]
      ).asInstanceOf[BasePythonRunner[Iterator[InternalRow], ColumnarBatch]]

      val columnarBatchIter = runner.compute(batchIter, context.partitionId(), context)

      val unsafeProj = UnsafeProjection.create(output, output)

      val result = columnarBatchIter.flatMap { batch =>
        val structVector = batch.column(0).asInstanceOf[ArrowColumnVector]
        val outputVectors = output.indices.map(structVector.getChild)
        val flattenedBatch = new ColumnarBatch(outputVectors.toArray)
        flattenedBatch.setNumRows(batch.numRows())
        flattenedBatch.rowIterator.asScala
      }.map(unsafeProj).toList

      durationMetric += NANOSECONDS.toMillis(System.nanoTime() - startTime)
      result.iterator
    }
  }
}

/**
 * DataFlint instrumented MapInPandasExec for Spark 3.3.x.
 * Replaces the original MapInPandasExec in the physical plan.
 */
case class DataFlintMapInPandasExec_3_3(
    func: Expression,
    output: Seq[Attribute],
    child: SparkPlan)
  extends DataFlintMapInBatchExec_3_3 with Logging {

  override def nodeName: String = "DataFlintMapInPandas"

  logInfo("DataFlint MapInPandas (Spark 3.3) is connected")

  override protected val pythonEvalType: Int = PythonEvalType.SQL_MAP_PANDAS_ITER_UDF

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan =
    copy(child = newChildren.head)
}

/**
 * DataFlint instrumented PythonMapInArrowExec for Spark 3.3.x.
 * Replaces the original PythonMapInArrowExec in the physical plan.
 */
case class DataFlintPythonMapInArrowExec_3_3(
    func: Expression,
    output: Seq[Attribute],
    child: SparkPlan)
  extends DataFlintMapInBatchExec_3_3 with Logging {

  override def nodeName: String = "DataFlintMapInArrow"

  logInfo("DataFlint MapInArrow (Spark 3.3) is connected")

  override protected val pythonEvalType: Int = PythonEvalType.SQL_MAP_ARROW_ITER_UDF

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan =
    copy(child = newChildren.head)
}
