/*
 * DataFlint instrumented MapInBatchExec for Spark 4.1.x
 *
 * Spark 4.1 characteristics:
 *   - Python worker logging support (SPARK-53976) — sessionUUID for log correlation
 *   - Python UDF profiler support (SPARK-54153, SPARK-54559) — conf.pythonUDFProfiler
 *   - Arrow schema validation (SPARK-51739) — pythonUDF.dataType passed as outputSchema
 *   - MapInBatchEvaluatorFactory now 13-arg constructor (adds outputSchema, sessionUUID, profiler)
 *   - ArrowPythonRunner now 11-arg constructor (adds sessionUUID)
 *   - MapInPandasExec fields: (func, output, child, isBarrier, profile) — same as 4.0
 *
 * Since this code compiles against Spark 4.0.1, reflection is used for the
 * MapInBatchEvaluatorFactory constructor which changed between 4.0 and 4.1.
 */
package org.apache.spark.sql.execution.python

import java.util.concurrent.TimeUnit.NANOSECONDS

import org.apache.spark.{JobArtifactSet, PartitionEvaluatorFactory}
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
 * DataFlint instrumented version of MapInBatchExec for Spark 4.1.x.
 * Adds a duration metric alongside PythonSQLMetrics.
 *
 * Key 4.1 changes over 4.0:
 *   - sessionUUID passed for Python worker log correlation
 *   - pythonUDFProfiler config for UDF profiling
 *   - outputSchema (pythonUDF.dataType) for Arrow schema validation
 *
 * Uses reflection for MapInBatchEvaluatorFactory since the 4.1 constructor (13-arg)
 * differs from the 4.0 constructor (10-arg) that this code compiles against.
 */
trait DataFlintMapInBatchExec_4_1 extends SparkPlan with PythonSQLMetrics with Logging {
  protected val func: Expression
  protected val pythonEvalType: Int
  protected val isBarrier: Boolean
  protected val profile: Option[ResourceProfile]

  def child: SparkPlan

  override def children: Seq[SparkPlan] = Seq(child)

  override def producedAttributes: AttributeSet = AttributeSet(output)

  private[this] val jobArtifactUUID = JobArtifactSet.getCurrentJobArtifactState.map(_.uuid)

  // Get sessionUUID via reflection (4.1-specific API: session.sessionUUID)
  private[this] val sessionUUID: Option[String] = {
    try {
      val sess = session
      if (sess != null) {
        val sessionConf = sess.sessionState.conf
        val loggingEnabled = sessionConf.getClass
          .getMethod("pythonWorkerLoggingEnabled")
          .invoke(sessionConf).asInstanceOf[Boolean]
        if (loggingEnabled) {
          Some(sess.getClass.getMethod("sessionUUID").invoke(sess).asInstanceOf[String])
        } else None
      } else None
    } catch {
      case _: Throwable => None
    }
  }

  // Get pythonUDFProfiler via reflection (4.1-specific config)
  private[this] val pythonUDFProfiler: Option[String] = {
    try {
      conf.getClass.getMethod("pythonUDFProfiler").invoke(conf) match {
        case s: Option[_] => s.map(_.toString)
        case _ => None
      }
    } catch {
      case _: Throwable => None
    }
  }

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override lazy val metrics: Map[String, SQLMetric] = pythonMetrics ++ Map(
    "duration" -> SQLMetrics.createTimingMetric(sparkContext, "duration")
  )

  override protected def doExecute(): RDD[InternalRow] = {
    logInfo("DataFlint MapInBatchExec (Spark 4.1) doExecute is running")

    val pythonRunnerConf = ArrowPythonRunner.getPythonRunnerConfMap(conf)
    val pythonUDF = func.asInstanceOf[PythonUDF]
    val pythonFunction = pythonUDF.func
    val chainedFunc = Seq((ChainedPythonFunctions(Seq(pythonFunction)), pythonUDF.resultId.id))

    // Use reflection for MapInBatchEvaluatorFactory: 4.1 has 13-arg constructor
    // vs 4.0's 10-arg constructor
    val evaluatorFactory = createEvaluatorFactory(
      chainedFunc, pythonUDF, pythonRunnerConf)

    val durationMetric = longMetric("duration")

    val rdd = if (isBarrier) {
      val rddBarrier = child.execute().barrier()
      if (conf.usePartitionEvaluator) {
        rddBarrier.mapPartitionsWithEvaluator(evaluatorFactory)
      } else {
        rddBarrier.mapPartitionsWithIndex { (index, iter) =>
          val startTime = System.nanoTime()
          val result = evaluatorFactory.createEvaluator().eval(index, iter)
          durationMetric += NANOSECONDS.toMillis(System.nanoTime() - startTime)
          result
        }
      }
    } else {
      val inputRdd = child.execute()
      if (conf.usePartitionEvaluator) {
        inputRdd.mapPartitionsWithEvaluator(evaluatorFactory)
      } else {
        inputRdd.mapPartitionsWithIndexInternal { (index, iter) =>
          val startTime = System.nanoTime()
          val result = evaluatorFactory.createEvaluator().eval(index, iter)
          durationMetric += NANOSECONDS.toMillis(System.nanoTime() - startTime)
          result
        }
      }
    }
    profile.map(rp => rdd.withResources(rp)).getOrElse(rdd)
  }

  /**
   * Create MapInBatchEvaluatorFactory via reflection for Spark 4.1's 13-arg constructor.
   *
   * 4.1 constructor: (output, chainedFunc, inputSchema, outputSchema, batchSize,
   *   pythonEvalType, sessionLocalTimeZone, largeVarTypes, pythonRunnerConf,
   *   pythonMetrics, jobArtifactUUID, sessionUUID, profiler)
   */
  private def createEvaluatorFactory(
      chainedFunc: Seq[(ChainedPythonFunctions, Long)],
      pythonUDF: PythonUDF,
      pythonRunnerConf: Map[String, String]
  ): PartitionEvaluatorFactory[InternalRow, InternalRow] = {
    val clazz = classOf[MapInBatchEvaluatorFactory]
    val ctor = clazz.getConstructors
      .find(_.getParameterCount == 13)
      .getOrElse(throw new RuntimeException(
        "Cannot find MapInBatchEvaluatorFactory 13-arg constructor. Expected Spark 4.1.x runtime."))

    ctor.newInstance(
      output.asInstanceOf[AnyRef],
      chainedFunc.asInstanceOf[AnyRef],
      child.schema.asInstanceOf[AnyRef],
      pythonUDF.dataType.asInstanceOf[AnyRef],       // outputSchema (4.1 addition)
      Int.box(conf.arrowMaxRecordsPerBatch),
      Int.box(pythonEvalType),
      conf.sessionLocalTimeZone.asInstanceOf[AnyRef],
      Boolean.box(conf.arrowUseLargeVarTypes),
      pythonRunnerConf.asInstanceOf[AnyRef],
      pythonMetrics.asInstanceOf[AnyRef],
      jobArtifactUUID.asInstanceOf[AnyRef],
      sessionUUID.asInstanceOf[AnyRef],              // sessionUUID (4.1 addition)
      pythonUDFProfiler.asInstanceOf[AnyRef]         // profiler (4.1 addition)
    ).asInstanceOf[PartitionEvaluatorFactory[InternalRow, InternalRow]]
  }
}

/**
 * DataFlint instrumented MapInPandasExec for Spark 4.1.x.
 * Replaces the original MapInPandasExec in the physical plan.
 */
case class DataFlintMapInPandasExec_4_1(
    func: Expression,
    output: Seq[Attribute],
    child: SparkPlan,
    override val isBarrier: Boolean,
    override val profile: Option[ResourceProfile])
  extends DataFlintMapInBatchExec_4_1 with Logging {

  override def nodeName: String = "DataFlintMapInPandas"

  logInfo("DataFlint MapInPandas (Spark 4.1) is connected")

  override protected val pythonEvalType: Int = PythonEvalType.SQL_MAP_PANDAS_ITER_UDF

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan =
    copy(child = newChildren.head)
}

/**
 * DataFlint instrumented PythonMapInArrowExec for Spark 4.1.x.
 * Replaces the original PythonMapInArrowExec in the physical plan.
 */
case class DataFlintPythonMapInArrowExec_4_1(
    func: Expression,
    output: Seq[Attribute],
    child: SparkPlan,
    override val isBarrier: Boolean,
    override val profile: Option[ResourceProfile])
  extends DataFlintMapInBatchExec_4_1 with Logging {

  override def nodeName: String = "DataFlintMapInArrow"

  logInfo("DataFlint MapInArrow (Spark 4.1) is connected")

  override protected val pythonEvalType: Int = PythonEvalType.SQL_MAP_ARROW_ITER_UDF

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan =
    copy(child = newChildren.head)
}
