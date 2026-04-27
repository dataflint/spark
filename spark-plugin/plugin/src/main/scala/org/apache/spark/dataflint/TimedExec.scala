package org.apache.spark.dataflint

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.util.DateTimeConstants.NANOS_PER_MILLIS
import org.apache.spark.sql.execution.{CodegenSupport, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * A generic SparkPlan wrapper that adds a `duration` metric to any node while preserving
 * all of the child's existing metrics (spillSize, numOutputRows, etc.).
 *
 * ## Single node in Spark plan graph
 * `children` is set to `child.children` (the wrapped node's own children), so `TimedExec`
 * appears as ONE node in the Spark plan tree and `SparkPlanGraph`. The wrapped `child` is
 * not a separate visible node, eliminating the "double node" problem in both Spark's native
 * SQL UI and the DataFlint UI.
 *
 * ## Node name
 * `nodeName` is prefixed with "DataFlint" so the node is clearly identified in plan displays.
 *
 * ## Plan reconstruction (Spark 3.2+)
 * When Spark updates our children (= child.children) via plan transformations, we rebuild
 * `child` with the new grandchildren via `child.withNewChildren(newChildren)`, then wrap
 * the rebuilt child in a new `TimedExec`.
 *
 * ## Spark 3.0/3.1 compatibility
 * Extends SparkPlan directly (not UnaryExecNode) to avoid a `UnaryLike` reference in
 * compiled bytecode, which would cause NoClassDefFoundError on Spark 3.0/3.1.
 * productElement/productArity support `makeCopy` on Spark 3.0/3.1.
 *
 * ## Codegen
 * `TimedExec` does NOT implement `CodegenSupport`. For nodes that support codegen,
 * `TimedWithCodegenExec` extends this class and adds codegen support. This avoids
 * ClassCastExceptions on Spark 3.0/3.1 where some nodes (e.g. FileSourceScanExec)
 * do not implement `CodegenSupport`.
 * Use `TimedExec.apply()` to automatically pick the right variant.
 */
class TimedExec(val child: SparkPlan) extends SparkPlan with Logging {
  override def nodeName: String = "DataFlint" + child.nodeName
  override def output: Seq[Attribute] = child.output

  // On Spark 3.2+: transparent — children = child.children, so TimedExec appears as one node
  // in the plan graph. withNewChildrenInternal rebuilds child with the new grandchildren.
  // On Spark 3.0/3.1: non-transparent — children = Seq(child), because 3.1's withNewChildren
  // maps product elements via containsChild which can't see through the transparent wrapper.
  // Shows two nodes in the plan graph, but plan transformations work correctly.
  override def children: Seq[SparkPlan] =
    if (TimedExec.isLegacySpark) Seq(child) else child.children

  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
  override def supportsColumnar: Boolean = child.supportsColumnar

  // Preserves ALL of child's existing metrics (spillSize, numOutputRows, etc.) + adds duration and rddId
  override lazy val metrics: Map[String, SQLMetric] =
    child.metrics ++ Map(
      MetricsUtils.getTimingMetric("duration")(sparkContext),
      MetricsUtils.getSizeMetric("rddId")(sparkContext)
    )

  // Delegate prepare() to child so that DataWritingCommandExec (and similar nodes that
  // override doPrepare) run their pre-execution setup. child is not in the plan tree, so
  // Spark won't call prepare() on it automatically. prepare() is idempotent so the
  // recursive call on the grandchildren is safe.
  override protected def doPrepare(): Unit = child.prepare()

  protected def postRddId(rddId: Int): Unit = {
      val rddIdMetric = longMetric("rddId")
      rddIdMetric += rddId
      MetricsUtils.postDriverMetrics(sparkContext, rddIdMetric)
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val childRdd = child.execute()
    postRddId(childRdd.id)
    DataFlintRDDUtils.withDurationMetric(childRdd, longMetric("duration"))
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val childRdd = child.executeColumnar()
    postRddId(childRdd.id)
    DataFlintRDDUtils.withDurationMetricColumnar(childRdd, longMetric("duration"))
  }

  // Write path: DataWritingCommandExec does work eagerly in sideEffectResult (a lazy val),
  // triggered by executeCollect(). doExecute()/withDurationMetric only wraps the trivial
  // result RDD and misses the actual I/O.
  //
  // To get per-partition timing (consistent with codegen/RDD paths), we reconstruct
  // DataWritingCommandExec with the data-producing plan wrapped in an RDDTimingWrapper.
  // The write command consumes this timed RDD via sparkContext.runJob, so the wall-clock-
  // per-partition timing captures both data production AND write I/O (writes happen between
  // next() calls on the timed iterator).
  //
  // Spark 3.4+ inserts WriteFilesExec between DataWritingCommandExec and the data plan.
  // cmd.run() type-checks for WriteFilesExec, so we wrap the data plan INSIDE WriteFilesExec
  // (one level deeper), preserving the type check. On older Spark we wrap the child directly.
  override def executeCollect(): Array[InternalRow] = {
    if (child.getClass.getSimpleName == "DataWritingCommandExec") {
      val durationMetric = longMetric("duration")
      val innerChild = child.children.head
      val wrappedChild = if (innerChild.getClass.getSimpleName == "WriteFilesExec") {
        // Spark 3.4+: wrap the data plan inside WriteFilesExec
        val dataPlan = innerChild.children.head
        val timedDataPlan = new TimedExec.RDDTimingWrapper(dataPlan, durationMetric)
        val wrappedWriteFiles = innerChild.withNewChildren(IndexedSeq(timedDataPlan))
        child.withNewChildren(IndexedSeq(wrappedWriteFiles))
      } else {
        // Older Spark: wrap the data plan directly
        val timedDataPlan = new TimedExec.RDDTimingWrapper(innerChild, durationMetric)
        child.withNewChildren(IndexedSeq(timedDataPlan))
      }
      wrappedChild.executeCollect()
    } else {
      super.executeCollect()
    }
  }

  override def canEqual(that: Any): Boolean = that.isInstanceOf[TimedExec]
  // productArity/productElement support makeCopy on Spark 3.0/3.1 (constructor arg = child)
  override def productArity: Int = 1
  override def productElement(n: Int): Any =
    if (n == 0) child else throw new IndexOutOfBoundsException(s"$n")

  // On 3.2+: children = child.children, so newChildren are the grandchildren → rebuild child.
  // On 3.0/3.1: children = Seq(child), so newChildren has one element → the new child itself.
  // (3.0/3.1 doesn't call withNewChildrenInternal, but makeCopy handles it via productElement.)
  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan =
    if (TimedExec.isLegacySpark) TimedExec(newChildren.head)
    else TimedExec(child.withNewChildren(newChildren))
}

/**
 * TimedExec variant for nodes that implement CodegenSupport.
 * Adds codegen timing by wrapping child.produce() with nanoTime deltas.
 * Only instantiated via TimedExec.apply() when child is CodegenSupport.
 */
class TimedWithCodegenExec(override val child: SparkPlan) extends TimedExec(child) with CodegenSupport {

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    val rdds = child.asInstanceOf[CodegenSupport].inputRDDs()
    rdds.headOption.foreach(rdd => postRddId(rdd.id))
    rdds
  }

  // On 3.2+ (transparent): children = child.children, multi-child nodes (joins) expose
  // multiple children which breaks codegen assumptions → restrict to single-child.
  // On 3.0/3.1 (non-transparent): children = Seq(child), always length 1 → no restriction.
  override def supportCodegen: Boolean = {
    val c = child.asInstanceOf[CodegenSupport]
    c.supportCodegen && (TimedExec.isLegacySpark || child.children.length <= 1)
  }

  override def needCopyResult: Boolean = child.asInstanceOf[CodegenSupport].needCopyResult

  override protected def doProduce(ctx: CodegenContext): String = {
    ctx.freshNamePrefix = ctx.freshNamePrefix.replaceAll("[^a-zA-Z0-9_]", "")
    val durationTerm = metricTerm(ctx, "duration")
    val startTime    = ctx.freshName("timedExecStart")
    val accumulated  = ctx.addMutableState("long", ctx.freshName("timedExecAccNs"),
      v => s"$v = 0L;")
    val childCode    = child.asInstanceOf[CodegenSupport].produce(ctx, this)
    val sleepMs = sparkContext.conf.getLong("spark.dataflint.test.codegenSleepMs", 0L)
    val sleepCode = if (sleepMs > 0) {
      val sleepDone = ctx.addMutableState("boolean", ctx.freshName("timedExecSleepDone"),
        v => s"$v = false;")
      s"""if (!$sleepDone) {
         |  try { Thread.sleep(${sleepMs}L); } catch (InterruptedException e) { }
         |  $sleepDone = true;
         |}""".stripMargin
    } else ""
    s"""
       |long $startTime = System.nanoTime();
       |$sleepCode
       |try {
       |  $childCode
       |} finally {
       |  $accumulated += System.nanoTime() - $startTime;
       |  $durationTerm.add($accumulated / $NANOS_PER_MILLIS);
       |  $accumulated = 0L;
       |}
     """.stripMargin
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String =
    consume(ctx, input)

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan =
    if (TimedExec.isLegacySpark) TimedExec(newChildren.head)
    else TimedExec(child.withNewChildren(newChildren))
}

object TimedExec {
  // Spark 3.0/3.1's withNewChildren uses mapProductIterator + containsChild which is
  // incompatible with the transparent wrapper (children = child.children). Detected by
  // checking for withNewChildrenInternal which was added in Spark 3.2.
  val isLegacySpark: Boolean = {
    val parts = org.apache.spark.SPARK_VERSION.split("\\.")
    parts.length >= 2 && parts(0).toInt == 3 && parts(1).toInt < 2
  }

  def apply(child: SparkPlan): TimedExec = child match {
    case _: CodegenSupport => new TimedWithCodegenExec(child)
    case _                 => new TimedExec(child)
  }

  /**
   * A minimal SparkPlan that wraps execute() with per-partition duration timing.
   * Used by the write path: inserted inside WriteFilesExec (or as the direct child on older
   * Spark) so that the write command consumes a timed RDD per partition.
   */
  private[dataflint] class RDDTimingWrapper(val child: SparkPlan, durationMetric: SQLMetric) extends SparkPlan {
    override def output: Seq[Attribute] = child.output
    override def children: Seq[SparkPlan] = Seq(child)
    override def outputPartitioning: Partitioning = child.outputPartitioning
    override def outputOrdering: Seq[SortOrder] = child.outputOrdering
    override def supportsColumnar: Boolean = child.supportsColumnar

    override protected def doExecute(): RDD[InternalRow] =
      DataFlintRDDUtils.withDurationMetric(child.execute(), durationMetric)

    override protected def doExecuteColumnar(): RDD[ColumnarBatch] =
      DataFlintRDDUtils.withDurationMetricColumnar(child.executeColumnar(), durationMetric)

    override def productArity: Int = 1
    override def productElement(n: Int): Any =
      if (n == 0) child else throw new IndexOutOfBoundsException(s"$n")
    override def canEqual(that: Any): Boolean = that.isInstanceOf[RDDTimingWrapper]
    override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan =
      new RDDTimingWrapper(newChildren.head, durationMetric)
  }
}