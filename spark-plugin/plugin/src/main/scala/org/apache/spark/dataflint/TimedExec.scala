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
 */
class TimedExec(val child: SparkPlan) extends SparkPlan with CodegenSupport with Logging {
  override def nodeName: String = "DataFlint" + child.nodeName
  override def output: Seq[Attribute] = child.output

  // Expose child's children directly so TimedExec appears as a single node in the plan graph.
  // The wrapped child is not visible in the tree; plan transformations see and update the
  // grandchildren directly, and withNewChildrenInternal rebuilds child with the new ones.
  override def children: Seq[SparkPlan] = child.children

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

  private def postRddId(rddId: Int): Unit = {
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

  // When Spark updates our children (= child's children), rebuild child with new children
  // and wrap in a new TimedExec. Used by Spark 3.2+ plan transformations (AQE, CollapseCodegen, etc.)
  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan =
    new TimedExec(child.withNewChildren(newChildren))

  // -----------------------------------------------------------------------------------------
  // Codegen execution path
  // -----------------------------------------------------------------------------------------

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    val rdds = child.asInstanceOf[CodegenSupport].inputRDDs()
    // Codegen path: doExecute() is not called, so post rddId here.
    // inputRDDs() is called during WSCE code generation on the driver.
    rdds.headOption.foreach(rdd => postRddId(rdd.id))
    rdds
  }

  // Delegate supportCodegen to child — must use child.supportCodegen, not just isInstanceOf.
  // Example: SortAggregateExec implements CodegenSupport but returns supportCodegen=false
  // when grouping keys are present (doProduce throws UnsupportedOperationException in that case).
  override def supportCodegen: Boolean = child match {
    case c: CodegenSupport => c.supportCodegen && child.children.length <= 1
    case _                 => false
  }

  // needCopyResult flows DOWN: the default asks children.head.needCopyResult.
  // With the transparent wrapper, children = child.children. For multi-child nodes (joins),
  // children.length > 1, which hits the default's UnsupportedOperationException branch.
  // Fix: ask child directly. child (e.g. SortMergeJoinExec) extends BlockingOperatorWithCodegen
  // which returns false, so no recursion.
  override def needCopyResult: Boolean = child match {
    case c: CodegenSupport => c.needCopyResult
    case _                 => false
  }

  // needStopCheck flows UP: do NOT override — the default (parent.needStopCheck) is correct.
  // Delegating to child.needStopCheck instead creates an infinite loop because
  // child.parent == TimedExec (set by doProduce's produce(ctx, this)), so:
  // TimedExec.needStopCheck → child.needStopCheck → child.parent.needStopCheck
  // → TimedExec.needStopCheck → ...
  /**
   * Wrap child.produce() with nanoTime deltas.
   *
   * Measures cumulative time of the full pipeline below this node.
   * Exclusive attribution is done post-hoc: exclusive(N) = D(N) - D(pipelined_child).
   *
   * The accumulated nanos are flushed to the SQLMetric after each produce invocation.
   * For pipelined children this happens per-row; for blocking children once after the
   * full build phase.
   *
   * Uses try/finally so the timing flushes even when blocking operators (SortExec, etc.)
   * exit early via shouldStop()/return — without this, duration would be 0.
   */
  override protected def doProduce(ctx: CodegenContext): String = {
    // CodegenSupport.variablePrefix is private and falls through to nodeName.toLowerCase
    // for unknown types (like TimedExec). Nodes with spaces in nodeName (e.g. RDDScanExec's
    // "Scan ExistingRDD") produce invalid Java identifiers. Sanitize the prefix here.
    ctx.freshNamePrefix = ctx.freshNamePrefix.replaceAll("[^a-zA-Z0-9_]", "")
    val durationTerm = metricTerm(ctx, "duration")
    val startTime    = ctx.freshName("timedExecStart")
    val accumulated  = ctx.addMutableState("long", ctx.freshName("timedExecAccNs"),
      v => s"$v = 0L;")
    val childCode    = child.asInstanceOf[CodegenSupport].produce(ctx, this)
    // Test-only: inject a per-partition sleep into the generated code so tests can verify
    // that the codegen timing path actually captures wall-clock time.
    // Uses a mutable flag so the sleep fires once per partition (not every processNext() call).
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

  // Fully transparent — no per-row logic, just forward to parent
  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String =
    consume(ctx, input)
}

object TimedExec {
  def apply(child: SparkPlan): TimedExec = new TimedExec(child)

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