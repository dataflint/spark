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

  // Preserves ALL of child's existing metrics (spillSize, numOutputRows, etc.) + adds duration
  override lazy val metrics: Map[String, SQLMetric] =
    child.metrics ++ Map(MetricsUtils.getTimingMetric("duration")(sparkContext))

  // Delegate prepare() to child so that DataWritingCommandExec (and similar nodes that
  // override doPrepare) run their pre-execution setup. child is not in the plan tree, so
  // Spark won't call prepare() on it automatically. prepare() is idempotent so the
  // recursive call on the grandchildren is safe.
  override protected def doPrepare(): Unit = child.prepare()

  override protected def doExecute(): RDD[InternalRow] =
    DataFlintRDDUtils.withDurationMetric(child.execute(), longMetric("duration"))

  // Write path: DataWritingCommandExec does work eagerly in sideEffectResult (a lazy val),
  // triggered by executeCollect(). doExecute()/withDurationMetric only wraps the trivial
  // result RDD and misses the actual I/O. This override captures the full write duration.
  // For non-write nodes this is harmless — executeCollect() on TimedExec is only called
  // when TimedExec is the plan root (writes), not when it's inside WSCE (codegen path).
  override def executeCollect(): Array[InternalRow] = {
    val start = System.nanoTime()
    val result = child.executeCollect()
    val durationMs = (System.nanoTime() - start) / (1000 * 1000)
    val metric = longMetric("duration")
    metric += durationMs
    // Driver-side accumulator updates are invisible to the SQL status listener.
    // Post explicitly so the metric appears in the Spark UI alongside executor-side metrics.
    val executionId = sparkContext.getLocalProperty(org.apache.spark.sql.execution.SQLExecution.EXECUTION_ID_KEY)
    if (executionId != null) {
      org.apache.spark.sql.execution.metric.SQLMetrics.postDriverMetricUpdates(
        sparkContext, executionId, Seq(metric))
    }
    result
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

  override def inputRDDs(): Seq[RDD[InternalRow]] =
    child.asInstanceOf[CodegenSupport].inputRDDs()

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
   * full build phase — both are correct.
   */
  override protected def doProduce(ctx: CodegenContext): String = {
    val durationTerm = metricTerm(ctx, "duration")
    val startTime    = ctx.freshName("timedExecStart")
    val accumulated  = ctx.addMutableState("long", ctx.freshName("timedExecAccNs"),
      v => s"$v = 0L;")
    val childCode    = child.asInstanceOf[CodegenSupport].produce(ctx, this)
    // Test-only: inject a per-partition sleep into the generated code so tests can verify
    // that the codegen timing path actually captures wall-clock time.
    val sleepMs = sparkContext.conf.getLong("spark.dataflint.test.codegenSleepMs", 0L)
    val sleepCode = if (sleepMs > 0) s"try { Thread.sleep(${sleepMs}L); } catch (InterruptedException e) { }" else ""
    s"""
       |long $startTime = System.nanoTime();
       |$sleepCode
       |$childCode
       |$accumulated += System.nanoTime() - $startTime;
       |$durationTerm.add($accumulated / $NANOS_PER_MILLIS);
       |$accumulated = 0L;
     """.stripMargin
  }

  // Fully transparent — no per-row logic, just forward to parent
  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String =
    consume(ctx, input)
}

object TimedExec {
  def apply(child: SparkPlan): TimedExec = new TimedExec(child)
}