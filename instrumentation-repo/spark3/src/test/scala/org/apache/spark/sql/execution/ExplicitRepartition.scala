package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.metric.{SQLMetrics, SQLShuffleReadMetricsReporter, SQLShuffleWriteMetricsReporter}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.{MapOutputStatistics, ShuffleDependency}

import scala.concurrent.Future

// 1. Logical Plan Node for Adaptive Repartition
case class ExplicitRepartition(
                                      partitionExprs: Seq[Expression],
                                      child: LogicalPlan
                                    ) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
  override protected def withNewChildInternal(newChild: LogicalPlan): ExplicitRepartition =
    copy(child = newChild)
}

// 2. DataFrame API Extension
object ExplicitRepartitionOps {
  implicit class AdaptiveRepartitionDataFrame(df: DataFrame) {
    @scala.annotation.varargs
    def adaptiveRepartition(cols: Column*): DataFrame = {
      val exprs = cols.map(_.expr)
      Dataset[Row](df.sparkSession, ExplicitRepartition(exprs, df.logicalPlan))(ExpressionEncoder.apply(df.schema))
    }
  }
}

// 3. Custom Shuffle Exchange that marks itself for adaptive coalescing
case class ExplicitShuffleExchangeExec(
                                        override val outputPartitioning: Partitioning,
                                        child: SparkPlan
                                      ) extends ShuffleExchangeLike {

  override def nodeName: String = "AdaptiveExchange"

  private lazy val writeMetrics =
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)
  private[sql] lazy val readMetrics =
    SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)
  override lazy val metrics = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "numPartitions" -> SQLMetrics.createMetric(sparkContext, "number of partitions")
  ) ++ readMetrics ++ writeMetrics

  private lazy val serializer: Serializer =
    new UnsafeRowSerializer(child.output.size, longMetric("dataSize"))

  @transient lazy val inputRDD: RDD[InternalRow] = child.execute()

  @transient
  override lazy val mapOutputStatisticsFuture: Future[MapOutputStatistics] = {
    if (inputRDD.getNumPartitions == 0) {
      Future.successful(null)
    } else {
      sparkContext.submitMapStage(shuffleDependency)
    }
  }

  override def numMappers: Int = shuffleDependency.rdd.getNumPartitions
  override def numPartitions: Int = shuffleDependency.partitioner.numPartitions
  override def getShuffleRDD(partitionSpecs: Array[ShufflePartitionSpec]): RDD[InternalRow] = {
    new ShuffledRowRDD(shuffleDependency, readMetrics, partitionSpecs)
  }
  override def runtimeStatistics: Statistics = {
    val dataSize = metrics("dataSize").value
    val rowCount = metrics(SQLShuffleWriteMetricsReporter.SHUFFLE_RECORDS_WRITTEN).value
    Statistics(dataSize, Some(rowCount))
  }

  @transient
  lazy val shuffleDependency: ShuffleDependency[Int, InternalRow, InternalRow] = {
    val dep = ShuffleExchangeExec.prepareShuffleDependency(
      inputRDD,
      child.output,
      outputPartitioning,
      serializer,
      writeMetrics)
    metrics("numPartitions").set(dep.partitioner.numPartitions)
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(
      sparkContext, executionId, metrics("numPartitions") :: Nil)
    dep
  }

  private var cachedShuffleRDD: ShuffledRowRDD = null
  override protected def doExecute(): RDD[InternalRow] = {
    if (cachedShuffleRDD == null) {
      cachedShuffleRDD = new ShuffledRowRDD(shuffleDependency, readMetrics)
    }
    cachedShuffleRDD
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ExplicitShuffleExchangeExec =
    copy(child = newChild)

  // Override to indicate this exchange should be adaptively coalesced
  override def advisoryPartitionSize: Option[Long] = Some(1L) // Force minimal partition size for coalescing
  override def shuffleOrigin: ShuffleOrigin = REPARTITION_BY_COL
}

// 4. Custom AQE Rule to coalesce only ExplicitShuffleExchangeExec
object CoalesceExplicitShufflePartitions extends Rule[SparkPlan] {
  override val ruleName = "CoalesceAdaptiveShufflePartitions"

  def apply(plan: SparkPlan): SparkPlan = {
    if (!SQLConf.get.adaptiveExecutionEnabled) {
      return plan
    }

    plan transformUp {
      case aqeRead: AQEShuffleReadExec if aqeRead.child.isInstanceOf[ShuffleQueryStageExec] =>
        val stage = aqeRead.child.asInstanceOf[ShuffleQueryStageExec]
        if (stage.shuffle.isInstanceOf[ExplicitShuffleExchangeExec]) {
          coalesceAQEShuffleRead(aqeRead, stage)
        } else {
          aqeRead
        }
      case other => other
    }
  }

  private def coalesceAQEShuffleRead(aqeRead: AQEShuffleReadExec, stage: ShuffleQueryStageExec): SparkPlan = {
    stage.mapStats match {
      case Some(mapStats) =>
        val nonEmptyPartitions = mapStats.bytesByPartitionId.zipWithIndex
          .filter(_._1 > 0)
          .map(_._2)

        if (nonEmptyPartitions.length < mapStats.bytesByPartitionId.length) {
          // Create coalesced specs that only include non-empty partitions
          val coalescedSpecs = nonEmptyPartitions.map { partitionId =>
            CoalescedPartitionSpec(
              startReducerIndex = partitionId,
              endReducerIndex = partitionId + 1,
              dataSize = mapStats.bytesByPartitionId(partitionId)
            )
          }.toArray

          // Replace the existing AQEShuffleReadExec with one using coalesced specs
          AQEShuffleReadExec(stage, coalescedSpecs.map(_.asInstanceOf[ShufflePartitionSpec]))
        } else {
          aqeRead
        }
      case None => aqeRead
    }
  }
}

// Custom partitioning that uses raw hash values as partition IDs
class ExplicitHashPartitioning(
                                   expressions: Seq[Expression],
                                 ) extends HashPartitioning(expressions, 100000) {
  
  override def partitionIdExpression: Expression = {
    // Use absolute value of hash to ensure positive partition IDs
    // Each distinct value gets its own partition based on hash
    Pmod(new Murmur3Hash(expressions), Literal(100000))
  }
  
  override def toString: String = s"DynamicDistinctPartitioning(${expressions.mkString(", ")})"
}


// 5. Planning Strategy
object ExplicitRepartitionStrategy extends SparkStrategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ExplicitRepartition(partitionExprs, child) if partitionExprs.forall(_.resolved) =>
      val physicalChild = planLater(child)
      val partitioning = new ExplicitHashPartitioning(partitionExprs)
      // Create adaptive shuffle exchange
      val adaptiveExchange = ExplicitShuffleExchangeExec(partitioning, physicalChild)

      adaptiveExchange :: Nil
    case _ => Nil
  }
}

// 7. Extension to register all components
class ExplicitRepartitionExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    // Add physical planning strategy
    extensions.injectPlannerStrategy(_ => ExplicitRepartitionStrategy)

    // Add AQE rule for coalescing
    extensions.injectQueryStageOptimizerRule(_ => CoalesceExplicitShufflePartitions)
  }
}

// 8. Usage Example
object ExplicitRepartitionExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ExplicitRepartitionTest")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")
      .getOrCreate()

    // Register the extension
    new ExplicitRepartitionExtension().apply(spark.extensions)

    import ExplicitRepartitionOps._
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val df = spark.range(10000).toDF("id")
      .withColumn("b", $"id" % 250)

    // Usage: Adaptive repartition that coalesces to unique values
    val result = df
      .repartition($"b")  // Will repartition by 'b' and coalesce to 5 partitions (unique values of b)
      .withColumn("partition_ida", spark_partition_id())
      .adaptiveRepartition($"b")  // Will repartition by 'b' and coalesce to 5 partitions (unique values of b)
      .withColumn("partition_idb", spark_partition_id())
      .groupBy("b")
      .agg(count("*").as("count"), min("partition_ida"), max("partition_ida"), min("partition_idb"), max("partition_idb"))

    result.explain(true)
    try {
      result.show()
    }  catch {
      case e: Throwable =>
        e.printStackTrace()
//        Thread.sleep(300000)
    }finally {
      Thread.sleep(300000)
      spark.stop()
    }

  }
}
