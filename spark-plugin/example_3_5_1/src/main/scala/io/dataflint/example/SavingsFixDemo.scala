package io.dataflint.example

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.{Files, Paths}

/**
 * Before/after scenarios for the savings estimate: each scenario runs the same query twice,
 * first with a common mistake and then with it fixed, so the predicted savings can be compared
 * with the real cost difference in the history server (http://localhost:18080).
 *
 *   SavingsFixDemo setup                         write the input tables
 *   SavingsFixDemo <scenario> <baseline|fixed>   run one side of a scenario
 *
 * Scenarios:
 *   etl         all of the mistakes below in one orders ETL
 *   skew        repartition by a key where 50% of rows share one value / round-robin repartition
 *   smalltasks  AQE disabled with 8000 shuffle partitions / AQE on with default partitions
 *   broadcast   sort-merge join with a 100k-row dimension table / broadcast join
 *   smallfiles  read a table stored as 2000 tiny files / read the compacted table
 *
 * Runs in local[8] by default; under spark-submit it uses the submitted master instead.
 */
object SavingsFixDemo {
  val dataDir: String = sys.env.getOrElse("DATAFLINT_DEMO_DIR", "/tmp/dataflint-savings-demo")
  val eventLogDir = "/tmp/spark-events"

  def main(args: Array[String]): Unit = {
    val scenario = args.headOption.getOrElse("etl")
    val variant = args.lift(1).getOrElse("baseline")
    val isSetup = scenario == "setup"
    Files.createDirectories(Paths.get(eventLogDir))

    val builder = SparkSession
      .builder()
      .appName(if (isSetup) "DataFlint Savings Demo - setup" else s"DataFlint Savings Demo - $scenario $variant")
      .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin")
      .config("spark.dataflint.telemetry.enabled", value = false)
      .config("spark.eventLog.enabled", !isSetup)
      .config("spark.eventLog.dir", eventLogDir)
    if (!sys.props.contains("spark.master")) builder.master("local[8]")
    val spark = builder.getOrCreate()

    try {
      val isBaseline = variant == "baseline"
      scenario match {
        case "setup" => writeTables(spark)
        case "etl" => runEtl(spark, isBaseline)
        case "skew" => runSkew(spark, isBaseline)
        case "smalltasks" => runSmallTasks(spark, isBaseline)
        case "broadcast" => runBroadcast(spark, isBaseline)
        case "smallfiles" => runSmallFiles(spark, isBaseline)
        case other => sys.error(s"unknown scenario: $other")
      }
    } finally {
      spark.stop()
    }
  }

  def writeTables(spark: SparkSession): Unit = {
    import spark.implicits._
    val orders = spark.range(0, 20000000L, 1, 16)
      .select(
        $"id".as("order_id"),
        // half of the orders are guest checkouts with customer_id 0
        when($"id" % 2 === 0, lit(0L)).otherwise($"id" % 100000).as("customer_id"),
        (rand(1) * 100).as("amount"),
        date_add(lit("2026-01-01").cast("date"), ($"id" % 300).cast("int")).as("order_date"))
    orders.write.mode("overwrite").parquet(s"$dataDir/orders")
    orders.repartition(2000).write.mode("overwrite").parquet(s"$dataDir/orders_small_files")

    spark.range(0, 100000L, 1, 1)
      .select(
        $"id".as("customer_id"),
        element_at(array(lit("US"), lit("UK"), lit("DE"), lit("IL"), lit("FR")), ($"id" % 5 + 1).cast("int")).as("country"))
      .write.mode("overwrite").parquet(s"$dataDir/customers")
  }

  def orders(spark: SparkSession): DataFrame = spark.read.parquet(s"$dataDir/orders")

  def customers(spark: SparkSession): DataFrame = spark.read.parquet(s"$dataDir/customers")

  // per-row CPU work, so the cost of a query follows how its rows are spread over tasks
  def withOrderHash(df: DataFrame): DataFrame =
    df.withColumn("order_hash", sha2(concat_ws("|", col("order_id"), col("amount")), 256))

  def runEtl(spark: SparkSession, isBaseline: Boolean): Unit = {
    import spark.implicits._
    if (isBaseline) {
      spark.conf.set("spark.sql.adaptive.enabled", "false")
      spark.conf.set("spark.sql.shuffle.partitions", "8000")
      spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    }

    spark.sparkContext.setJobDescription("Revenue by country (orders join customers)")
    val customersSide = if (isBaseline) customers(spark) else broadcast(customers(spark))
    withOrderHash(orders(spark).join(customersSide, "customer_id"))
      .groupBy($"country")
      .agg(sum($"amount").as("revenue"), max($"order_hash").as("max_hash"))
      .show()

    spark.sparkContext.setJobDescription("Daily revenue")
    orders(spark).groupBy($"order_date").agg(sum($"amount").as("revenue")).orderBy($"order_date").show(5)
  }

  def runSkew(spark: SparkSession, isBaseline: Boolean): Unit = {
    spark.sparkContext.setJobDescription("Hash orders after repartition")
    val repartitioned =
      if (isBaseline) orders(spark).repartition(200, col("customer_id")) else orders(spark).repartition(200)
    withOrderHash(repartitioned).agg(max("order_hash")).show()
  }

  def runSmallTasks(spark: SparkSession, isBaseline: Boolean): Unit = {
    import spark.implicits._
    if (isBaseline) {
      spark.conf.set("spark.sql.adaptive.enabled", "false")
      spark.conf.set("spark.sql.shuffle.partitions", "8000")
    }
    spark.sparkContext.setJobDescription("Daily revenue")
    orders(spark).groupBy($"order_date").agg(sum($"amount").as("revenue")).orderBy($"order_date").show(5)
  }

  def runBroadcast(spark: SparkSession, isBaseline: Boolean): Unit = {
    import spark.implicits._
    if (isBaseline) {
      spark.conf.set("spark.sql.adaptive.enabled", "false")
      spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    }
    spark.sparkContext.setJobDescription("Revenue by country (orders join customers on a non-skewed key)")
    val customersSide = if (isBaseline) customers(spark) else broadcast(customers(spark))
    orders(spark)
      .withColumn("customer_id", $"order_id" % 100000)
      .join(customersSide, "customer_id")
      .groupBy($"country")
      .agg(sum($"amount").as("revenue"))
      .show()
  }

  def runSmallFiles(spark: SparkSession, isBaseline: Boolean): Unit = {
    import spark.implicits._
    spark.sparkContext.setJobDescription("Revenue by date from the orders table")
    val table = if (isBaseline) "orders_small_files" else "orders"
    withOrderHash(spark.read.parquet(s"$dataDir/$table"))
      .groupBy($"order_date")
      .agg(sum($"amount").as("revenue"), max($"order_hash").as("max_hash"))
      .orderBy($"order_date")
      .show(5)
  }
}
