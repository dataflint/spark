package io.dataflint.example

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.{Files, Paths}

/**
 * Local dev playground: runs a mix of workloads (scan, aggregation, joins, cache, skew)
 * so most DataFlint views and alerts have something to show, then blocks until Enter
 * is pressed so the live UI stays up on http://localhost:10000/dataflint/.
 *
 * Event logs are written to /tmp/spark-events so the run can also be viewed later
 * in the local history server.
 */
object DevPlayground extends App {
  val eventLogDir = "/tmp/spark-events"
  Files.createDirectories(Paths.get(eventLogDir))

  val spark = SparkSession
    .builder()
    .appName("DataFlint Dev Playground")
    .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin")
    .config("spark.ui.port", "10000")
    .config("spark.dataflint.telemetry.enabled", value = false)
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", eventLogDir)
    .config("spark.sql.maxMetadataStringLength", "10000")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  def shakespeare: DataFrame = spark.read
    .format("csv")
    .option("sep", ";")
    .option("inferSchema", true)
    .load("./test_data/will_play_text.csv")
    .toDF("line_id", "play_name", "speech_number", "line_number", "speaker", "text_entry")

  spark.sparkContext.setJobDescription("Scan + aggregation: lines per speaker")
  shakespeare.groupBy($"play_name", $"speaker").count().orderBy(desc("count")).show(5)

  spark.sparkContext.setJobDescription("Explode + distinct: unique words")
  val uniqueWords = shakespeare.select(explode(split($"text_entry", " "))).distinct().count()
  println(s"number of unique words : $uniqueWords")

  spark.sparkContext.setJobDescription("Broadcast join")
  val plays = shakespeare.select($"play_name").distinct().withColumn("play_id", monotonically_increasing_id())
  shakespeare.join(broadcast(plays), "play_name").groupBy($"play_id").count().show(5)

  spark.sparkContext.setJobDescription("Sort-merge join (broadcast disabled)")
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
  val speakers = shakespeare.select($"speaker").distinct().withColumn("speaker_len", length($"speaker"))
  shakespeare.join(speakers, "speaker").groupBy($"speaker_len").count().show(5)
  spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")

  spark.sparkContext.setJobDescription("Cache + reuse")
  val cached = spark.range(0, 10000000L).repartition(100).cache()
  cached.count()
  cached.selectExpr("sum(id)").show()
  cached.unpersist()

  spark.sparkContext.setJobDescription("Partition skew (one slow task)")
  val numbers = spark.range(1, 100).repartition(100)
  numbers.mapPartitions(it => it.map { i => if (i == 50L) Thread.sleep(5000); i })(numbers.encoder).count()

  println("DataFlint UI: http://localhost:10000/dataflint/  — press Enter to stop the job")
  scala.io.StdIn.readLine()
  spark.stop()
}
