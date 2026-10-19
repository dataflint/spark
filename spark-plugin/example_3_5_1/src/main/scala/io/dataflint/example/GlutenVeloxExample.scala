package io.dataflint.example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object GlutenVeloxExample extends App {
  val spark = SparkSession
    .builder()
    .appName("GlutenVeloxExample")
    .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin,org.apache.gluten.GlutenPlugin")
    .config("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
    .config("spark.memory.offHeap.enabled", "true")
    .config("spark.memory.offHeap.size", "4g")
    .config("spark.ui.port", "10000")
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", "/tmp/spark-events")
    .config("spark.dataflint.telemetry.enabled", value = false)
    .config("spark.sql.maxMetadataStringLength", "10000")
    .config("spark.sql.adaptive.enabled", "true")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  def shakespeareDF: DataFrame = spark.read
    .format("csv")
    .option("sep", ";")
    .option("inferSchema", true)
    .load("./test_data/will_play_text.csv")
    .toDF("line_id", "play_name", "speech_number", "line_number", "speaker", "text_entry")

  // --- Filter + Project ---
  spark.sparkContext.setJobDescription("Filter and Select")
  val filtered = shakespeareDF
    .filter($"speaker".isNotNull && $"line_id" > 100)
    .select($"play_name", $"speaker", $"text_entry", $"speech_number")
  filtered.show(10, truncate = false)

  // --- Aggregation (GroupBy + Count/Sum) ---
  spark.sparkContext.setJobDescription("GroupBy Aggregation")
  val linesPerSpeaker = shakespeareDF
    .filter($"speaker".isNotNull)
    .groupBy("play_name", "speaker")
    .agg(
      count("*").alias("line_count"),
      sum("speech_number").alias("total_speech_numbers"),
      avg("speech_number").alias("avg_speech_number")
    )
  linesPerSpeaker.show(20, truncate = false)

  // --- Sort ---
  spark.sparkContext.setJobDescription("Sort by line count")
  val sortedSpeakers = linesPerSpeaker
    .orderBy(col("line_count").desc)
  sortedSpeakers.show(20, truncate = false)

  // --- Broadcast Hash Join ---
  spark.sparkContext.setJobDescription("Broadcast Hash Join")
  val topSpeakers = linesPerSpeaker
    .filter($"line_count" > 50)
    .select($"speaker".alias("top_speaker"), $"play_name".alias("top_play"))
  val broadcastJoined = shakespeareDF
    .join(broadcast(topSpeakers), $"speaker" === $"top_speaker" && $"play_name" === $"top_play")
  println(s"Broadcast join result count: ${broadcastJoined.count()}")

  // --- Sort Merge Join (disable broadcast to force SMJ) ---
  spark.sparkContext.setJobDescription("Sort Merge Join")
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
  val plays1 = shakespeareDF
    .groupBy("play_name")
    .agg(count("*").alias("total_lines"))
    .repartition(10)
  val plays2 = shakespeareDF
    .groupBy("play_name")
    .agg(countDistinct("speaker").alias("unique_speakers"))
    .repartition(10)
  val smjResult = plays1.join(plays2, Seq("play_name"))
  smjResult.show(20, truncate = false)
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760)

  // --- Window Functions ---
  spark.sparkContext.setJobDescription("Window Functions")
  val speakerWindow = Window.partitionBy("play_name").orderBy(col("line_count").desc)
  val rankedSpeakers = linesPerSpeaker
    .withColumn("rank", rank().over(speakerWindow))
    .withColumn("dense_rank", dense_rank().over(speakerWindow))
    .withColumn("total_in_play", sum("line_count").over(Window.partitionBy("play_name")))
    .withColumn("pct", round(col("line_count") / col("total_in_play") * 100, 2))
    .filter(col("rank") <= 3)
    .orderBy("play_name", "rank")
  rankedSpeakers.show(30, truncate = false)

  // --- Explode / Generate ---
  spark.sparkContext.setJobDescription("Explode words from text")
  val words = shakespeareDF
    .filter($"text_entry".isNotNull)
    .select($"play_name", $"speaker", explode(split($"text_entry", "\\s+")).alias("word"))
    .filter(length($"word") > 0)
  val wordCounts = words
    .groupBy("word")
    .agg(count("*").alias("word_count"))
    .orderBy(col("word_count").desc)
  wordCounts.show(20, truncate = false)

  // --- Union + distinct ---
  spark.sparkContext.setJobDescription("Union and Distinct")
  val hamlet = shakespeareDF.filter($"play_name" === "Hamlet").select("speaker")
  val macbeth = shakespeareDF.filter($"play_name" === "macbeth").select("speaker")
  val allSpeakers = hamlet.union(macbeth).distinct()
  println(s"Distinct speakers in Hamlet + Macbeth: ${allSpeakers.count()}")

  println("GlutenVeloxExample completed. Spark UI available at http://localhost:10000")
  println("Press Ctrl+C to stop.")
  Thread.sleep(Long.MaxValue)
}
