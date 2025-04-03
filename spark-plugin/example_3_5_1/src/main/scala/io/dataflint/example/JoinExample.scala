package io.dataflint.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast

object JoinExample extends App {
  val spark = SparkSession
    .builder()
    .appName("JoinExample")
    .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin")
    .config("spark.ui.port", "10000")
    .config("spark.dataflint.telemetry.enabled", value = false)
    .config("spark.eventLog.enabled", "true")
    .config("spark.sql.maxMetadataStringLength", "10000")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val df1 = Seq(1, 2).toDF("id1")
  val df2 = Seq(1, 2, 3, 4).toDF("id2")

  spark.sparkContext.setJobDescription("Cross Join Broadcast Nested Loop Join")
  val result2 = df1.join(broadcast(df2), $"id1" > $"id2")
  result2.show()

  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  spark.sparkContext.setJobDescription("Cross Join Broadcast Cartesian Product")
  val result = df1.repartition(2).crossJoin(df2.repartition(2))
  result.show()

  // INNER JOIN EXAMPLE (Reduces rows)
  val df3 = Seq((1, "a"), (2, "b"), (3, "c"), (4, "d")).toDF("id", "value")
  val df4 = Seq((2, "x"), (4, "y")).toDF("id", "desc")

  spark.sparkContext.setJobDescription("Inner Join (reduces rows)")
  val innerJoinResult = df3.join(df4, Seq("id"), "inner")
  innerJoinResult.show()

  // LEFT OUTER JOIN EXAMPLE (Increases rows)
  val df5 = Seq((1, "a"), (1, "a")).toDF("id", "value")
  val df6 = Seq((1, "x"), (1, "y"), (1, "a"), (1, "b")).toDF("id", "desc")

  spark.sparkContext.setJobDescription("Left Outer Join (increases rows)")
  val leftJoinResult = df5.join(df6, Seq("id"), "left_outer")
  leftJoinResult.show()

  scala.io.StdIn.readLine()
  spark.stop()
}
