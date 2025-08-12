package io.dataflint.example

import org.apache.spark.sql.{SparkSession, functions => F}

object ProjectExample extends App {
  val spark = SparkSession
    .builder()
    .appName("Select ETL")
    .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin")
    .config("spark.ui.port", "10000")
    .master("local[*]")
    .getOrCreate()

  val salesFilesLocation = sys.env.getOrElse("SALES_FILES_LOCATION", throw new Exception("SALES_FILES_LOCATION env var not set"))

  val df = spark.read.load(salesFilesLocation)

  df.count()

  scala.io.StdIn.readLine("job ended, press any key to continue..")

  spark.stop()
}

