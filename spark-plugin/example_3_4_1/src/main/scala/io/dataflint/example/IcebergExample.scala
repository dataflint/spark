package io.dataflint.example

import org.apache.spark.sql.SparkSession

object IcebergExample extends App{
  val spark = SparkSession
    .builder()
    .appName("Iceberg Example")
    .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin")
    .config("spark.dataflint.telemetry.enabled", false)
    .config("spark.ui.port", "10000")
    .config("spark.sql.maxMetadataStringLength", "10000")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    .config("spark.sql.catalog.spark_catalog.type", "hive")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "/tmp/iceberg-example/warehouse")
    .config("spark.sql.defaultCatalog", "local")
    .config("spark.sql.catalog.local.metrics-reporter-impl", "org.apache.spark.dataflint.iceberg.DataflintIcebergMetricsReporter")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setJobDescription("Drop table if exists")
  spark.sql("DROP TABLE IF EXISTS demo.nyc.taxis PURGE")

  spark.sparkContext.setJobDescription("Create table")
  spark.sql(
    """
      |CREATE TABLE demo.nyc.taxis
      |(
      |  vendor_id bigint,
      |  trip_id bigint,
      |  trip_distance float,
      |  fare_amount double,
      |  store_and_fwd_flag string
      |)
      |PARTITIONED BY (vendor_id);
      |""".stripMargin)

  spark.sparkContext.setJobDescription("Insert 4 records to table")
  spark.sql(
    """
      |INSERT INTO demo.nyc.taxis
      |VALUES (1, 1000371, 1.8, 15.32, 'N'), (2, 1000372, 2.5, 22.15, 'N');
      |""".stripMargin)

  spark.sparkContext.setJobDescription("Select from table")
  spark.sql("SELECT * FROM demo.nyc.taxis").show()

  scala.io.StdIn.readLine()
}

