package io.dataflint.example

import io.dataflint.spark.SparkDataflint
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object IcebergExample333 extends App with Logging {
  logInfo("start iceberg example")
  logInfo("IcebergExample class loader: " + getClass.getClassLoader.toString)
  val spark = SparkSession
    .builder()
    .appName("Iceberg Example")
    .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin")
    .config("spark.dataflint.telemetry.enabled", false)
    .config("spark.ui.port", "10000")
    .config("spark.sql.maxMetadataStringLength", "10000")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.metrics-reporter-impl", "org.apache.spark.dataflint.iceberg.DataflintIcebergMetricsReporter")

    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "/tmp/iceberg-example/warehouse")
    .config("spark.sql.defaultCatalog", "local")
    .config("spark.dataflint.iceberg.autoCatalogDiscovery", false)
    .config("spark.eventLog.enabled", "true")
    .master("local[*]")
    .getOrCreate()

  SparkDataflint.install(spark.sparkContext)

  spark.sparkContext.setJobDescription("Drop table if exists")
  spark.sql("DROP TABLE IF EXISTS demo.nyc.taxis PURGE")
  spark.sql("DROP TABLE IF EXISTS demo.nyc.taxis_unpartitioned PURGE")
  spark.sql("DROP TABLE IF EXISTS demo.nyc.taxis_small_files PURGE")
  spark.sql("DROP TABLE IF EXISTS demo.nyc.taxis_read_on_merge PURGE")


  spark.sparkContext.setJobDescription("Create taxis table")
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

  spark.sparkContext.setJobDescription("Insert 2 records to taxis table")
  spark.sql(
    """
      |INSERT INTO demo.nyc.taxis
      |VALUES (1, 1000371, 1.8, 15.32, 'N'), (2, 1000372, 2.5, 22.15, 'N');
      |""".stripMargin)
//
  spark.sparkContext.setJobDescription("Insert 2 records to taxis table (2)")
  spark.sql(
    """
      |INSERT INTO demo.nyc.taxis
      |VALUES (1, 1000375, 1.8, 15.32, 'N'), (2, 1000376, 2.5, 22.15, 'N');
      |""".stripMargin)

  spark.sparkContext.setJobDescription("Select from table")
  spark.sql("SELECT * FROM demo.nyc.taxis").show()

  spark.sparkContext.setJobDescription("Select by partition field table")
  spark.sql("SELECT * FROM demo.nyc.taxis WHERE vendor_id = 1").show()

  spark.sparkContext.setJobDescription("Select by non partition field table")
  spark.sql("SELECT * FROM demo.nyc.taxis WHERE trip_id = 1000375").show()

  spark.sparkContext.setJobDescription("Update record in table")
  spark.sql(
    """
      |UPDATE demo.nyc.taxis
      |SET fare_amount = 5.0
      |WHERE trip_id = 1000371
      |""".stripMargin)

  spark.sparkContext.setJobDescription("Delete record from table")
  spark.sql(
    """
      |DELETE FROM demo.nyc.taxis
      |WHERE trip_id = 1000371
      |""".stripMargin)

  spark.sparkContext.setJobDescription("Merge all records in table")
  spark.sql(
    """
      MERGE INTO demo.nyc.taxis t USING(SELECT * FROM demo.nyc.taxis) u ON t.trip_id = u.trip_id
      WHEN MATCHED THEN UPDATE SET
      t.fare_amount = t.fare_amount + u.fare_amount
      WHEN NOT MATCHED THEN INSERT *;
    """)

  spark.sparkContext.setJobDescription("Select from table after merge")
  spark.sql("SELECT * FROM demo.nyc.taxis").show()

  spark.sparkContext.setJobDescription("Compaction")
  spark.sql("""
      |CALL local.system.rewrite_data_files(
      |  table => 'demo.nyc.taxis',
      |  strategy => 'sort',
      |  sort_order => 'trip_id ASC NULLS LAST',
      |  options => map(
      |  'rewrite-all','true',
      |  'min-input-files','1',
      |  'rewrite-job-order','bytes-asc',
      |  'target-file-size-bytes','1073741824', -- 1GB
      |  'max-file-group-size-bytes','10737418240' -- 10GB
      |  )
      |)
      |""".stripMargin)


  val currentTimeISO = java.time.format.DateTimeFormatter.ISO_INSTANT.format(java.time.Instant.now())
  spark.sparkContext.setJobDescription("Expire snapshots")
  spark.sql(s"""
    CALL local.system.expire_snapshots('demo.nyc.taxis', TIMESTAMP '${currentTimeISO}', 1)
   """)

  spark.sparkContext.setJobDescription("Create taxis unpartitioned table")
  spark.sql(
    """
      |CREATE TABLE demo.nyc.taxis_unpartitioned
      |(
      |  vendor_id bigint,
      |  trip_id bigint,
      |  trip_distance float,
      |  fare_amount double,
      |  store_and_fwd_flag string
      |)
      |TBLPROPERTIES (
      |'write.distribution.mode'='range'
      |)
      |""".stripMargin)

  spark.sparkContext.setJobDescription("Set table sorting order")
  spark.sql("ALTER TABLE demo.nyc.taxis_unpartitioned WRITE ORDERED BY vendor_id, trip_id")

  spark.sparkContext.setJobDescription("Insert 100 records to taxis unpartitioned table")
  spark.sql(
    """
      |SELECT
      |    id as vendor_id,
      |    1000370 + id as trip_id,
      |    1.5 + (id % 100) * 0.1 as trip_distance,
      |    15.0 + (id % 100) * 0.7 as fare_amount,
      |    'N' as store_and_fwd_flag
      |FROM (
      |    SELECT id FROM range(1, 101)
      |) t
      |""".stripMargin)
      .writeTo("demo.nyc.taxis_unpartitioned")
      .append()

  spark.sparkContext.setJobDescription("Delete record from unpartitioned table")
  spark.sql(
    """
      |DELETE FROM demo.nyc.taxis_unpartitioned
      |WHERE trip_id = 1000371
      |""".stripMargin)

  spark.sparkContext.setJobDescription("Merge 20% of records to table")
  spark.sql(
    """
      MERGE INTO demo.nyc.taxis_unpartitioned t USING(
      SELECT
        id as vendor_id,
        1000370 + id as trip_id,
        1.5 + (id % 100) * 0.1 as trip_distance,
        15.0 + (id % 100) * 0.7 as fare_amount,
        'N' as store_and_fwd_flag
    FROM (
        SELECT id FROM range(90, 110)
    ) t
      ) u ON t.trip_id = u.trip_id
      WHEN MATCHED THEN UPDATE SET
      t.fare_amount = t.fare_amount + u.fare_amount
      WHEN NOT MATCHED THEN INSERT *;
    """)

  spark.sparkContext.setJobDescription("Create taxis small files table")
  spark.sql(
    """
      |CREATE TABLE demo.nyc.taxis_small_files
      |(
      |  vendor_id bigint,
      |  trip_id bigint,
      |  trip_distance float,
      |  fare_amount double,
      |  store_and_fwd_flag string
      |)
      |""".stripMargin)

  spark.sparkContext.setJobDescription("Insert small files to table")
  spark.sql(
      """
        |SELECT
        |    id as vendor_id,
        |    1000370 + id as trip_id,
        |    1.5 + (id % 100) * 0.1 as trip_distance,
        |    15.0 + (id % 100) * 0.7 as fare_amount,
        |    'N' as store_and_fwd_flag
        |FROM (
        |    SELECT id FROM range(1, 201)
        |) t
        |""".stripMargin)
    .repartition(200)
    .writeTo("demo.nyc.taxis_small_files")
    .append()

  spark.sparkContext.setJobDescription("Select from table with small files")
  spark.sql("SELECT * FROM demo.nyc.taxis_small_files").show(200)

  spark.sparkContext.setJobDescription("Create taxis read on merge")
  spark.sql(
    """
      |CREATE TABLE demo.nyc.taxis_read_on_merge
      |(
      |  vendor_id bigint,
      |  trip_id bigint,
      |  trip_distance float,
      |  fare_amount double,
      |  store_and_fwd_flag string
      |)
      |TBLPROPERTIES (
      |'write.delete.mode'='merge-on-read',
      |'write.update.mode'='merge-on-read',
      |'write.merge.mode'='merge-on-read',
      |'write.distribution.mode'='range'
      |)
      |""".stripMargin)

  spark.sql("ALTER TABLE demo.nyc.taxis_read_on_merge WRITE ORDERED BY vendor_id, trip_id")

  spark.sparkContext.setJobDescription("Insert 2 records to taxis_read_on_merge")
  spark.sql(
    """
      |INSERT INTO demo.nyc.taxis_read_on_merge
      |VALUES (1, 1000371, 1.8, 15.32, 'N'), (1, 1000372, 2.5, 22.15, 'N');
      |""".stripMargin)

  spark.sparkContext.setJobDescription("Delete record from taxis_read_on_merge")
  spark.sql(
    """
      |DELETE FROM demo.nyc.taxis_read_on_merge
      |WHERE trip_id = 1000371
      |""".stripMargin)

  spark.sparkContext.setJobDescription("Update record in taxis_read_on_merge")
  spark.sql(
    """
      |UPDATE demo.nyc.taxis_read_on_merge
      |SET fare_amount = 5.0
      |WHERE trip_id = 1000372
      |""".stripMargin)

  spark.sparkContext.setJobDescription("Merge all records in taxis_read_on_merge")
  spark.sql(
    """
      MERGE INTO demo.nyc.taxis_read_on_merge t USING(SELECT * FROM demo.nyc.taxis_read_on_merge) u ON t.trip_id = u.trip_id
      WHEN MATCHED THEN UPDATE SET
      t.fare_amount = t.fare_amount + u.fare_amount
      WHEN NOT MATCHED THEN INSERT *;
    """)

  scala.io.StdIn.readLine()
}
