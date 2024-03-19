package io.dataflint.example

import org.apache.spark.sql.SparkSession

import java.sql.Timestamp

object DeltaLakeExample extends App {
  val spark = SparkSession
    .builder()
    .appName("DeltaLakeExample")
    .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin")
    .config("spark.dataflint.telemetry.enabled", false)
    .config("spark.ui.port", "10000")
    .config("spark.sql.maxMetadataStringLength", "10000")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  spark.sparkContext.setJobDescription("Create Table")
  spark.sql("CREATE TABLE IF NOT EXISTS delta.`/tmp/delta-table` USING DELTA AS SELECT col1 as id FROM VALUES 0,1,2,3,4;")

  spark.sparkContext.setJobDescription("Insert data to table")
  spark.sql("INSERT OVERWRITE delta.`/tmp/delta-table` SELECT col1 as id FROM VALUES 5,6,7,8,9;")

  spark.sparkContext.setJobDescription("Select data from table")
  spark.sql("SELECT * FROM delta.`/tmp/delta-table`;").show()

  spark.sparkContext.setJobDescription("Insert overwrite data to table")
  spark.sql("INSERT OVERWRITE delta.`/tmp/delta-table` SELECT col1 as id FROM VALUES 5,6,7,8,9;")

  spark.sparkContext.setJobDescription("Update data from table")
  spark.sql("UPDATE delta.`/tmp/delta-table` SET id = id + 100 WHERE id % 2 == 0;")

  spark.sparkContext.setJobDescription("Delete data from table")
  spark.sql("DELETE FROM delta.`/tmp/delta-table` WHERE id % 2 == 0;")

  spark.sparkContext.setJobDescription("Create view from table")
  spark.sql("CREATE TEMP VIEW newData AS SELECT col1 AS id FROM VALUES 1,3,5,7,9,11,13,15,17,19;")

  spark.sparkContext.setJobDescription("Merge data to table")
  spark.sql(
    """MERGE INTO delta.`/tmp/delta-table` AS oldData
      |USING newData
      |ON oldData.id = newData.id
      |WHEN MATCHED
      |  THEN UPDATE SET id = newData.id
      |WHEN NOT MATCHED
      |  THEN INSERT (id) VALUES (newData.id);
      |""".stripMargin)

  spark.sparkContext.setJobDescription("Select data from table")
  spark.sql("SELECT * FROM delta.`/tmp/delta-table`;").show()

  spark.sparkContext.setJobDescription("Select data from table by version")
  spark.sql("SELECT * FROM delta.`/tmp/delta-table` VERSION AS OF 0;").show()

  scala.io.StdIn.readLine()
  spark.stop()
}
