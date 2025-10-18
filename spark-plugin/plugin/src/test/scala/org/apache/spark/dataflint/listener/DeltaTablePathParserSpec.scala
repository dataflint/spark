package org.apache.spark.dataflint.listener

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DeltaTablePathParserSpec extends AnyFunSuite with Matchers {

  test("extractTableNameFromDesc - should extract table name from FileScan description") {
    val desc = "FileScan parquet dataflint_user_simulator.test.zorder_table[category#850,value#851]..."
    val result = DeltaTablePathParser.extractTableNameFromDesc(desc)
    result shouldBe Some("dataflint_user_simulator.test.zorder_table")
  }

  test("extractTableNameFromDesc - should extract table name from complex Databricks plan") {
    val desc = "FileScan parquet dataflint_user_simulator.test.zorder_table[category#1126,value#1127,_databricks_internal_edge_computed_column_skip_row#1156] " +
      "Batched: true, DataFilters: [], Format: Parquet, Location: PreparedDeltaFileIndex(1 paths)" +
      "[s3://databricks-workspace-stack-c49b0-bucket/unity-catalog/1485094215217358/__unitystorage/catalogs/" +
      "22232196-5af9-4073-9f55-1aeccb701020/tables/88e720e8-f105-4e21-86f1-80331268c834], " +
      "PartitionFilters: [], PushedFilters: [], ReadSchema: struct<category:string,value:int,_databricks_internal_edge_computed_column_skip_row:boolean>"
    
    val result = DeltaTablePathParser.extractTableNameFromDesc(desc)
    result shouldBe Some("dataflint_user_simulator.test.zorder_table")
  }

  test("extractTableNameFromDesc - should extract table name with single component") {
    val desc = "FileScan parquet my_table[id#123,name#124]"
    val result = DeltaTablePathParser.extractTableNameFromDesc(desc)
    result shouldBe Some("my_table")
  }

  test("extractTableNameFromDesc - should extract table name with catalog and schema") {
    val desc = "Scan parquet my_catalog.my_schema.my_table[id#123]"
    val result = DeltaTablePathParser.extractTableNameFromDesc(desc)
    result shouldBe Some("my_catalog.my_schema.my_table")
  }

  test("extractTableNameFromDesc - should return None for non-matching pattern") {
    val desc = "SomeOtherNode without table name"
    val result = DeltaTablePathParser.extractTableNameFromDesc(desc)
    result shouldBe None
  }

  test("extractTableNameFromDesc - should return None for empty string") {
    val result = DeltaTablePathParser.extractTableNameFromDesc("")
    result shouldBe None
  }

  test("extractTablePathFromDesc - should extract S3 path from Databricks plan") {
    val desc = "FileScan parquet dataflint_user_simulator.test.zorder_table[category#1126,value#1127] " +
      "Location: PreparedDeltaFileIndex(1 paths)[s3://databricks-workspace-stack-c49b0-bucket/unity-catalog/" +
      "1485094215217358/__unitystorage/catalogs/22232196-5af9-4073-9f55-1aeccb701020/tables/88e720e8-f105-4e21-86f1-80331268c834]"
    
    val result = DeltaTablePathParser.extractTablePathFromDesc(desc)
    result shouldBe Some("s3://databricks-workspace-stack-c49b0-bucket/unity-catalog/1485094215217358/__unitystorage/catalogs/22232196-5af9-4073-9f55-1aeccb701020/tables/88e720e8-f105-4e21-86f1-80331268c834")
  }

  test("extractTablePathFromDesc - should extract file path from TahoeBatchFileIndex") {
    val desc = "TahoeBatchFileIndex(1 paths)[file:/tmp/my_table]"
    val result = DeltaTablePathParser.extractTablePathFromDesc(desc)
    result shouldBe Some("file:/tmp/my_table")
  }

  test("extractTablePathFromDesc - should extract dbfs path") {
    val desc = "TahoeBatchFileIndex(1 paths)[dbfs:/mnt/delta/my_table]"
    val result = DeltaTablePathParser.extractTablePathFromDesc(desc)
    result shouldBe Some("dbfs:/mnt/delta/my_table")
  }

  test("extractTablePathFromDesc - should return None for _delta_log scan") {
    val desc = "FileScan[file:/tmp/table/_delta_log/00000000000000000000.json]"
    val result = DeltaTablePathParser.extractTablePathFromDesc(desc)
    result shouldBe None
  }

  test("extractTablePathFromDesc - should return None for truncated path") {
    val desc = "TahoeBatchFileIndex[file:/tmp/very_long_path...]"
    val result = DeltaTablePathParser.extractTablePathFromDesc(desc)
    result shouldBe None
  }

  test("extractTablePathFromDesc - should return None when no path found") {
    val desc = "SomeNode without any path"
    val result = DeltaTablePathParser.extractTablePathFromDesc(desc)
    result shouldBe None
  }

  test("extractTableNameFromPath - should extract table name from S3 path") {
    val path = "s3://databricks-workspace-stack-c49b0-bucket/unity-catalog/1485094215217358/__unitystorage/catalogs/22232196-5af9-4073-9f55-1aeccb701020/tables/88e720e8-f105-4e21-86f1-80331268c834"
    val result = DeltaTablePathParser.extractTableNameFromPath(path)
    result shouldBe Some("88e720e8-f105-4e21-86f1-80331268c834")
  }

  test("extractTableNameFromPath - should extract table name from file path") {
    val path = "file:/tmp/my_table"
    val result = DeltaTablePathParser.extractTableNameFromPath(path)
    result shouldBe Some("my_table")
  }

  test("extractTableNameFromPath - should extract table name from local path") {
    val path = "/local/path/table_name"
    val result = DeltaTablePathParser.extractTableNameFromPath(path)
    result shouldBe Some("table_name")
  }

  test("extractTableNameFromPath - should return None for empty path") {
    val result = DeltaTablePathParser.extractTableNameFromPath("")
    result shouldBe None
  }

  test("extractTableNameFromPath - should handle path ending with slash") {
    val result = DeltaTablePathParser.extractTableNameFromPath("/path/to/table/")
    // Trailing slash is stripped, so we get "table"
    result shouldBe Some("table")
  }
  
  test("extractTableNameFromPath - should return None for only slashes") {
    val result = DeltaTablePathParser.extractTableNameFromPath("/")
    result shouldBe None
  }

  test("integration test - full Databricks plan should prefer qualified table name over UUID") {
    val nodeName = "Scan parquet dataflint_user_simulator.test.zorder_table"
    val planDescription = "FileScan parquet dataflint_user_simulator.test.zorder_table[category#1126,value#1127,_databricks_internal_edge_computed_column_skip_row#1156] " +
      "Batched: true, DataFilters: [], Format: Parquet, Location: PreparedDeltaFileIndex(1 paths)" +
      "[s3://databricks-workspace-stack-c49b0-bucket/unity-catalog/1485094215217358/__unitystorage/catalogs/" +
      "22232196-5af9-4073-9f55-1aeccb701020/tables/88e720e8-f105-4e21-86f1-80331268c834], " +
      "PartitionFilters: [], PushedFilters: [], ReadSchema: struct<category:string,value:int,_databricks_internal_edge_computed_column_skip_row:boolean>"
    
    // Extract both table name and path
    val tableName = DeltaTablePathParser.extractTableNameFromDesc(planDescription)
    val tablePath = DeltaTablePathParser.extractTablePathFromDesc(planDescription)
    val tableNameFromPath = tablePath.flatMap(DeltaTablePathParser.extractTableNameFromPath)
    
    // The qualified table name should be extracted
    tableName shouldBe Some("dataflint_user_simulator.test.zorder_table")
    
    // The path should be extracted
    tablePath shouldBe Some("s3://databricks-workspace-stack-c49b0-bucket/unity-catalog/1485094215217358/__unitystorage/catalogs/22232196-5af9-4073-9f55-1aeccb701020/tables/88e720e8-f105-4e21-86f1-80331268c834")
    
    // The UUID would be extracted from path as fallback
    tableNameFromPath shouldBe Some("88e720e8-f105-4e21-86f1-80331268c834")
    
    // The qualified table name should be preferred over the UUID
    tableName should not be tableNameFromPath
  }

  test("integration test - should use qualified table name in event, not UUID from path") {
    val planDescription = "FileScan parquet dataflint_user_simulator.test.zorder_table[category#1126,value#1127] " +
      "Location: PreparedDeltaFileIndex(1 paths)" +
      "[s3://databricks-workspace-stack-c49b0-bucket/unity-catalog/__unitystorage/tables/19d5195d-2fbc-495c-be52-a32b0b49a66a]"
    
    val tablePath = DeltaTablePathParser.extractTablePathFromDesc(planDescription)
    val tableNameOrPath = DeltaTablePathParser.extractTableNameFromDesc(planDescription).getOrElse(tablePath.get)
    
    // The tableNameOrPath should be the qualified name, not the path
    tableNameOrPath shouldBe "dataflint_user_simulator.test.zorder_table"
    
    // This is what should be used in the event's tableName field
    // NOT DeltaTablePathParser.extractTableNameFromPath(tablePath.get)
    tableNameOrPath should not be "19d5195d-2fbc-495c-be52-a32b0b49a66a"
  }
}

