package io.dataflint.example

import org.apache.spark.sql.SparkSession

import java.sql.Timestamp

object DeltaLakeLiquidClusteringExample extends App {
  val spark = SparkSession
    .builder()
    .appName("DeltaLakeLiquidClusteringExample")
    .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin")
    .config("spark.dataflint.telemetry.enabled", false)
    .config("spark.dataflint.instrument.deltalake.enabled", true)
    // Optional: Configure z-index field collection and caching behavior
     .config("spark.dataflint.instrument.deltalake.collectZindexFields", true) // default: true
    // .config("spark.dataflint.instrument.deltalake.cacheZindexFieldsToProperties", true) // default: true
    .config("spark.ui.port", "10000")
    .config("spark.sql.maxMetadataStringLength", "10000")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  import org.apache.spark.sql.functions._
  import io.delta.tables._
  import org.apache.hadoop.fs.Path

  // === Hard Reset: Delete all test table directories ===
  println("=== Hard Reset: Cleaning up test table directories ===")
  val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
  val testPaths = Seq("/tmp/zorder_table", "/tmp/partitioned_table", "/tmp/liquid_cluster_table")
  testPaths.foreach { pathStr =>
    val path = new Path(pathStr)
    if (fs.exists(path)) {
      fs.delete(path, true)
      println(s"Deleted existing directory: $pathStr")
    }
  }
  
  // Drop tables if they exist
  spark.sql(s"DROP TABLE IF EXISTS zorder_table")
  spark.sql(s"DROP TABLE IF EXISTS partitioned_table")
  spark.sql(s"DROP TABLE IF EXISTS liquid_cluster_table")
  println("Hard reset completed - all tables and data cleared\n")

  // Create dummy data
  spark.sparkContext.setJobDescription("Creating dummy data for Delta table")
  val data = (1 to 100000).map { i =>
    val category = if (i % 5 == 0) "A" else if (i % 3 == 0) "B" else "C"
    (i, category, i % 100)
  }.toDF("id", "category", "value")

  // Write data to Delta table
  spark.sparkContext.setJobDescription("Writing data to Delta table")
  val path = "/tmp/zorder_table"
  data.write
    .format("delta")
    .mode("overwrite")
    .save(path)

  // Register as table if needed
  spark.sparkContext.setJobDescription("Registering Delta table in Spark SQL")
  spark.sql(s"CREATE TABLE zorder_table USING DELTA LOCATION '$path'")

  // === ✅ Native Delta Lake Z-Ordering ===
  spark.sparkContext.setJobDescription("Running Z-Ordering optimization on Delta table")
  spark.sql("""
  OPTIMIZE zorder_table
  ZORDER BY (category)
""")
  println("Z-Order optimization completed - Dataflint will detect and cache z-order fields")

  // Query 1: First query after OPTIMIZE - should detect z-order and cache it
  spark.sparkContext.setJobDescription("Query 1: First query after OPTIMIZE - detecting z-order")
  val dfAll = spark.read.format("delta").load(path)
  val resultAll = dfAll.groupBy("category").agg(sum("value").as("total_value"), count("*").as("count")).collect()
  println(s"Query 1 (detects z-order): Total aggregation results: ${resultAll.mkString(", ")}")

  // Query 2: Second query - should use cached z-order from metadata (no history scan)
  spark.sparkContext.setJobDescription("Query 2: Using cached z-order metadata (fast path)")
  val dfFiltered = spark.read.format("delta").load(path)
    .filter($"category" === "A")
  val resultFiltered = dfFiltered.agg(sum("value").as("total_value"), count("*").as("count")).collect()
  println(s"Query 2 (uses cache): Filtered aggregation (category = 'A'): ${resultFiltered.mkString(", ")}")

  // === ✅ Partitioned Table Example ===
  spark.sparkContext.setJobDescription("Creating partitioned Delta table")
  val partitionPath = "/tmp/partitioned_table"
  
  // Create larger dataset for partitioning
  val partitionData = (1 to 100000).map { i =>
    val region = if (i % 4 == 0) "North" else if (i % 4 == 1) "South" else if (i % 4 == 2) "East" else "West"
    val status = if (i % 2 == 0) "Active" else "Inactive"
    (i, region, status, i * 10)
  }.toDF("id", "region", "status", "amount")

  // Write partitioned table
  spark.sparkContext.setJobDescription("Writing partitioned Delta table")
  partitionData.write
    .format("delta")
    .partitionBy("region", "status")
    .mode("overwrite")
    .save(partitionPath)

  // Register partitioned table
  spark.sparkContext.setJobDescription("Registering partitioned Delta table")
  spark.sql(s"CREATE TABLE partitioned_table USING DELTA LOCATION '$partitionPath'")

  // Read without partition filter
  spark.sparkContext.setJobDescription("Reading partitioned table without partition filter")
  val dfPartitionedAll = spark.read.format("delta").load(partitionPath)
  val resultPartitionedAll = dfPartitionedAll.groupBy("region", "status").agg(sum("amount").as("total_amount"), count("*").as("count")).collect()
  println(s"Partitioned table - All regions aggregation: ${resultPartitionedAll.length} groups, sample: ${resultPartitionedAll.take(3).mkString(", ")}")

  // Read with partition filter (should use partition pruning)
  spark.sparkContext.setJobDescription("Reading partitioned table with partition filter")
  val dfPartitionedFiltered = spark.read.format("delta").load(partitionPath)
    .filter($"region" === "North" && $"status" === "Active")
  val resultPartitionedFiltered = dfPartitionedFiltered.agg(sum("amount").as("total_amount"), count("*").as("count")).collect()
  println(s"Partitioned table - Filtered (region = 'North' AND status = 'Active'): ${resultPartitionedFiltered.mkString(", ")}")

  // === ✅ Liquid Clustering Example ===
  spark.sparkContext.setJobDescription("Creating liquid clustered Delta table")
  val liquidClusterPath = "/tmp/liquid_cluster_table"
  
  // Create dataset for liquid clustering
  val liquidClusterData = (1 to 100000).map { i =>
    val department = if (i % 5 == 0) "Engineering" else if (i % 5 == 1) "Sales" else if (i % 5 == 2) "Marketing" else if (i % 5 == 3) "HR" else "Finance"
    val city = if (i % 3 == 0) "NYC" else if (i % 3 == 1) "SF" else "LA"
    (i, department, city, i * 100)
  }.toDF("id", "department", "city", "salary")

  // Create table with liquid clustering (Spark 3.5+ / Delta 3.0+ feature)
  spark.sparkContext.setJobDescription("Creating liquid clustered Delta table with CLUSTER BY")
  spark.sql(s"""
    CREATE TABLE liquid_cluster_table (
      id INT,
      department STRING,
      city STRING,
      salary INT
    )
    USING DELTA
    LOCATION '$liquidClusterPath'
    CLUSTER BY (department, city)
  """)
  
  // Write data to the clustered table
  spark.sparkContext.setJobDescription("Writing data to liquid clustered Delta table")
  liquidClusterData.write
    .format("delta")
    .mode("append")
    .save(liquidClusterPath)

  // Optimize to apply clustering
  spark.sparkContext.setJobDescription("Optimizing liquid clustered table")
  spark.sql("OPTIMIZE liquid_cluster_table")

  // Read without clustering filter
  spark.sparkContext.setJobDescription("Reading liquid clustered table without filter")
  val dfLiquidAll = spark.read.format("delta").load(liquidClusterPath)
  val resultLiquidAll = dfLiquidAll.groupBy("department", "city").agg(sum("salary").as("total_salary"), count("*").as("count")).collect()
  println(s"Liquid clustered table - All departments aggregation: ${resultLiquidAll.length} groups, sample: ${resultLiquidAll.take(3).mkString(", ")}")

  // Read with clustering filter (should benefit from clustering)
  spark.sparkContext.setJobDescription("Reading liquid clustered table with clustering filter")
  val dfLiquidFiltered = spark.read.format("delta").load(liquidClusterPath)
    .filter($"department" === "Engineering" && $"city" === "NYC")
  val resultLiquidFiltered = dfLiquidFiltered.agg(sum("salary").as("total_salary"), count("*").as("count")).collect()
  println(s"Liquid clustered table - Filtered (department = 'Engineering' AND city = 'NYC'): ${resultLiquidFiltered.mkString(", ")}")

  // === ✅ Changing Cluster Keys Example ===
  println("\n=== Changing Cluster Keys Scenario ===")
  
  // Query with old clustering keys (department, city)
  spark.sparkContext.setJobDescription("Query 1: Using old cluster keys (department, city)")
  val dfOldClustering1 = spark.read.format("delta").load(liquidClusterPath)
    .filter($"department" === "Sales" && $"city" === "SF")
  val resultOldClustering1 = dfOldClustering1.agg(sum("salary").as("total_salary"), count("*").as("count")).collect()
  println(s"Query with old clustering (department='Sales', city='SF'): ${resultOldClustering1.mkString(", ")}")

  // Change clustering keys to optimize for different access patterns
  // Now we only cluster by city (removing department from clustering)
  // Note: ALTER TABLE CLUSTER BY is only supported for tables that already have clustering
  spark.sparkContext.setJobDescription("Altering table to use new cluster keys (city only)")
  spark.sql("""
    ALTER TABLE liquid_cluster_table
    CLUSTER BY (city)""")
  println("Changed clustering keys from (department, city) to (city)")

  // Optimize table with new clustering keys
  spark.sparkContext.setJobDescription("Optimizing table with new cluster keys")
  spark.sql("OPTIMIZE liquid_cluster_table")
  println("Table optimized with new clustering keys")

  // Query with new clustering keys - filters by city (which is in the new cluster keys)
  spark.sparkContext.setJobDescription("Query 2: Filter by city (in new cluster keys)")
  val dfNewClustering1 = spark.read.format("delta").load(liquidClusterPath)
    .filter($"city" === "SF")
  val resultNewClustering1 = dfNewClustering1.groupBy("department").agg(sum("salary").as("total_salary"), count("*").as("count")).collect()
  println(s"Query filtering by city='SF' (in new cluster keys): ${resultNewClustering1.length} departments, sample: ${resultNewClustering1.take(2).mkString(", ")}")

  // Query that filters by department (which was in OLD cluster keys but NOT in new cluster keys)
  // This tests that we correctly show the metadata with the new cluster keys
  spark.sparkContext.setJobDescription("Query 3: Filter by department (NOT in new cluster keys, WAS in old cluster keys)")
  val dfNewClustering2 = spark.read.format("delta").load(liquidClusterPath)
    .filter($"department" === "Engineering")
  val resultNewClustering2 = dfNewClustering2.groupBy("city").agg(sum("salary").as("total_salary"), count("*").as("count")).collect()
  println(s"Query filtering by department='Engineering' (NOT in new cluster keys): ${resultNewClustering2.length} cities, result: ${resultNewClustering2.mkString(", ")}")
  println("  ^ This query should show cluster keys as (city) NOT (department, city)")

  // Another query with city filter to benefit from new clustering
  spark.sparkContext.setJobDescription("Query 4: Multi-city filter with new clustering")
  val dfNewClustering3 = spark.read.format("delta").load(liquidClusterPath)
    .filter($"city".isin("NYC", "LA"))
  val resultNewClustering3 = dfNewClustering3.agg(sum("salary").as("total_salary"), count("*").as("count")).collect()
  println(s"Query with new clustering (city in ['NYC', 'LA']): ${resultNewClustering3.mkString(", ")}")

  // Query 3: Third query with different filter - also uses cached z-order
  spark.sparkContext.setJobDescription("Query 5: Another query using cached z-order metadata")
  val dfFilteredB = spark.read.format("delta").load(path)
    .filter($"category" === "B")
  val resultFilteredB = dfFilteredB.agg(sum("value").as("total_value"), count("*").as("count")).collect()
  println(s"Query 3 (uses cache): Filtered aggregation (category = 'B'): ${resultFilteredB.mkString(", ")}")

  println("\n=== All Delta Lake examples completed ===")
  scala.io.StdIn.readLine()
  spark.stop()
}