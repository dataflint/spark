from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Sales Filterer") \
    .config("spark.ui.port", "11000") \
    .config("spark.eventLog.enabled", True) \
    .config("spark.sql.maxMetadataStringLength", "10000") \
    .master("local[1]") \
    .getOrCreate()

df = spark.read.load("/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/store_sales")

df_filtered = df.filter(df.ss_quantity > 1)

df_filtered.write \
    .mode("overwrite") \
    .partitionBy("ss_quantity") \
    .parquet("/tmp/store_sales")

# input("job ended")

spark.stop()