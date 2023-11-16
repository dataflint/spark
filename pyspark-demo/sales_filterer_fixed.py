#!/usr/bin/python3
from pyspark.sql import SparkSession
import os

spark = SparkSession \
    .builder \
    .appName("Sales Filterer Fixed") \
    .config("spark.jars.packages", "io.dataflint:spark_2.12:0.0.4-SNAPSHOT") \
    .config("spark.jars.repositories", "https://s01.oss.sonatype.org/content/repositories/snapshots") \
    .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin") \
    .config("spark.ui.port", "11000") \
    .config("spark.eventLog.enabled", True) \
    .config("spark.sql.maxMetadataStringLength", "10000") \
    .master("local[1]") \
    .getOrCreate()

df = spark.read.load(os.getenv('SALES_FILES_LOCATION'))

df_filtered = df.filter(df.ss_quantity > 1).repartition("ss_quantity") # CHANGE

df_filtered.write \
    .mode("overwrite") \
    .partitionBy("ss_quantity") \
    .parquet("/tmp/store_sales")

input("sales filterer ended, press any key to continue..")
spark.stop()