#!/usr/bin/python3
"""
PySpark mapInPandas example with DataFlint instrumentation.

Usage:
    pip install pyspark pandas pyarrow
    python map_in_pandas_example.py
"""
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Resolve the plugin JAR path relative to this script's location
_script_dir = Path(__file__).resolve().parent
_project_root = _script_dir.parent  # up to spark-plugin/
_plugin_jar = _project_root / "pluginspark3" / "target" / "scala-2.12" / "spark_2.12-0.8.3-SNAPSHOT.jar"

if not _plugin_jar.exists():
    raise FileNotFoundError(
        f"Plugin JAR not found at {_plugin_jar}\n"
        f"Run: cd {_project_root} && sbt pluginspark3/assembly"
    )

spark = SparkSession \
    .builder \
    .appName("MapInPandas Example") \
    .config("spark.jars", str(_plugin_jar)) \
    .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin") \
    .config("spark.ui.port", "10000") \
    .config("spark.sql.maxMetadataStringLength", "10000") \
    .config("spark.dataflint.telemetry.enabled", "false") \
    .config("spark.dataflint.instrument.spark.mapInPandas.enabled", "true") \
    .master("local[*]") \
    .getOrCreate()

data = [
    ("Alice", "Electronics", 5, 299.99),
    ("Bob", "Books", 1, 12.50),
    ("Charlie", "Electronics", 3, 149.99),
    ("Alice", "Books", 2, 24.99),
    ("Bob", "Clothing", None, 45.00),
    ("Charlie", "Electronics", 10, 9.99),
    ("Alice", "Clothing", 1, 89.99),
    ("Bob", "Electronics", 4, 199.99),
    ("Charlie", "Books", None, 15.00),
    ("Alice", "Electronics", 7, 499.99),
] * 1000

schema = StructType([
    StructField("customer", StringType(), False),
    StructField("category", StringType(), False),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), False),
])

df = spark.createDataFrame(data, schema).repartition(4)


def compute_discounted_totals(iterator):
    for pdf in iterator:
        pdf = pdf.copy()
        pdf["quantity"] = pdf["quantity"].fillna(0)
        pdf["total_cost"] = pdf["quantity"] * pdf["price"]
        pdf["discount"] = pdf["quantity"].apply(lambda q: 0.10 if q > 3 else 0.0)
        pdf["final_cost"] = pdf["total_cost"] * (1.0 - pdf["discount"])
        yield pdf[["customer", "category", "quantity", "price", "total_cost", "final_cost"]]


output_schema = "customer string, category string, quantity int, price double, total_cost double, final_cost double"

df_result = df.mapInPandas(compute_discounted_totals, output_schema)

df_result.write \
    .mode("overwrite") \
    .parquet("/tmp/dataflint_map_in_pandas_example")

df_result.show(20, truncate=False)

input("\nPress Enter to exit...")
spark.stop()
