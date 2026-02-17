#!/usr/bin/python3
"""
PySpark mapInPandas and mapInArrow example with DataFlint instrumentation.

This script demonstrates both mapInPandas and mapInArrow operations with DataFlint plugin enabled.

Usage:
    pip install pyspark pandas pyarrow
    python dataflint_pyspark_example.py
"""
from pathlib import Path
import pyarrow as pa
import pyarrow.compute as pc
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Resolve the plugin JAR path relative to this script's location
# Detect Spark version from environment to load the correct JAR
_script_dir = Path(__file__).resolve().parent
_project_root = _script_dir.parent

# Try to detect Spark version from SPARK_HOME
import os
spark_home = os.environ.get('SPARK_HOME', '')
spark_major_version = 3  # default to Spark 3

if spark_home:
    # Try to extract version from SPARK_HOME path
    if '4.0' in spark_home or 'spark-4' in spark_home:
        spark_major_version = 4
    elif '3.' in spark_home or 'spark-3' in spark_home:
        spark_major_version = 3

# Select the appropriate plugin JAR based on Spark version
if spark_major_version == 4:
    _plugin_jar = _project_root / "pluginspark4" / "target" / "scala-2.13" / "dataflint-spark4_2.13-0.8.4.jar"
    _plugin_module = "pluginspark4"
else:
    _plugin_jar = _project_root / "pluginspark3" / "target" / "scala-2.12" / "spark_2.12-0.8.4.jar"
    _plugin_module = "pluginspark3"

if not _plugin_jar.exists():
    raise FileNotFoundError(
        f"Plugin JAR not found at {_plugin_jar}\n"
        f"Run: cd {_project_root} && sbt {_plugin_module}/assembly"
    )

spark = SparkSession \
    .builder \
    .appName("DataFlint Pyspark Example") \
    .config("spark.jars", str(_plugin_jar)) \
    .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin") \
    .config("spark.ui.port", "10000") \
    .config("spark.sql.maxMetadataStringLength", "10000") \
    .config("spark.dataflint.telemetry.enabled", "false") \
    .config("spark.dataflint.instrument.spark.mapInPandas.enabled", "true") \
    .config("spark.dataflint.instrument.spark.mapInArrow.enabled", "true") \
    .master("local[*]") \
    .getOrCreate()

# Get Spark version and check if mapInArrow is supported
spark_version = spark.version
version_parts = spark_version.split('.')
major = int(version_parts[0])
minor = int(version_parts[1])
supports_map_in_arrow = (major > 3) or (major == 3 and minor >= 3)

print(f"\nSpark version: {spark_version}")
print(f"mapInArrow supported: {supports_map_in_arrow}\n")

# Sample data
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


# mapInPandas function
def compute_discounted_totals_pandas(iterator):
    for pdf in iterator:
        pdf = pdf.copy()
        pdf["quantity"] = pdf["quantity"].fillna(0)
        pdf["total_cost"] = pdf["quantity"] * pdf["price"]
        pdf["discount"] = pdf["quantity"].apply(lambda q: 0.10 if q > 3 else 0.0)
        pdf["final_cost"] = pdf["total_cost"] * (1.0 - pdf["discount"])
        yield pdf[["customer", "category", "quantity", "price", "total_cost", "final_cost"]]


# mapInArrow function
def compute_discounted_totals_arrow(iterator):
    for batch in iterator:
        quantity = batch.column("quantity")
        price = batch.column("price")

        # Fill nulls with 0 (must use int32 scalar to preserve the IntegerType schema)
        quantity_filled = pc.if_else(pc.is_null(quantity), pa.scalar(0, type=pa.int32()), quantity)

        # Compute total_cost = quantity * price
        total_cost = pc.multiply(pc.cast(quantity_filled, pa.float64()), price)

        # Compute discount: 10% if quantity > 3, else 0%
        discount = pc.if_else(
            pc.greater(quantity_filled, pa.scalar(3, type=pa.int32())),
            pa.scalar(0.10, type=pa.float64()),
            pa.scalar(0.0, type=pa.float64()),
        )

        # Compute final_cost = total_cost * (1 - discount)
        final_cost = pc.multiply(total_cost, pc.subtract(pa.scalar(1.0, type=pa.float64()), discount))

        result = pa.RecordBatch.from_arrays(
            [
                batch.column("customer"),
                batch.column("category"),
                pc.cast(quantity_filled, pa.int32()),
                price,
                total_cost,
                final_cost,
            ],
            names=["customer", "category", "quantity", "price", "total_cost", "final_cost"],
        )
        yield result


output_schema = "customer string, category string, quantity int, price double, total_cost double, final_cost double"


# Run mapInPandas
print("="*80)
print("Running mapInPandas example")
print("="*80)

df_pandas = df.mapInPandas(compute_discounted_totals_pandas, output_schema)

df_pandas.write \
    .mode("overwrite") \
    .parquet("/tmp/dataflint_map_in_pandas_example")

print("\nResult written to /tmp/dataflint_map_in_pandas_example")
print("\nSample output:")
df_pandas.show(10, truncate=False)


# Run mapInArrow (only if Spark version >= 3.3.0)
if supports_map_in_arrow:
    print("\n" + "="*80)
    print("Running mapInArrow example")
    print("="*80)

    df_arrow = df.mapInArrow(compute_discounted_totals_arrow, output_schema)

    df_arrow.write \
        .mode("overwrite") \
        .parquet("/tmp/dataflint_map_in_arrow_example")

    print("\nResult written to /tmp/dataflint_map_in_arrow_example")
    print("\nSample output:")
    df_arrow.show(10, truncate=False)
else:
    print("\n" + "="*80)
    print("Skipping mapInArrow example")
    print("="*80)
    print(f"mapInArrow is only supported in Spark 3.3.0+")
    print(f"Current version: {spark_version}")


print("\n" + "="*80)
print("Done!")
print("="*80)

input("\nPress Enter to exit...")
spark.stop()
