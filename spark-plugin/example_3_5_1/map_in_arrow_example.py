#!/usr/bin/python3
"""
PySpark mapInArrow example with DataFlint instrumentation.

mapInArrow uses PyArrow record batches instead of Pandas DataFrames,
which avoids the Pandas serialization overhead and is typically faster.

Usage:
    pip install pyspark pandas pyarrow
    python map_in_arrow_example.py
"""
from pathlib import Path
import pyarrow as pa
import pyarrow.compute as pc
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Resolve the plugin JAR path relative to this script's location
_script_dir = Path(__file__).resolve().parent
_project_root = _script_dir.parent  # up to spark-plugin/
_plugin_jar = _project_root / "pluginspark3" / "target" / "scala-2.12" / "spark_2.12-0.8.4-SNAPSHOT.jar"

if not _plugin_jar.exists():
    raise FileNotFoundError(
        f"Plugin JAR not found at {_plugin_jar}\n"
        f"Run: cd {_project_root} && sbt pluginspark3/assembly"
    )

spark = SparkSession \
    .builder \
    .appName("MapInArrow Example") \
    .config("spark.jars", str(_plugin_jar)) \
    .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin") \
    .config("spark.ui.port", "10000") \
    .config("spark.sql.maxMetadataStringLength", "10000") \
    .config("spark.dataflint.telemetry.enabled", "false") \
    .config("spark.dataflint.instrument.spark.mapInArrow.enabled", "true") \
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

df_result = df.mapInArrow(compute_discounted_totals_arrow, output_schema)

df_result.write \
    .mode("overwrite") \
    .parquet("/tmp/dataflint_map_in_arrow_example")

df_result.show(20, truncate=False)

input("\nPress Enter to exit...")
spark.stop()
