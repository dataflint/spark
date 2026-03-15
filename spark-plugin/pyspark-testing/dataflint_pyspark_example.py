#!/usr/bin/python3
"""
PySpark example with DataFlint instrumentation for all Python exec types:
  - mapInPandas         → DataFlintMapInPandasExec
  - mapInArrow          → DataFlintPythonMapInArrowExec
  - Window (SQL)        → DataFlintWindowExec
  - Window (pandas UDF) → DataFlintWindowInPandasExec
  - pandas_udf SCALAR   → DataFlintArrowEvalPythonExec
  - applyInPandas       → DataFlintFlatMapGroupsInPandasExec
  - cogroup             → DataFlintFlatMapCoGroupsInPandasExec

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
    import re
    m = re.search(r'[/_-](\d+)\.\d', spark_home)
    if m:
        spark_major_version = int(m.group(1))

# Select the appropriate plugin JAR based on Spark version
if spark_major_version == 4:
    _plugin_jar = _project_root / "pluginspark4" / "target" / "scala-2.13" / "dataflint-spark4_2.13-0.8.5.jar"
    _plugin_module = "pluginspark4"
else:
    _plugin_jar = _project_root / "pluginspark3" / "target" / "scala-2.12" / "spark_2.12-0.8.5.jar"
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
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.dataflint.telemetry.enabled", "false") \
    .config("spark.dataflint.instrument.spark.mapInPandas.enabled", "true") \
    .config("spark.dataflint.instrument.spark.mapInArrow.enabled", "true") \
    .config("spark.dataflint.instrument.spark.window.enabled", "true") \
    .config("spark.dataflint.instrument.spark.arrowEvalPython.enabled", "true") \
    .config("spark.dataflint.instrument.spark.flatMapGroupsInPandas.enabled", "true") \
    .config("spark.dataflint.instrument.spark.flatMapCoGroupsInPandas.enabled", "true") \
    .config("spark.dataflint.instrument.spark.batchEvalPython.enabled", "true") \
    .master("local[*]") \
    .getOrCreate()
# spark.sparkContext.setLogLevel("INFO")
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

spark.sparkContext.setJobDescription("mapInPandas: compute discounted totals → DataFlintMapInPandasExec")
df_pandas.write \
    .mode("overwrite") \
    .parquet("/tmp/dataflint_map_in_pandas_example")

print("\nResult written to /tmp/dataflint_map_in_pandas_example")


# Run mapInArrow (only if Spark version >= 3.3.0)
if supports_map_in_arrow:
    print("\n" + "="*80)
    print("Running mapInArrow example")
    print("="*80)

    df_arrow = df.mapInArrow(compute_discounted_totals_arrow, output_schema)

    spark.sparkContext.setJobDescription("mapInArrow: compute discounted totals → DataFlintPythonMapInArrowExec")
    df_arrow.write \
        .mode("overwrite") \
        .parquet("/tmp/dataflint_map_in_arrow_example")

    print("\nResult written to /tmp/dataflint_map_in_arrow_example")
else:
    print("\n" + "="*80)
    print("Skipping mapInArrow example")
    print("="*80)
    print(f"mapInArrow is only supported in Spark 3.3.0+")
    print(f"Current version: {spark_version}")


print("\n" + "="*80)
print("Running Window function example")
print("="*80)

from pyspark.sql import Window
from pyspark.sql.functions import rank, sum as spark_sum, avg
from pyspark.sql.types import DoubleType

window_by_category = Window.partitionBy("category").orderBy("price")
window_category_total = Window.partitionBy("category")

df_window = df.withColumn("rank_in_category", rank().over(window_by_category)) \
              .withColumn("cumulative_revenue", spark_sum("price").over(window_by_category)) \
              .withColumn("avg_price_in_category", avg("price").over(window_category_total))

spark.sparkContext.setJobDescription("Window SQL: rank + cumulative sum + avg → DataFlintWindowExec")
df_window.write \
    .mode("overwrite") \
    .parquet("/tmp/dataflint_window_example")

print("\nResult written to /tmp/dataflint_window_example")


print("\n" + "="*80)
print("Running Window function with Python UDF example")
print("="*80)

import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf(DoubleType())
def discounted_sum(prices: pd.Series) -> float:
    """Pandas UDF used as a window aggregate: sum of prices with a 10% discount."""
    import time
    time.sleep(2)
    return prices.sum() * 0.9

df_window_udf = df.withColumn(
    "discounted_category_revenue",
    discounted_sum("price").over(window_category_total)
)

spark.sparkContext.setJobDescription("Window pandas UDF: discounted sum → DataFlintWindowInPandasExec")
df_window_udf.write \
    .mode("overwrite") \
    .parquet("/tmp/dataflint_window_udf_example")

print("\nResult written to /tmp/dataflint_window_udf_example")


print("\n" + "="*80)
print("Running regular @udf example (BatchEvalPython)")
print("="*80)

# Regular Python @udf → DataFlintBatchEvalPythonExec
# Spark serializes rows to/from Python via pickle (no Arrow); the UDF runs in a Python subprocess
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(StringType())
def price_tier(price):
    """Classify price into tiers without using pandas/Arrow."""
    if price is None:
        return "unknown"
    if price < 20:
        return "budget"
    if price < 100:
        return "mid-range"
    return "premium"

df_batch_udf = df.withColumn("price_tier", price_tier("price"))

spark.sparkContext.setJobDescription("@udf: classify price tier → DataFlintBatchEvalPythonExec")
df_batch_udf.write \
    .mode("overwrite") \
    .parquet("/tmp/dataflint_batch_eval_python_example")

print("\nResult written to /tmp/dataflint_batch_eval_python_example")


print("\n" + "="*80)
print("Running pandas_udf SCALAR example (ArrowEvalPython)")
print("="*80)

# pandas_udf SCALAR → DataFlintArrowEvalPythonExec
# The UDF is applied column-wise; Spark executes it via ArrowEvalPythonExec
@pandas_udf(DoubleType())
def apply_discount(price: pd.Series, quantity: pd.Series) -> pd.Series:
    """Apply a 10% discount when quantity > 3, otherwise no discount."""
    return price * quantity.apply(lambda q: 0.90 if q > 3 else 1.0)

df_scalar_udf = df.withColumn(
    "discounted_price",
    apply_discount("price", "quantity")
)

spark.sparkContext.setJobDescription("pandas_udf SCALAR: apply discount → DataFlintArrowEvalPythonExec")
df_scalar_udf.write \
    .mode("overwrite") \
    .parquet("/tmp/dataflint_arrow_eval_python_example")

print("\nResult written to /tmp/dataflint_arrow_eval_python_example")


print("\n" + "="*80)
print("Running applyInPandas example (FlatMapGroupsInPandas)")
print("="*80)

# applyInPandas / GROUPED_MAP → DataFlintFlatMapGroupsInPandasExec
# Groups data by category, then applies a pandas function to each group
def enrich_group(pdf):
    """Compute per-category stats and add them as new columns."""
    pdf = pdf.copy()
    pdf["quantity"] = pdf["quantity"].fillna(0)
    pdf["cat_total_revenue"] = (pdf["quantity"] * pdf["price"]).sum()
    pdf["cat_avg_price"] = pdf["price"].mean()
    pdf["revenue"] = pdf["quantity"] * pdf["price"]
    return pdf[["customer", "category", "quantity", "price", "revenue",
                "cat_total_revenue", "cat_avg_price"]]

group_output_schema = (
    "customer string, category string, quantity int, price double, "
    "revenue double, cat_total_revenue double, cat_avg_price double"
)

df_grouped = df.groupby("category").applyInPandas(enrich_group, schema=group_output_schema)

spark.sparkContext.setJobDescription("applyInPandas: per-category stats → DataFlintFlatMapGroupsInPandasExec")
df_grouped.write \
    .mode("overwrite") \
    .parquet("/tmp/dataflint_flat_map_groups_in_pandas_example")

print("\nResult written to /tmp/dataflint_flat_map_groups_in_pandas_example")


print("\n" + "="*80)
print("Running cogroup applyInPandas example (FlatMapCoGroupsInPandas)")
print("="*80)

# cogroup / applyInPandas on two DataFrames → DataFlintFlatMapCoGroupsInPandasExec
# Join two DataFrames co-located on the same key and process each co-group together
discounts_data = [
    ("Electronics", 0.15),
    ("Books", 0.05),
    ("Clothing", 0.10),
]
discounts_schema = "category string, discount_rate double"
df_discounts = spark.createDataFrame(discounts_data, discounts_schema)

def apply_category_discount(left_pdf, right_pdf):
    """Apply per-category discount from the right DataFrame to the left."""
    import pandas as pd
    discount_rate = right_pdf["discount_rate"].iloc[0] if len(right_pdf) > 0 else 0.0
    left_pdf = left_pdf.copy()
    left_pdf["quantity"] = left_pdf["quantity"].fillna(0)
    left_pdf["final_price"] = left_pdf["price"] * (1.0 - discount_rate)
    left_pdf["discount_rate"] = discount_rate
    return left_pdf[["customer", "category", "quantity", "price",
                      "discount_rate", "final_price"]]

cogroup_output_schema = (
    "customer string, category string, quantity int, price double, "
    "discount_rate double, final_price double"
)

df_cogrouped = (
    df.groupby("category")
    .cogroup(df_discounts.groupby("category"))
    .applyInPandas(apply_category_discount, schema=cogroup_output_schema)
)

spark.sparkContext.setJobDescription("cogroup applyInPandas: apply category discounts → DataFlintFlatMapCoGroupsInPandasExec")
df_cogrouped.write \
    .mode("overwrite") \
    .parquet("/tmp/dataflint_flat_map_cogroups_in_pandas_example")

print("\nResult written to /tmp/dataflint_flat_map_cogroups_in_pandas_example")


print("\n" + "="*80)
print("Done!")
print("="*80)

input("\nPress Enter to exit...")
spark.stop()