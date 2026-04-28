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
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import time

SLEEP_ENABLED = False

def sleep(seconds):
    if SLEEP_ENABLED:
        time.sleep(seconds)

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
    _plugin_jar = _project_root / "pluginspark4" / "target" / "scala-2.13" / "dataflint-spark4_2.13-0.9.5.jar"
    _plugin_module = "pluginspark4"
else:
    _plugin_jar = _project_root / "pluginspark3" / "target" / "scala-2.12" / "spark_2.12-0.9.5.jar"
    _plugin_module = "pluginspark3"

if not _plugin_jar.exists():
    raise FileNotFoundError(
        f"Plugin JAR not found at {_plugin_jar}\n"
        f"Run: cd {_project_root} && sbt {_plugin_module}/assembly"
    )
instrument = "true"
spark = SparkSession \
    .builder \
    .appName("DataFlint Pyspark Example") \
    .config("spark.jars", str(_plugin_jar)) \
    .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin") \
    .config("spark.ui.port", "10000") \
    .config("spark.sql.maxMetadataStringLength", "10000") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.dataflint.telemetry.enabled", "false") \
    .config("spark.dataflint.instrument.spark.mapInPandas.enabled", instrument) \
    .config("spark.dataflint.instrument.spark.mapInArrow.enabled", instrument) \
    .config("spark.dataflint.instrument.spark.window.enabled", instrument) \
    .config("spark.dataflint.instrument.spark.arrowEvalPython.enabled", instrument) \
    .config("spark.dataflint.instrument.spark.batchEvalPython.enabled", instrument) \
    .config("spark.dataflint.instrument.spark.flatMapGroupsInPandas.enabled", instrument) \
    .config("spark.dataflint.instrument.spark.flatMapCoGroupsInPandas.enabled", instrument) \
    .config("spark.dataflint.instrument.spark.sqlNodes.enabled", instrument) \
    .master("local[*]") \
    .getOrCreate()
# .config("spark.dataflint.test.codegenSleepMs", "1") \
#     .config("spark.sql.codegen.wholeStage", "false") \
# spark.sparkContext.setLogLevel("INFO")

# Register the SlowSumAggregator (Scala UDAF) via py4j after the SparkSession exists
_jvm = spark._jvm
_aggregator = _jvm.org.apache.spark.dataflint.SlowSumAggregator()
_input_encoder = _jvm.org.apache.spark.sql.Encoders.scalaDouble()
spark._jsparkSession.udf().register(
    "slow_sum",
    _jvm.org.apache.spark.sql.functions.udaf(_aggregator, _input_encoder)
)
print("Registered slow_sum UDAF")
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
] * 10000

schema = StructType([
    StructField("customer", StringType(), False),
    StructField("category", StringType(), False),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), False),
])

df = (spark.createDataFrame(data, schema))
      #.repartition(4))


# mapInPandas function
def compute_discounted_totals_pandas(iterator):
    for pdf in iterator:
        sleep(1)
        pdf = pdf.copy()
        pdf["quantity"] = pdf["quantity"].fillna(0)
        pdf["total_cost"] = pdf["quantity"] * pdf["price"]
        pdf["discount"] = pdf["quantity"].apply(lambda q: 0.10 if q > 3 else 0.0)
        pdf["final_cost"] = pdf["total_cost"] * (1.0 - pdf["discount"])
        yield pdf[["customer", "category", "quantity", "price", "total_cost", "final_cost"]]


# mapInArrow function
def compute_discounted_totals_arrow(iterator):
    for batch in iterator:
        sleep(20)
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

df_pandas = df.mapInPandas(compute_discounted_totals_pandas, output_schema) \
    .withColumn("final_cost", col("final_cost") * 2) \
    .filter("final_cost>10")

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

    df_arrow = df.mapInArrow(compute_discounted_totals_arrow, output_schema) \
        .withColumn("final_cost", col("final_cost") * 2)

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
from pyspark.sql.functions import rank, sum as spark_sum, avg, expr
from pyspark.sql.types import DoubleType

window_by_category = Window.partitionBy("category").orderBy("price")
window_category_total = Window.partitionBy("category")

# slow_sum is a Scala UDAF registered by DataFlintInstrumentationExtension.
# It sums Doubles but sleeps `spark.dataflint.test.slowSumSleepMs` per row on the JVM.
df_window = df.limit(1000).withColumn("rank_in_category", rank().over(window_by_category)) \
              .withColumn("cumulative_slow_revenue", expr("slow_sum(price)").over(window_by_category)) \
              .withColumn("avg_price_in_category", expr("slow_sum(price)").over(window_category_total))

spark.sparkContext.setJobDescription("Window SQL: rank + slow_sum (Scala UDAF) + avg → DataFlintWindowExec")
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
    sleep(0.0002)
    return prices.sum() * 0.9

df_window_udf = (df.withColumn("discounted_category_revenue", discounted_sum("price").over(window_by_category))
                 .withColumn("cumulative_revenue", discounted_sum("price").over(window_category_total))
                 .withColumn("price2", col("price")*2)
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
    sleep(0.0001)
    """Classify price into tiers without using pandas/Arrow."""
    if price is None:
        return "unknown"
    if price < 20:
        return "budget"
    if price < 100:
        return "mid-range"
    return "premium"

@udf(StringType())
def price_tier2(price):
    sleep(0.0001)
    """Classify price into tiers without using pandas/Arrow."""
    if price is None:
        return "unknown"
    if price < 50:
        return "budget"
    if price < 200:
        return "mid-range"
    return "premium"

df_batch_udf = df.withColumn("price_tier", price_tier("price")) \
    .withColumn("price_tier2", price_tier2("price"))

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
    sleep(0.1)
    """Apply a 10% discount when quantity > 3, otherwise no discount."""
    return price * quantity.apply(lambda q: 0.90 if q > 3 else 1.0)
@pandas_udf(DoubleType())
def apply_discount2(price: pd.Series, quantity: pd.Series) -> pd.Series:
    sleep(0.1)
    """Apply a 10% discount when quantity > 3, otherwise no discount."""
    return price * quantity.apply(lambda q: 0.80 if q > 3 else 1.0)

df_scalar_udf = df.withColumn(
    "discounted_price",
    apply_discount("price", "quantity")
).withColumn(
    "discounted_price2",
    apply_discount2("price", "quantity")
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
    sleep(5)
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

df_grouped = df.groupby("category").applyInPandas(enrich_group, schema=group_output_schema) \
    .withColumn("price", col("price")*2)


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
    sleep(5)
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
    .withColumn("price", col("price")*2)
)

spark.sparkContext.setJobDescription("cogroup applyInPandas: apply category discounts → DataFlintFlatMapCoGroupsInPandasExec")
df_cogrouped.write \
    .mode("overwrite") \
    .parquet("/tmp/dataflint_flat_map_cogroups_in_pandas_example")

print("\nResult written to /tmp/dataflint_flat_map_cogroups_in_pandas_example")


from pyspark.sql.functions import explode, array, collect_list, row_number, lit

# ── Python UDF over Parquet (columnar scan + shuffle) ────────────────────────
# Reproduces ColumnarBatch→InternalRow ClassCastException on Spark 3.0/3.1 when
# the transparent wrapper prevents ColumnarToRowExec insertion.
print("="*80)
print("Running Python UDF over Parquet source (columnar scan + shuffle)")
print("="*80)
spark.sparkContext.setJobDescription("Python UDF over Parquet: columnar scan + ArrowEvalPython + shuffle")
df.write.mode("overwrite").parquet("/tmp/dataflint_columnar_test_input")
df_parquet = spark.read.parquet("/tmp/dataflint_columnar_test_input")

@pandas_udf(DoubleType())
def double_price(price: pd.Series) -> pd.Series:
    return price * 2.0

df_parquet.withColumn("doubled_price", double_price("price")) \
    .groupBy("category") \
    .agg(spark_sum("doubled_price").alias("total")) \
    .write.mode("overwrite").parquet("/tmp/dataflint_columnar_udf_shuffle_example")
print("\nResult written to /tmp/dataflint_columnar_udf_shuffle_example")

# ── FilterExec + ProjectExec ──────────────────────────────────────────────────
print("="*80)
print("Running FilterExec + ProjectExec example")
print("="*80)
spark.sparkContext.setJobDescription("FilterExec + ProjectExec")
df.filter(col("price") > 200) \
  .select("customer", "category", "price") \
  .write.mode("overwrite").parquet("/tmp/dataflint_filter_project_example")
print("\nResult written to /tmp/dataflint_filter_project_example")

# ── ExpandExec (ROLLUP) ───────────────────────────────────────────────────────
print("="*80)
print("Running ExpandExec example (ROLLUP)")
print("="*80)
spark.sparkContext.setJobDescription("ExpandExec: rollup aggregation")
df.rollup("category", "customer") \
  .agg(spark_sum("price").alias("total_price")) \
  .filter(col("category").isNotNull()) \
  .write.mode("overwrite").parquet("/tmp/dataflint_expand_example")
print("\nResult written to /tmp/dataflint_expand_example")

# ── GenerateExec (explode) ────────────────────────────────────────────────────
print("="*80)
print("Running GenerateExec example (explode)")
print("="*80)
spark.sparkContext.setJobDescription("GenerateExec: explode array")
df.select("customer", "category",
          explode(array(col("price"), col("price") * 1.1)).alias("price_variant")) \
  .write.mode("overwrite").parquet("/tmp/dataflint_generate_example")
print("\nResult written to /tmp/dataflint_generate_example")

# ── BroadcastHashJoinExec ─────────────────────────────────────────────────────
print("="*80)
print("Running BroadcastHashJoinExec example")
print("="*80)
category_tiers = spark.createDataFrame([
    ("Electronics", "premium"), ("Books", "standard"),
    ("Clothing", "standard"), ("Sports", "premium"), ("Toys", "budget"),
], "category string, tier string")
spark.sparkContext.setJobDescription("BroadcastHashJoinExec: join with small broadcast table")
df.join(category_tiers, "category") \
  .write.mode("overwrite").parquet("/tmp/dataflint_broadcast_hash_join_example")
print("\nResult written to /tmp/dataflint_broadcast_hash_join_example")

# # ── SortMergeJoinExec ─────────────────────────────────────────────────────────
# print("="*80)
# print("Running SortMergeJoinExec example")
# print("="*80)
# spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
# spark.sparkContext.setJobDescription("SortMergeJoinExec: equi-join with broadcast disabled")
# df.alias("a").join(df.alias("b"), col("a.category") == col("b.category")) \
#   .select(col("a.customer").alias("c1"), col("a.price").alias("p1"),
#           col("b.customer").alias("c2"), col("b.price").alias("p2")) \
#   .write.mode("overwrite").parquet("/tmp/dataflint_sort_merge_join_example")
# spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
# print("\nResult written to /tmp/dataflint_sort_merge_join_example")

# ── BroadcastNestedLoopJoinExec ───────────────────────────────────────────────
print("="*80)
print("Running BroadcastNestedLoopJoinExec example")
print("="*80)
price_ranges = spark.createDataFrame(
    [(100.0, 400.0, "mid-range")], "min_p double, max_p double, range_name string")
spark.sparkContext.setJobDescription("BroadcastNestedLoopJoinExec: non-equi join")
df.join(price_ranges, (col("price") >= col("min_p")) & (col("price") <= col("max_p"))) \
  .select("customer", "category", "price", "range_name") \
  .write.mode("overwrite").parquet("/tmp/dataflint_bnlj_example")
print("\nResult written to /tmp/dataflint_bnlj_example")

# ── CartesianProductExec ──────────────────────────────────────────────────────
print("="*80)
print("Running CartesianProductExec example")
print("="*80)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
currencies = spark.createDataFrame([("USD",), ("EUR",)], "currency string")
spark.sparkContext.setJobDescription("CartesianProductExec: cross join with broadcast disabled")
df.crossJoin(currencies) \
  .select("customer", "category", "price", "currency") \
  .write.mode("overwrite").parquet("/tmp/dataflint_cartesian_example")
spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
print("\nResult written to /tmp/dataflint_cartesian_example")

# ── WindowGroupLimitExec (Spark 3.4+ top-N per group optimization) ────────────
print("="*80)
print("Running WindowGroupLimitExec example (top-3 per category)")
print("="*80)
w_top = Window.partitionBy("category").orderBy(col("price").desc())
spark.sparkContext.setJobDescription("WindowGroupLimitExec: top-N per group")
df.withColumn("rn", row_number().over(w_top)) \
  .filter(col("rn") <= 3) \
  .drop("rn") \
  .write.mode("overwrite").parquet("/tmp/dataflint_window_group_limit_example")
print("\nResult written to /tmp/dataflint_window_group_limit_example")

# ── SortAggregateExec ─────────────────────────────────────────────────────────
print("="*80)
print("Running SortAggregateExec example")
print("="*80)
spark.conf.set("spark.sql.execution.useObjectHashAggregateExec", "false")
spark.sparkContext.setJobDescription("SortAggregateExec: collect_list forces sort-based agg")
df.groupBy("category") \
  .agg(collect_list("customer").alias("customers")) \
  .write.mode("overwrite").parquet("/tmp/dataflint_sort_agg_example")
spark.conf.set("spark.sql.execution.useObjectHashAggregateExec", "true")
print("\nResult written to /tmp/dataflint_sort_agg_example")


# ── Cache + Filter + Aggregate + Union ────────────────────────────────────────
print("="*80)
print("Running Cache + Filter + Aggregate + Union example")
print("="*80)
spark.sparkContext.setJobDescription("Cache + Filter + Agg + Union: read, cache, two branches, union")

df.write.mode("overwrite").parquet("/tmp/tempsave")

cached_df = spark.read.parquet("/tmp/tempsave").filter(col("price") > 0).cache()
# cached_df.count()  # materialize the cache

high_value = cached_df.filter(col("price") > 200) \
    .groupBy("category") \
    .agg(spark_sum("price").alias("total_price"), avg("price").alias("avg_price"))

low_value = cached_df.filter(col("price") <= 200) \
    .groupBy("category") \
    .agg(spark_sum("price").alias("total_price"), avg("price").alias("avg_price"))

high_value.union(low_value) \
    .write.mode("overwrite").parquet("/tmp/dataflint_cache_filter_union_example")

cached_df.unpersist()
print("\nResult written to /tmp/dataflint_cache_filter_union_example")


# ── RDDScanExec codegen with complex types ───────────────────────────────────
# Reproduces codegen compilation error when TimedWithCodegenExec wraps RDDScanExec
# ("Scan ExistingRDD") — the space in nodeName produces invalid Java identifiers
# via variablePrefix. GenerateUnsafeProjection uses ctx.freshName("previousCursor")
# which picks up the space-containing prefix when projecting structs/arrays/maps.
print("="*80)
print("Running RDDScanExec codegen with complex types (struct/array)")
print("="*80)
from pyspark.sql.types import ArrayType, MapType
from pyspark.sql.functions import struct, array as spark_array, create_map

complex_data = [
    ("Alice", "Electronics", [1, 2, 3], {"color": "red", "size": "M"}),
    ("Bob", "Books", [4, 5], {"color": "blue", "size": "L"}),
    ("Charlie", "Clothing", [6], {"color": "green", "size": "S"}),
] * 1000

complex_schema = StructType([
    StructField("customer", StringType(), False),
    StructField("category", StringType(), False),
    StructField("tags", ArrayType(IntegerType()), True),
    StructField("attributes", MapType(StringType(), StringType()), True),
])

df_complex = spark.createDataFrame(complex_data, complex_schema)

spark.sparkContext.setJobDescription("RDDScanExec codegen: struct/array/map projection triggers UnsafeProjection")
df_complex.select(
    col("customer"),
    struct(col("category"), col("tags")).alias("info"),
    col("attributes"),
) \
  .filter(col("customer") != "Alice") \
  .write.mode("overwrite").parquet("/tmp/dataflint_rddscan_complex_codegen_test")
print("\nResult written to /tmp/dataflint_rddscan_complex_codegen_test")

# ── RDDScanExec codegen stress test ──────────────────────────────────────────
print("="*80)
print("Running RDDScanExec codegen stress test (variablePrefix space issue)")
print("="*80)
spark.sparkContext.setJobDescription("RDDScanExec codegen: multi-column projection + filter + agg over createDataFrame")
df.select(
    col("customer"),
    col("category"),
    (col("price") * col("quantity")).alias("revenue"),
    (col("price") * 0.9).alias("discounted_price"),
    (col("quantity") + 1).alias("quantity_plus_one"),
) \
  .filter(col("revenue") > 100) \
  .groupBy("category") \
  .agg(
      spark_sum("revenue").alias("total_revenue"),
      avg("discounted_price").alias("avg_discounted"),
  ) \
  .write.mode("overwrite").parquet("/tmp/dataflint_rddscan_codegen_test")
print("\nResult written to /tmp/dataflint_rddscan_codegen_test")


print("\n" + "="*80)
print("Done!")
print("="*80)

input("\nPress Enter to exit...")
spark.stop()