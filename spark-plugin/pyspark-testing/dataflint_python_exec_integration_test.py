"""
Integration test: verifies all 4 DataFlint Python exec nodes are instrumented.

Connects to the Scala test's SparkSession via the Py4J gateway that PythonRunner
sets up automatically (PYSPARK_GATEWAY_PORT / PYSPARK_GATEWAY_SECRET env vars).

Tested nodes:
  BatchEvalPython        -> DataFlintBatchEvalPython      (@udf)
  ArrowEvalPython        -> DataFlintArrowEvalPython      (@pandas_udf scalar)
  FlatMapGroupsInPandas  -> DataFlintFlatMapGroupsInPandas (applyInPandas)
  FlatMapCoGroupsInPandas-> DataFlintFlatMapCoGroupsInPandas (cogroup applyInPandas)
"""
import sys
import pyspark
import pyspark.java_gateway
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.types import LongType
import pandas as pd

# ---------------------------------------------------------------------------
# Connect to the Scala test's SparkSession via the existing Py4J gateway.
# PythonRunner has already set PYSPARK_GATEWAY_PORT in this process's env,
# so launch_gateway() connects to the running JVM instead of launching a new one.
# ---------------------------------------------------------------------------
gateway   = pyspark.java_gateway.launch_gateway()
static    = gateway.jvm.org.apache.spark.dataflint.DataFlintStaticSession
jsc       = static.javaSparkContext()
spark_jvm = static.session()

sc    = pyspark.SparkContext(gateway=gateway, jsc=jsc)
spark = SparkSession(sc, jsparkSession=spark_jvm)

print(f"Connected to Spark {spark.version}")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
failures = []

def executed_plan_str(df):
    """Return the full executedPlan string after triggering execution."""
    df.collect()
    return df._jdf.queryExecution().executedPlan().toString()

def assert_dataflint_node(df, expected_node, test_name):
    plan = executed_plan_str(df)
    if expected_node in plan:
        print(f"  PASS  [{test_name}] — {expected_node} found in plan")
    else:
        msg = f"[{test_name}] '{expected_node}' not found in plan:\n{plan}"
        failures.append(msg)
        print(f"  FAIL  {msg}")

# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------
df = spark.createDataFrame(
    [(1, "a"), (2, "b"), (3, "a"), (4, "b")],
    ["id", "cat"]
)

# ---------------------------------------------------------------------------
# Test 1 — @udf  →  BatchEvalPython  →  DataFlintBatchEvalPython
# ---------------------------------------------------------------------------
print("\n[1] BatchEvalPython (@udf)")

@udf(returnType=LongType())
def double_udf(x):
    return x * 2

assert_dataflint_node(
    df.select(double_udf("id")),
    "DataFlintBatchEvalPython",
    "BatchEvalPython",
)

# ---------------------------------------------------------------------------
# Test 2 — @pandas_udf scalar  →  ArrowEvalPython  →  DataFlintArrowEvalPython
# ---------------------------------------------------------------------------
print("\n[2] ArrowEvalPython (@pandas_udf scalar)")

@pandas_udf(LongType())
def double_pandas_udf(s: pd.Series) -> pd.Series:
    return s * 2

assert_dataflint_node(
    df.select(double_pandas_udf("id")),
    "DataFlintArrowEvalPython",
    "ArrowEvalPython",
)

# ---------------------------------------------------------------------------
# Test 3 — applyInPandas  →  FlatMapGroupsInPandas  →  DataFlintFlatMapGroupsInPandas
# ---------------------------------------------------------------------------
print("\n[3] FlatMapGroupsInPandas (applyInPandas)")

def identity_group(key, pdf):
    return pdf

assert_dataflint_node(
    df.groupby("cat").applyInPandas(identity_group, schema="id long, cat string"),
    "DataFlintFlatMapGroupsInPandas",
    "FlatMapGroupsInPandas",
)

# ---------------------------------------------------------------------------
# Test 4 — cogroup applyInPandas  →  FlatMapCoGroupsInPandas  →  DataFlintFlatMapCoGroupsInPandas
# ---------------------------------------------------------------------------
print("\n[4] FlatMapCoGroupsInPandas (cogroup applyInPandas)")

df2 = spark.createDataFrame(
    [(1, "x"), (2, "y"), (3, "z"), (4, "w")],
    ["id", "label"]
)

def cogroup_fn(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
    left = left.copy()
    left["label"] = right["label"].values[0] if len(right) > 0 else None
    return left[["id", "cat", "label"]]

assert_dataflint_node(
    df.groupby("id").cogroup(df2.groupby("id")).applyInPandas(
        cogroup_fn, schema="id long, cat string, label string"
    ),
    "DataFlintFlatMapCoGroupsInPandas",
    "FlatMapCoGroupsInPandas",
)

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
if failures:
    print(f"FAILED — {len(failures)} test(s) did not find the expected DataFlint node:")
    for f in failures:
        print(f"  • {f}")
    sys.exit(1)
else:
    print("ALL PASSED — all 4 DataFlint Python exec nodes are instrumented")
    sys.exit(0)