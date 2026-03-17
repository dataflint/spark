"""
Integration test helper: registers 4 DataFlint Python exec node scenarios as temp views.

The Scala test (DataFlintPythonIntegrationSpec) connects via PythonRunner / Py4J,
calls this script, then checks each view's executedPlan for the DataFlint node.

Registered views:
  batch_eval_view        — @udf               → DataFlintBatchEvalPython
  arrow_eval_view        — @pandas_udf scalar  → DataFlintArrowEvalPython
  flat_map_groups_view   — applyInPandas       → DataFlintFlatMapGroupsInPandas
  flat_map_cogroups_view — cogroup.applyIn…    → DataFlintFlatMapCoGroupsInPandas
"""
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

# Build SparkConf from the existing JavaSparkContext so spark.master (and all other
# existing configs) are present — without this PySpark raises MASTER_URL_NOT_SET.
conf  = pyspark.conf.SparkConf(True, gateway.jvm, jsc.getConf())
sc    = pyspark.SparkContext(gateway=gateway, jsc=jsc, conf=conf)
spark = SparkSession(sc, jsparkSession=spark_jvm)

df = spark.createDataFrame(
    [(1, "a"), (2, "b"), (3, "a"), (4, "b")],
    ["id", "cat"]
)

# 1 — BatchEvalPython
@udf(returnType=LongType())
def double_udf(x):
    return x * 2

df.select(double_udf("id")).createOrReplaceTempView("batch_eval_view")

# 2 — ArrowEvalPython
@pandas_udf(LongType())
def double_pandas_udf(s: pd.Series) -> pd.Series:
    return s * 2

df.select(double_pandas_udf("id")).createOrReplaceTempView("arrow_eval_view")

# 3 — FlatMapGroupsInPandas
def identity_group(key, pdf):
    return pdf

df.groupby("cat").applyInPandas(
    identity_group, schema="id long, cat string"
).createOrReplaceTempView("flat_map_groups_view")

# 4 — FlatMapCoGroupsInPandas
df2 = spark.createDataFrame(
    [(1, "x"), (2, "y"), (3, "z"), (4, "w")],
    ["id", "label"]
)

def cogroup_fn(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
    left = left.copy()
    left["label"] = right["label"].values[0] if len(right) > 0 else None
    return left[["id", "cat", "label"]]

df.groupby("id").cogroup(df2.groupby("id")).applyInPandas(
    cogroup_fn, schema="id long, cat string, label string"
).createOrReplaceTempView("flat_map_cogroups_view")