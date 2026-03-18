"""
Integration test helper: registers one DataFlint Python exec node scenario as a temp view.

Usage: pass the test name as the first argument (sys.argv[1]):
  batch_eval       — @udf               → registers batch_eval_view
  arrow_eval       — @pandas_udf scalar  → registers arrow_eval_view
  flat_map_groups  — applyInPandas       → registers flat_map_groups_view
  flat_map_cogroups— cogroup.applyIn…    → registers flat_map_cogroups_view

The Scala test (DataFlintPythonIntegrationSpec) calls PythonRunner.main with one of
the above names, then checks the corresponding view's executedPlan.
"""
import sys
import pyspark
import pyspark.java_gateway
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.types import LongType
import pandas as pd

test_name = sys.argv[1] if len(sys.argv) > 1 else ""

gateway   = pyspark.java_gateway.launch_gateway()
static    = gateway.jvm.org.apache.spark.dataflint.DataFlintStaticSession
jsc       = static.javaSparkContext()
spark_jvm = static.session()

conf  = pyspark.conf.SparkConf(True, gateway.jvm, jsc.getConf())
sc    = pyspark.SparkContext(gateway=gateway, jsc=jsc, conf=conf)
spark = SparkSession(sc, jsparkSession=spark_jvm)

df = spark.createDataFrame(
    [(1, "a"), (2, "b"), (3, "a"), (4, "b")],
    ["id", "cat"]
)


def register_batch_eval():
    @udf(returnType=LongType())
    def double_udf(x):
        return x * 2
    df.select(double_udf("id")).createOrReplaceTempView("batch_eval_view")


def register_arrow_eval():
    @pandas_udf(LongType())
    def double_pandas_udf(s: pd.Series) -> pd.Series:
        return s * 2
    df.select(double_pandas_udf("id")).createOrReplaceTempView("arrow_eval_view")


def register_flat_map_groups():
    def identity_group(key, pdf):
        return pdf
    df.groupby("cat").applyInPandas(
        identity_group, schema="id long, cat string"
    ).createOrReplaceTempView("flat_map_groups_view")


def register_flat_map_cogroups():
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


_tests = {
    "batch_eval":        register_batch_eval,
    "arrow_eval":        register_arrow_eval,
    "flat_map_groups":   register_flat_map_groups,
    "flat_map_cogroups": register_flat_map_cogroups,
}

if test_name not in _tests:
    print(f"Unknown test '{test_name}'. Valid options: {list(_tests)}", file=sys.stderr)
    sys.exit(1)

_tests[test_name]()