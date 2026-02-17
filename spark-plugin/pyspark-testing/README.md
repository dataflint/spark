# DataFlint PySpark Testing

This directory contains PySpark example scripts that demonstrate and test DataFlint instrumentation for `mapInPandas` and `mapInArrow` operations.

## Files

### `dataflint_pyspark_example.py`
Simple example script that runs both `mapInPandas` and `mapInArrow` operations with DataFlint instrumentation enabled.

**Features:**
- Automatically detects Spark version and loads the correct plugin JAR (Spark 3.x or 4.x)
- Skips `mapInArrow` on Spark versions < 3.3.0 (not supported)
- Writes results to `/tmp/` and displays sample output

**Usage:**
```bash
cd utils
./run-with-spark.sh 3.3.4 ../pyspark-testing/dataflint_pyspark_example.py
./run-with-spark.sh 3.5.1 ../pyspark-testing/dataflint_pyspark_example.py
./run-with-spark.sh 4.0.2 ../pyspark-testing/dataflint_pyspark_example.py  # Requires Java 17+
```

### `dataflint_pyspark_example_test.py`
Test script that executes the same operations and attempts to validate DataFlint instrumentation.

**Features:**
- Executes queries and shows physical plans
- Attempts to detect DataFlint nodes (with known limitations)
- Provides guidance on verifying instrumentation via Spark UI
- Exits with success if queries execute successfully

**Important Note:**
Due to how Spark's columnar transformation rules work, DataFlint nodes may not be visible through Python's `explain()` or `queryExecution()` APIs. They ARE applied at runtime and visible in the Spark UI. The test acknowledges this limitation.

**Usage:**
```bash
cd utils
./run-with-spark.sh 3.3.4 ../pyspark-testing/dataflint_pyspark_example_test.py
```

## Requirements

### Spark Version Support
- **Spark 3.0 - 3.2**: `mapInPandas` only
- **Spark 3.3+**: Both `mapInPandas` and `mapInArrow`
- **Spark 4.0+**: Both operations (requires Java 17+)

### Java Version Requirements
- **Spark 3.x**: Java 8 or 11
- **Spark 4.x**: Java 17 or higher

The `run-with-spark.sh` script will check Java version and fail with a helpful message if requirements aren't met.

### Plugin JARs
The scripts automatically detect the Spark major version and load the appropriate plugin:
- **Spark 3.x**: `pluginspark3/target/scala-2.12/spark_2.12-0.8.5.jar`
- **Spark 4.x**: `pluginspark4/target/scala-2.13/spark_2.13-0.8.5.jar`

Build the required JAR before running:
```bash
# For Spark 3.x
sbt pluginspark3/assembly

# For Spark 4.x
sbt pluginspark4/assembly
```

## Verifying DataFlint Instrumentation

To verify that DataFlint instrumentation is working:

1. Run one of the example scripts
2. Open the Spark UI (typically http://localhost:10001)
3. Go to the **SQL** tab
4. Click on a completed query
5. Look for these nodes in the physical plan:
   - `DataFlintMapInPandasExec` (for mapInPandas)
   - `DataFlintPythonMapInArrowExec` (for mapInArrow)
6. Check for the **duration** metric added by DataFlint

### Why Python API doesn't show DataFlint nodes

Spark's columnar transformation rules (which DataFlint uses) are applied during the `preColumnarTransitions` phase at execution time. The Python API's `explain()` and `queryExecution()` methods show the plan before these transformations are applied, so DataFlint nodes typically won't appear there. However, they ARE applied during actual execution and are visible in the Spark UI.

## Example Output

When DataFlint instrumentation is working, you'll see nodes like this in the Spark UI:

```
DataFlintMapInPandasExec
  duration total (min, med, max (stageId: taskId))
  2.3 s (581 ms, 581 ms, 582 ms (stage 2.0: task 19))
```

This shows:
- The DataFlint instrumented node is being used
- Custom duration metrics are being collected
- Detailed timing information per task

## Troubleshooting

### Java Version Error (Spark 4.x)
```
Error: UnsupportedClassVersionError: ... class file version 61.0
```

**Solution:** Install Java 17+ and set `JAVA_HOME`:
```bash
# macOS with Homebrew
brew install openjdk@17
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
export PATH=$JAVA_HOME/bin:$PATH
```

### Plugin JAR Not Found
```
FileNotFoundError: Plugin JAR not found at ...
```

**Solution:** Build the plugin JAR:
```bash
cd spark-plugin
sbt pluginspark3/assembly  # For Spark 3.x
sbt pluginspark4/assembly  # For Spark 4.x
```

### Python Version Mismatch
```
RuntimeError: Python in worker has different version 3.9 than that in driver 3.12
```

**Solution:** The `run-with-spark.sh` script sets `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` to use the same Python interpreter. Make sure you're using the script to run the examples.
