# Spark History Server with DataFlint

Docker image for Apache Spark History Server with the DataFlint plugin and UI pre-installed.

## Quick Start

### Using Docker (from Maven)

```bash
# Build the image (downloads JAR from Maven Central)
docker build -t dataflint-history-server .

# Run with your event logs directory
docker run -d \
  -p 18080:18080 \
  -v /path/to/spark-events:/spark-history:ro \
  --name dataflint-history-server \
  dataflint-history-server
```

Access the History Server at http://localhost:18080. Click on any application to see the DataFlint tab.

### Using Local JARs (for development)

```bash
# Step 1: Build the JARs locally
./build-jars.sh

# Step 2: Build Docker image with local JAR
docker build -t dataflint-history-server --build-arg USE_LOCAL_JAR=true .

# Step 3: Run
docker run -d \
  -p 18080:18080 \
  -v /path/to/spark-events:/spark-history:ro \
  dataflint-history-server
```

### Using Docker Compose

```bash
# Set your event logs directory
export SPARK_HISTORY_DIR=/path/to/spark-events

# Start the service
docker-compose up -d
```

## Build Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `SPARK_VERSION` | `3.5.1` | Apache Spark version |
| `SCALA_VERSION` | `2.12` | Scala binary version (2.12 or 2.13) |
| `DATAFLINT_VERSION` | `0.8.3` | DataFlint plugin version |
| `USE_LOCAL_JAR` | `false` | Use locally built JAR instead of Maven |

### Examples

```bash
# Spark 3.5.3 from Maven
docker build -t dataflint-hs:3.5.3 --build-arg SPARK_VERSION=3.5.3 .

# Spark 3.4.1 with Scala 2.13 from Maven
docker build -t dataflint-hs:3.4.1 \
  --build-arg SPARK_VERSION=3.4.1 \
  --build-arg SCALA_VERSION=2.13 .

# Spark 4.0.0 from Maven (automatically uses Scala 2.13)
docker build -t dataflint-hs:4.0.0 --build-arg SPARK_VERSION=4.0.0 .

# Local JAR for Spark 3.x
./build-jars.sh
docker build -t dataflint-hs:local --build-arg USE_LOCAL_JAR=true .

# Local JAR for Spark 4.x
./build-jars.sh
docker build -t dataflint-hs:4.0.0-local \
  --build-arg USE_LOCAL_JAR=true \
  --build-arg SPARK_VERSION=4.0.0 .
```

## Build Script

The `build-jars.sh` script automates the local build process:

1. Builds the React UI (`spark-ui`)
2. Builds plugin JARs for all Scala versions (`spark-plugin`)
3. Copies JARs to `docker/jars/` directory

**Prerequisites:**
- Node.js 20+
- Java 8+
- sbt

## Runtime Configuration

### Environment Variables (docker-compose)

| Variable | Default | Description |
|----------|---------|-------------|
| `SPARK_HISTORY_DIR` | `./spark-events` | Host path to Spark event logs |
| `HISTORY_SERVER_PORT` | `18080` | Host port for History Server |

### Volume Mount

The container expects event logs at `/spark-history`. Mount your Spark event logs directory:

```bash
docker run -v /your/spark/events:/spark-history:ro ...
```

## Spark Version Compatibility

| Spark Version | Scala Version | Notes |
|---------------|---------------|-------|
| 3.2.x - 3.5.x | 2.12, 2.13 | Default: 2.12 |
| 4.0.x | 2.13 | Automatically selected |

## Generating Event Logs

To enable event logging in your Spark applications:

```python
spark = SparkSession.builder \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "/path/to/spark-events") \
    .getOrCreate()
```

Or via spark-submit:
```bash
spark-submit \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=/path/to/spark-events \
  your_app.py
```

## Cloud Storage

For S3, GCS, or other cloud storage, you may need to add additional JARs and configuration:

```bash
docker run -d \
  -p 18080:18080 \
  -e SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=s3a://bucket/spark-events" \
  -v /path/to/aws-hadoop-jars:/opt/spark/jars/cloud:ro \
  dataflint-history-server
```

## Troubleshooting

### No applications showing
- Verify event logs exist in the mounted directory
- Check logs: `docker logs dataflint-history-server`
- Ensure event log files are complete (not still being written)

### DataFlint tab not appearing
- The DataFlint tab appears when you click on a specific application
- Verify the plugin JAR was downloaded: `docker exec dataflint-history-server ls /opt/spark/jars/dataflint*`

### Local JAR build fails
- Ensure Node.js 20+ is installed: `node --version`
- Ensure Java 8+ is installed: `java -version`
- Ensure sbt is installed: `sbt --version`
- Check that you're running from the `docker/` directory