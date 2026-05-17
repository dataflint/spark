# Spark + Gluten/Velox + DataFlint example runner
#
# Build arguments:
#   SPARK_VERSION - Spark version (default: 3.5.7)
#   GLUTEN_JAR    - Filename of the Gluten bundle jar in jars/ directory
#
# Usage:
#   ./run-gluten-example.sh   (recommended — builds everything and runs)
#   docker compose up --build (if jars are already in jars/)

ARG SPARK_VERSION=3.5.7

FROM apache/spark:${SPARK_VERSION}

ARG SPARK_VERSION=3.5.7

USER root

# Create directories for event logs and test data
RUN mkdir -p /tmp/spark-events && \
    chown -R spark:spark /tmp/spark-events && \
    mkdir -p /opt/spark/work-dir/test_data && \
    chown -R spark:spark /opt/spark/work-dir/test_data

# Copy all jars (Gluten bundle + DataFlint plugin + example) into Spark's jars dir
COPY jars/*.jar /opt/spark/jars/

# Copy test data
COPY test_data/ /opt/spark/work-dir/test_data/

# Configure Spark defaults for Gluten
# The --add-opens flags are required because the Gluten nightly (JDK8 target) uses
# sun.misc.Unsafe / DirectByteBuffer internals that are module-restricted on Java 11+.
RUN mkdir -p /opt/spark/conf && \
    echo "spark.plugins=io.dataflint.spark.SparkDataflintPlugin,org.apache.gluten.GlutenPlugin" >> /opt/spark/conf/spark-defaults.conf && \
    echo "spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager" >> /opt/spark/conf/spark-defaults.conf && \
    echo "spark.memory.offHeap.enabled=true" >> /opt/spark/conf/spark-defaults.conf && \
    echo "spark.memory.offHeap.size=4g" >> /opt/spark/conf/spark-defaults.conf && \
    echo "spark.eventLog.enabled=true" >> /opt/spark/conf/spark-defaults.conf && \
    echo "spark.eventLog.dir=/tmp/spark-events" >> /opt/spark/conf/spark-defaults.conf && \
    echo "spark.ui.port=10000" >> /opt/spark/conf/spark-defaults.conf && \
    echo "spark.dataflint.telemetry.enabled=false" >> /opt/spark/conf/spark-defaults.conf && \
    echo "spark.sql.maxMetadataStringLength=10000" >> /opt/spark/conf/spark-defaults.conf && \
    echo "spark.sql.adaptive.enabled=true" >> /opt/spark/conf/spark-defaults.conf && \
    echo "spark.driver.extraJavaOptions=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.misc=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED" >> /opt/spark/conf/spark-defaults.conf && \
    echo "spark.executor.extraJavaOptions=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.misc=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED" >> /opt/spark/conf/spark-defaults.conf

USER spark

EXPOSE 10000

WORKDIR /opt/spark/work-dir

ENV _JAVA_OPTIONS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.misc=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true"

# Run the Gluten example via spark-submit
CMD ["/opt/spark/bin/spark-submit", \
     "--master", "local[*]", \
     "--class", "io.dataflint.example.GlutenVeloxExample", \
     "--driver-memory", "2g", \
     "/opt/spark/jars/example.jar"]
