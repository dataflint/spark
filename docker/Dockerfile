# Spark History Server with DataFlint Plugin
#
# Build arguments:
#   SPARK_VERSION - Spark version to use (default: 3.5.1)
#   SCALA_VERSION - Scala binary version: 2.12 or 2.13 (default: 2.12)
#   DATAFLINT_VERSION - DataFlint plugin version (default: 0.8.3)
#   USE_LOCAL_JAR - Use locally built JAR instead of Maven (default: false)
#
# Runtime:
#   Mount your event log directory to /spark-history
#   Example: docker run -v /path/to/logs:/spark-history -p 18080:18080 dataflint-history-server
#
# Build examples:
#   # From Maven (default):
#   docker build -t dataflint-history-server .
#   docker build -t dataflint-history-server --build-arg SPARK_VERSION=3.5.3 .
#
#   # From local JAR (run ./build-jars.sh first):
#   docker build -t dataflint-history-server --build-arg USE_LOCAL_JAR=true .
#   docker build -t dataflint-history-server --build-arg USE_LOCAL_JAR=true --build-arg SPARK_VERSION=4.0.0 .

ARG SPARK_VERSION=3.5.1

FROM apache/spark:${SPARK_VERSION}

# Re-declare ARGs after FROM to make them available
ARG SPARK_VERSION=3.5.1
ARG SCALA_VERSION=2.12
ARG DATAFLINT_VERSION=0.8.3
ARG USE_LOCAL_JAR=false

# Labels
LABEL maintainer="DataFlint"
LABEL description="Spark History Server with DataFlint plugin and UI"
LABEL spark.version="${SPARK_VERSION}"
LABEL dataflint.version="${DATAFLINT_VERSION}"

USER root

# Install curl for downloading the JAR (only needed for Maven download)
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy local JARs directory (will be empty if not using local JARs)
COPY jars/ /tmp/dataflint-jars/

# Install DataFlint plugin - either from local JAR or Maven
RUN SPARK_MAJOR=$(echo ${SPARK_VERSION} | cut -d. -f1) && \
    if [ "$SPARK_MAJOR" = "4" ]; then \
        ARTIFACT_NAME="dataflint-spark4"; \
        SCALA_VER="2.13"; \
    else \
        ARTIFACT_NAME="spark"; \
        SCALA_VER="${SCALA_VERSION}"; \
    fi && \
    JAR_NAME="${ARTIFACT_NAME}_${SCALA_VER}-${DATAFLINT_VERSION}-SNAPSHOT.jar" && \
    if [ "${USE_LOCAL_JAR}" = "true" ]; then \
        echo "Using local JAR: ${JAR_NAME}" && \
        if [ -f "/tmp/dataflint-jars/${JAR_NAME}" ]; then \
            cp "/tmp/dataflint-jars/${JAR_NAME}" /opt/spark/jars/dataflint-plugin.jar; \
        else \
            echo "ERROR: Local JAR not found: /tmp/dataflint-jars/${JAR_NAME}" && \
            echo "Available JARs:" && ls -la /tmp/dataflint-jars/ && \
            exit 1; \
        fi; \
    else \
        echo "Downloading from Maven: io.dataflint:${ARTIFACT_NAME}_${SCALA_VER}:${DATAFLINT_VERSION}" && \
        curl -fSL -o /opt/spark/jars/dataflint-plugin.jar \
            "https://repo1.maven.org/maven2/io/dataflint/${ARTIFACT_NAME}_${SCALA_VER}/${DATAFLINT_VERSION}/${ARTIFACT_NAME}_${SCALA_VER}-${DATAFLINT_VERSION}.jar"; \
    fi && \
    rm -rf /tmp/dataflint-jars

# Create directory for spark event logs
RUN mkdir -p /spark-history && \
    chown -R spark:spark /spark-history

# Configure Spark defaults for History Server with DataFlint
RUN mkdir -p /opt/spark/conf && \
    touch /opt/spark/conf/spark-defaults.conf && \
    echo "spark.history.fs.logDirectory=/spark-history" >> /opt/spark/conf/spark-defaults.conf && \
    echo "spark.history.ui.port=18080" >> /opt/spark/conf/spark-defaults.conf

# Create history server startup script
RUN echo '#!/bin/bash' > /opt/spark/bin/start-history-server.sh && \
    echo 'exec /opt/spark/bin/spark-class org.apache.spark.deploy.history.HistoryServer "$@"' >> /opt/spark/bin/start-history-server.sh && \
    chmod +x /opt/spark/bin/start-history-server.sh

# Switch back to spark user
USER spark

# Expose History Server port
EXPOSE 18080

# Volume for event logs
VOLUME ["/spark-history"]

# Set working directory
WORKDIR /opt/spark

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:18080 || exit 1

# Start History Server
CMD ["/opt/spark/bin/start-history-server.sh"]