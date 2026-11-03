#!/usr/bin/env bash
# Local dev helper for DataFlint OSS.
#
#   ./dev.sh job [MainClass] [args]  Run a Spark 3.5 example with the plugin (default: DevPlayground).
#                                    Live UI: http://localhost:10000/dataflint/  (press Enter to stop)
#   ./dev.sh cluster-job MainClass [args]
#                                    Run an example on a local standalone cluster (1 worker, 4 executors x 2 cores),
#                                    starting the cluster if needed. Master UI: http://localhost:8080
#   ./dev.sh cluster-stop            Stop the local standalone cluster
#   ./dev.sh ui                      Hot-reload UI dev server against the live job: http://localhost:4000
#   ./dev.sh deploy-ui               Build the UI into the plugin resources (needed for the :10000 / :18080 UI)
#   ./dev.sh shs                     Run a Spark history server with the plugin: http://localhost:18080
#   ./dev.sh shs-ui                  Hot-reload UI dev server against the history server: http://localhost:3000
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
SPARK_HOME="${SPARK_HOME:-/opt/spark-3.5.1}"
EVENT_LOG_DIR="${EVENT_LOG_DIR:-/tmp/spark-events}"
export JAVA_HOME="${DEV_JAVA_HOME:-$(/usr/libexec/java_home -v 11)}"
# keeps the standalone cluster's logs, pids and work dirs out of SPARK_HOME
CLUSTER_DIR="${CLUSTER_DIR:-/tmp/dataflint-dev-cluster}"
export SPARK_LOG_DIR="$CLUSTER_DIR/logs" SPARK_PID_DIR="$CLUSTER_DIR" SPARK_WORKER_DIR="$CLUSTER_DIR/work"

cmd="${1:-}"
shift || true

case "$cmd" in
  job)
    main="${1:-DevPlayground}"
    [[ "$main" == *.* ]] || main="io.dataflint.example.$main"
    cd "$ROOT/spark-plugin"
    shift || true
    exec sbt "example_3_5_1/runMain $main $*"
    ;;
  cluster-job)
    main="${1:?usage: ./dev.sh cluster-job MainClass [args]}"
    [[ "$main" == *.* ]] || main="io.dataflint.example.$main"
    shift
    mkdir -p "$EVENT_LOG_DIR" "$CLUSTER_DIR"
    cd "$ROOT/spark-plugin"
    sbt -batch pluginspark3/assembly example_3_5_1/package
    plugin_jar="$ROOT/spark-plugin/$(ls -t pluginspark3/target/scala-2.12/spark_2.12-*.jar | head -1)"
    example_jar="$ROOT/spark-plugin/$(ls -t example_3_5_1/target/scala-2.12/dataflintsparkexample351_2.12-*.jar | head -1)"
    if ! nc -z localhost 7077 2>/dev/null; then
      "$SPARK_HOME/sbin/start-master.sh" --host localhost
      "$SPARK_HOME/sbin/start-worker.sh" spark://localhost:7077 --cores 8 --memory 8g
      sleep 5
    fi
    exec "$SPARK_HOME/bin/spark-submit" --master spark://localhost:7077 \
      --executor-cores 2 --total-executor-cores 8 --executor-memory 1g --driver-memory 2g \
      --jars "$plugin_jar" --class "$main" "$example_jar" "$@"
    ;;
  cluster-stop)
    "$SPARK_HOME/sbin/stop-worker.sh" || true
    exec "$SPARK_HOME/sbin/stop-master.sh"
    ;;
  ui)
    cd "$ROOT/spark-ui"
    trap 'kill 0' EXIT
    npm run proxy &
    BROWSER=none npm run start
    ;;
  deploy-ui)
    cd "$ROOT/spark-ui"
    exec npm run deploy
    ;;
  shs)
    mkdir -p "$EVENT_LOG_DIR"
    cd "$ROOT/spark-plugin"
    sbt -batch pluginspark3/assembly
    jar="$(ls -t pluginspark3/target/scala-2.12/spark_2.12-*.jar | head -1)"
    echo "Using plugin jar: $jar"
    export SPARK_DAEMON_CLASSPATH="$ROOT/spark-plugin/$jar"
    export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file:$EVENT_LOG_DIR -Dspark.history.fs.update.interval=5s"
    exec "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.history.HistoryServer
    ;;
  shs-ui)
    cd "$ROOT/spark-ui"
    trap 'kill 0' EXIT
    npm run proxy-history &
    BROWSER=none npm run start-history
    ;;
  *)
    sed -n '2,14p' "$0"
    exit 1
    ;;
esac
