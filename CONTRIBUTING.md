# Contributing to DataFlint

## Getting started with development

### Setup

Requirements:
1. Node v21.5.0
2. Java 8 or 11
3. Scala 2.12
4. SBT 1.3.13
5. IntelliJ IDEA with Scala and SBT plugins
6. Visual Studio Code

### Installation Steps

1. Clone the repository:
   ```
   git clone https://github.com/dataflint/spark.git
   cd spark
   ```

2. Set up the Spark Plugin:
   - Open the `spark-plugin` folder with IntelliJ IDEA
   - Ensure Scala and SBT plugins are installed in IntelliJ

3. Set up the UI:
   - Open the repository with Visual Studio Code
   - Install UI dependencies:
     ```
     cd spark-ui
     npm install
     ```

4. Build the UI for the plugin:
   ```
   cd spark-ui
   npm run deploy
   ```

5. (Optional) Install Local CORS Proxy (LCP) for local development:
   ```
   brew install lcp
   ```

### Quick Local Dev Loop (`dev.sh`)

`dev.sh` at the repo root wraps the common workflows (macOS; uses Java 11 via `/usr/libexec/java_home`,
override with `DEV_JAVA_HOME`). Run each in its own terminal:

| Command | What it does | URL |
|---|---|---|
| `./dev.sh deploy-ui` | Build the UI into the plugin resources (run after UI changes you want to see on :10000 / :18080) | |
| `./dev.sh job [MainClass] [args]` | Run a Spark 3.5 example from `example_3_5_1` (default `DevPlayground`); it waits for Enter before stopping | http://localhost:10000/dataflint/ |
| `./dev.sh cluster-job <MainClass> [args]` | Run an example on a local standalone cluster (1 worker, 4 executors x 2 cores), starting the cluster if needed; stop it with `./dev.sh cluster-stop` | http://localhost:8080 (master) |
| `./dev.sh ui` | Hot-reload UI dev server proxied to the running job | http://localhost:4000 |
| `./dev.sh shs` | Spark history server (`SPARK_HOME`, default `/opt/spark-3.5.1`) with the plugin jar, reading `/tmp/spark-events` | http://localhost:18080 |
| `./dev.sh shs-ui` | Hot-reload UI dev server proxied to the history server | http://localhost:3000 |

`DevPlayground` runs a mix of scans, aggregations, joins, cache and skew so most views and alerts have data,
and writes event logs to `/tmp/spark-events` so every run also shows up in the history server.

### Running the Project

1. Run one of the examples in the `example_3_5_1` project using IntelliJ (or `./dev.sh job <ExampleName>`)

2. Access the Spark UI:
   - Browse to `http://localhost:10000`
   - Open DataFlint successfully

### Live Frontend Development

For live frontend development, follow these steps:

1. Start the development server and proxy:
   ```
   cd spark-ui
   npm run start
   npm run proxy
   ```

2. Access the development UI:
   - Browse to `http://localhost:4000`
   - This should run the DataFlint UI with live reloading

## Contributing Guidelines

- Please ensure your code follows the project's coding standards
- Submit pull requests for review

Thank you for contributing to DataFlint!
