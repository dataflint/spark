<p align="center">
<img alt="Logo" src="documentation/resources/logo.png" height="300">
</p>

<h2 align="center">
 Data-Application Performance Monitoring for data engineers
</h2>

<div align="center">

[![Maven Package](https://maven-badges.herokuapp.com/maven-central/io.dataflint/spark_2.12/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.dataflint/spark_2.12)
[![Slack](https://img.shields.io/badge/Slack-Join%20Us-purple)](https://join.slack.com/t/dataflint/shared_invite/zt-28sr3r3pf-Td_mLx~0Ss6D1t0EJb8CNA)
[![Test Status](https://github.com/dataflint/spark/actions/workflows/ci.yml/badge.svg)](https://github.com/your_username/your_repo/actions/workflows/tests.yml)
[![Docs](https://img.shields.io/badge/Docs-Read%20the%20Docs-blue)](https://dataflint.gitbook.io/dataflint-for-spark/)
![License](https://img.shields.io/badge/License-Apache%202.0-orange)

If you enjoy DataFlint please give us a ⭐️ and join our [slack community](https://join.slack.com/t/dataflint/shared_invite/zt-28sr3r3pf-Td_mLx~0Ss6D1t0EJb8CNA) for feature requests, support and more!

</div>

## What is DataFlint?

DataFlint is an open-source D-APM (Data-Application Performance Monitoring) for Apache Spark, built for big data engineers.

DataFlint mission is to bring the development experience of using APM (Application Performance Monitoring) solutions such as DataDog and New Relic for the big data world.

DataFlint is installed within minutes via open source library, working on top of the existing Spark-UI infrastructure, all in order to help you solve big data performance issues and debug failures!

![Demo](documentation/resources/demo.gif)

## Features

- 📈 Real-time query and cluster status
- 📊 Query breakdown with performance heat map
- 📋 Application Run Summary
- ⚠️ Performance alerts and suggestions
- 👀 Identify query failures
- 🤖 Spark AI Assistant

See [Our Features](https://dataflint.gitbook.io/dataflint-for-spark/overview/our-features) for more information

## Installation

Install DataFlint via sbt:
```sbt
libraryDependencies += "io.dataflint" %% "spark_2.12" % "0.1.0"
```

Then instruct spark to load the DataFlint plugin:
```scala
val spark = SparkSession
    .builder
    .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin")
    ...
    .getOrCreate()
```

Alternatively, install DataFlint with **no code change** as a spark ivy package by adding these 2 lines to your spark-submit command:

```bash
spark-submit
--packages io.dataflint:spark_2.12:0.1.0 \
--conf spark.plugins=io.dataflint.spark.SparkDataflintPlugin \
...
```

After the installations you will see a "DataFlint" button in Spark UI, click on it to start using DataFlint

<img alt="Logo" src="documentation/resources/usage.png">


* For more installation options, including for python and k8s spark-operator, see [Install on Spark docs](https://dataflint.gitbook.io/dataflint-for-spark/getting-started/install-on-spark)
* For installing DataFlint in spark history server for observability on completed runs see [install on spark history server docs](https://dataflint.gitbook.io/dataflint-for-spark/getting-started/install-on-spark-history-server)
* For installing DataFlint on DataBricks see [install on databricks docs](https://dataflint.gitbook.io/dataflint-for-spark/getting-started/install-on-databricks)

## How it Works

![How it Works](documentation/resources/howitworks.png)

DataFlint is installed as a plugin on the spark driver and history server.

The plugin exposes an additional HTTP resoures for additional metrics not available in Spark UI, and a modern SPA web-app that fetches data from spark without the need to refresh the page.

For more information, see [how it works docs](https://dataflint.gitbook.io/dataflint-for-spark/overview/how-it-works)

## Compatibility Matrix

DataFlint require spark version 3.2 and up, and scala version 2.12. 

| Platforms                 | Spark | History Server |
|---------------------------|-------|----------------|
| Local                     |   ✅   |       ✅       |
| Standalone                |   ✅   |       ✅       |
| Kubernetes Spark Operator |   ✅   |       ✅       |
| EMR                       |   ✅   |       ✅       |
| Dataproc                  |   ✅   |       ❓       |
| HDInsights                |   ✅   |       ❓       |
| Databricks                |   ✅   |       ❌       |

For more information, see [supported versions docs](https://dataflint.gitbook.io/dataflint-for-spark/overview/supported-versions)