# Spark development template with SBT

## Install & Setup

```
git clone https://github.com/sanori/spark-sbt.git
cd spark-sbt
```

If you want to import this project as Scala-IDE, Eclipse project, run the following command:

```
sbt eclipse
```

## Word count of Shakespeare

A sample Spark program that count the words of all the works of William Shakespeare are included. There are two versions of word-count. One for RDD version, and the other is DataFrame version.

The text of William Shakespeare was gotten from <https://datahub.io/dataset/william-shakespeare-plays/resource/514d3c17-8469-4ae8-b83f-57678af50735>

## Running Spark on Scala Worksheet

An example to run Spark on Scala Worksheet is presented as <src/wordCount.sc>. A Scala worksheet template for Spark is also provided as <src/sparkWoksheet.template.sc>.

The detail explanation of the Spark worksheet is available at <http://sanori.github.io/2017/07/Running-Spark-on-Scala-Worksheet/>.

## `sbt console` like `spark-shell`

In this project, `sbt console` works like as `spark-shell`. You don't need to install Spark to use spark-shell if you are developing with [sbt](http://www.scala-sbt.org/).

The first time to run `sbt console`, it may take several minutes or almost an hour due to download Spark modules.
