import xerial.sbt.Sonatype._

lazy val versionNum: String = "0.1.1"

lazy val dataflint = project
  .in(file("."))
  .aggregate(
    plugin,
    example_3_1_3,
    example_3_2_4,
    example_3_3_3,
    example_3_4_1,
    example_3_5_0,
    example_3_4_1_remote
  ).settings(
    publish / skip := true
  )

lazy val plugin = (project in file("plugin"))
  .settings(
    name := "spark",
    organization := "io.dataflint",
    scalaVersion := "2.12.18",
    version      := (if (git.gitCurrentTags.value.exists(_.startsWith("v"))) {
      versionNum
    } else {
      versionNum + "-SNAPSHOT"
    }),
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.1" % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.1"  % "provided",
    publishTo := sonatypePublishToBundle.value
  )

lazy val example_3_1_3 = (project in file("example_3_1_3"))
  .settings(
    name := "DataflintSparkExample313",
    organization := "io.dataflint",
    scalaVersion := "2.12.18",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.3",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.3",
    publish / skip := true
  ).dependsOn(plugin)

lazy val example_3_2_4 = (project in file("example_3_2_4"))
  .settings(
    name := "DataflintSparkExample324",
    organization := "io.dataflint",
    scalaVersion := "2.12.18",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.4",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.4",
    publish / skip := true
  ).dependsOn(plugin)

lazy val example_3_3_3 = (project in file("example_3_3_3"))
  .settings(
    name := "DataflintSparkExample333",
    organization := "io.dataflint",
    scalaVersion := "2.12.18",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.3",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.3",
    publish / skip := true
  ).dependsOn(plugin)

lazy val example_3_4_1 = (project in file("example_3_4_1"))
  .settings(
    name := "DataflintSparkExample341",
    organization := "io.dataflint",
    scalaVersion := "2.12.18",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.1",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.1",
    publish / skip := true
  ).dependsOn(plugin)

lazy val example_3_5_0 = (project in file("example_3_5_0"))
  .settings(
    name := "DataflintSparkExample350",
    organization := "io.dataflint",
    scalaVersion := "2.12.18",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0",
    libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.5.0",
    libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0",
    libraryDependencies += "io.delta" %% "delta-spark" % "3.0.0",
    publish / skip := true
  ).dependsOn(plugin)

lazy val example_3_4_1_remote = (project in file("example_3_4_1_remote"))
  .settings(
      name := "DataflintSparkExample341Remote",
      organization := "io.dataflint",
      scalaVersion := "2.12.18",
      libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.1",
      libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.1",
      publish / skip := true
  ).dependsOn()

