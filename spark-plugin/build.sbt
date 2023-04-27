lazy val root = project
  .in(file("."))
  .aggregate(
    plugin,
    example_3_3_2
  )

lazy val plugin = (project in file("plugin"))
  .settings(
    name := "SparkPlugin",
    organization := "com.menis",
    scalaVersion := "2.12.13",
    version      := "0.1.0",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.2" % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.2" % "provided"
  )

lazy val example_3_3_2 = (project in file("example_3_3_2"))
  .settings(
    name := "SparkExample322",
    organization := "com.menis",
    scalaVersion := "2.12.13",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.2",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.2"
  ).dependsOn(plugin)