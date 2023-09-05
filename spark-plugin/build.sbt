lazy val root = project
  .in(file("."))
  .aggregate(
    plugin,
    example_3_4_1
  )

lazy val plugin = (project in file("plugin"))
  .settings(
    name := "SparkPlugin",
    organization := "com.anecdota",
    scalaVersion := "2.12.18",
    version      := "0.1.0",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.1" % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.1"  % "provided"
  )

lazy val example_3_4_1 = (project in file("example_3_4_1"))
  .settings(
    name := "SparkExample341",
    organization := "com.anecdota",
    scalaVersion := "2.12.18",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.1" % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.1" % "provided"
).dependsOn(plugin)