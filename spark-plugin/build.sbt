import xerial.sbt.Sonatype._
import sbtassembly.AssemblyPlugin.autoImport._

lazy val versionNum: String = "0.6.1"
lazy val scala212 = "2.12.20"
lazy val scala213 = "2.13.16"
lazy val supportedScalaVersions = List(scala212, scala213)

lazy val dataflint = project
  .in(file("."))
  .aggregate(
    plugin,
    pluginspark3,
    pluginspark4,
    example_3_1_3,
    example_3_2_4,
    example_3_3_3,
    example_3_4_1,
    example_3_5_1,
    example_3_4_1_remote,
    example_4_0_1
  ).settings(
    crossScalaVersions := Nil, // Aggregate project version must be Nil, see docs: https://www.scala-sbt.org/1.x/docs/Cross-Build.html
    publish / skip := true
  )

lazy val plugin = (project in file("plugin"))
  .settings(
    name := "dataflint-common",
    organization := "io.dataflint",
    crossScalaVersions := supportedScalaVersions,
    version      := (if (git.gitCurrentTags.value.exists(_.startsWith("v"))) {
      versionNum
    } else {
      versionNum + "-SNAPSHOT"
    }),
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.1" % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.1"  % "provided",
    libraryDependencies +=  "com.amazonaws" % "aws-java-sdk-s3" % "1.12.470" % "provided",
    libraryDependencies += "org.apache.iceberg" %% "iceberg-spark-runtime-3.5" % "1.5.0" % "provided",
    libraryDependencies += "io.delta" %% "delta-spark" % "3.2.0" % "provided",
    // Sonatype Central Portal repository configuration
    publishTo := {
      val realm = "Sonatype Nexus Repository Manager"
      if (isSnapshot.value)
        Some(realm at "https://central.sonatype.com/repository/maven-snapshots/")
      else  
        sonatypePublishToBundle.value
    }
  )

lazy val pluginspark3 = (project in file("pluginspark3"))
  .enablePlugins(AssemblyPlugin)
  .settings(
    name := "spark", // Keep the name as "spark" for compatibility with existing versions
    organization := "io.dataflint",
    crossScalaVersions := supportedScalaVersions,
    version      := (if (git.gitCurrentTags.value.exists(_.startsWith("v"))) {
      versionNum
    } else {
      versionNum + "-SNAPSHOT"
    }),
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.1" % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.1"  % "provided",
    libraryDependencies +=  "com.amazonaws" % "aws-java-sdk-s3" % "1.12.470" % "provided",
    libraryDependencies += "org.apache.iceberg" %% "iceberg-spark-runtime-3.5" % "1.5.0" % "provided",
    libraryDependencies += "io.delta" %% "delta-spark" % "3.2.0" % "provided",
    
    // Assembly configuration to create fat JAR with common code
    assembly / assemblyJarName := s"${name.value}_${scalaBinaryVersion.value}-${version.value}.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "application.conf" => MergeStrategy.concat
      case "reference.conf" => MergeStrategy.concat
      case _ => MergeStrategy.first
    },
    
    // Publish the assembled JAR instead of the regular JAR
    Compile / packageBin := assembly.value,
    // Sonatype Central Portal repository configuration
    publishTo := {
      val realm = "Sonatype Nexus Repository Manager"
      if (isSnapshot.value)
        Some(realm at "https://central.sonatype.com/repository/maven-snapshots/")
      else  
        sonatypePublishToBundle.value
    },
    
    // Include source from plugin directory for self-contained build
    Compile / unmanagedSourceDirectories += (plugin / Compile / sourceDirectory).value / "scala",
    
    // Include resources from plugin directory for static UI files
    Compile / unmanagedResourceDirectories += (plugin / Compile / resourceDirectory).value
  )

lazy val pluginspark4 = (project in file("pluginspark4"))
  .enablePlugins(AssemblyPlugin)
  .settings(
    name := "dataflint-spark4",
    organization := "io.dataflint",
    scalaVersion := scala213,
    crossScalaVersions := List(scala213), // Only Scala 2.13 for Spark 4.x
    version      := (if (git.gitCurrentTags.value.exists(_.startsWith("v"))) {
      versionNum
    } else {
      versionNum + "-SNAPSHOT"
    }),
    libraryDependencies += "org.apache.spark" %% "spark-core" % "4.0.1" % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "4.0.1"  % "provided",
    libraryDependencies +=  "com.amazonaws" % "aws-java-sdk-s3" % "1.12.470" % "provided",
    libraryDependencies += "org.apache.iceberg" %% "iceberg-spark-runtime-3.5" % "1.5.0" % "provided",
    libraryDependencies += "io.delta" %% "delta-spark" % "3.2.0" % "provided",
    
    // Assembly configuration to create fat JAR with common code
    assembly / assemblyJarName := s"${name.value}_${scalaBinaryVersion.value}-${version.value}.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "application.conf" => MergeStrategy.concat
      case "reference.conf" => MergeStrategy.concat
      case _ => MergeStrategy.first
    },
    
    // Publish the assembled JAR instead of the regular JAR
    Compile / packageBin := assembly.value,
    // Sonatype Central Portal repository configuration
    publishTo := {
      val realm = "Sonatype Nexus Repository Manager"
      if (isSnapshot.value)
        Some(realm at "https://central.sonatype.com/repository/maven-snapshots/")
      else  
        sonatypePublishToBundle.value
    },
    
    // Include source from plugin directory for self-contained build
    Compile / unmanagedSourceDirectories += (plugin / Compile / sourceDirectory).value / "scala",
    
    // Include resources from plugin directory for static UI files
    Compile / unmanagedResourceDirectories += (plugin / Compile / resourceDirectory).value
  )

lazy val example_3_1_3 = (project in file("example_3_1_3"))
  .settings(
    name := "DataflintSparkExample313",
    organization := "io.dataflint",
    crossScalaVersions := List(scala212),
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.3",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.3",
    publish / skip := true
  ).dependsOn(pluginspark3)

lazy val example_3_2_4 = (project in file("example_3_2_4"))
  .settings(
    name := "DataflintSparkExample324",
    organization := "io.dataflint",
    crossScalaVersions := supportedScalaVersions,
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.4",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.4",
    libraryDependencies +=  "com.amazonaws" % "aws-java-sdk-s3" % "1.12.470",
    publish / skip := true
  ).dependsOn(pluginspark3)

lazy val example_3_3_3 = (project in file("example_3_3_3"))
  .settings(
    name := "DataflintSparkExample333",
    organization := "io.dataflint",
    crossScalaVersions := List(scala212),
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.3",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.3",
    libraryDependencies +=  "com.amazonaws" % "aws-java-sdk-s3" % "1.12.470",
    libraryDependencies += "org.apache.iceberg" %% "iceberg-spark-runtime-3.3" % "1.5.0",
    libraryDependencies += "org.scala-lang.modules" %% "scala-collection-compat" % "2.11.0",
    publish / skip := true
  ).dependsOn(pluginspark3)

lazy val example_3_4_1 = (project in file("example_3_4_1"))
  .settings(
    name := "DataflintSparkExample341",
    organization := "io.dataflint",
    crossScalaVersions := supportedScalaVersions,
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.1",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.1",
    libraryDependencies +=  "com.amazonaws" % "aws-java-sdk-s3" % "1.12.470",
    libraryDependencies += "org.apache.iceberg" %% "iceberg-spark-runtime-3.4" % "1.5.0",
    publish / skip := true
  ).dependsOn(pluginspark3)

lazy val example_3_5_1 = (project in file("example_3_5_1"))
  .settings(
    name := "DataflintSparkExample351",
    organization := "io.dataflint",
    crossScalaVersions := supportedScalaVersions,
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.7",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.7",
    libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.5.1",
    libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.1",
    libraryDependencies +=  "com.amazonaws" % "aws-java-sdk-s3" % "1.12.470",
    libraryDependencies += "io.delta" %% "delta-spark" % "3.2.0",
    libraryDependencies += "org.apache.iceberg" %% "iceberg-spark-runtime-3.5" % "1.5.0",
    libraryDependencies += "org.scala-lang.modules" %% "scala-collection-compat" % "2.11.0",
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.17",
    libraryDependencies += "org.apache.datafusion" % "comet-spark-spark3.5_2.12" % "0.4.0",
    libraryDependencies += "com.h2database" % "h2" % "2.2.224",
    publish / skip := true
  ).dependsOn(pluginspark3)

lazy val example_3_4_1_remote = (project in file("example_3_4_1_remote"))
  .settings(
      name := "DataflintSparkExample341Remote",
      organization := "io.dataflint",
    crossScalaVersions := supportedScalaVersions,
      libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.1",
      libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.1",

    publish / skip := true
  ).dependsOn()

lazy val example_4_0_1 = (project in file("example_4_0_1"))
  .settings(
    name := "DataflintSparkExample401",
    organization := "io.dataflint",
    scalaVersion := scala213,
    crossScalaVersions := List(scala213), // Only Scala 2.13 for Spark 4.x
    // there is no scala 2.12 version so we need to force 2.13 to make it compile
    libraryDependencies += "org.apache.spark" % "spark-core_2.13" % "4.0.1",
    libraryDependencies += "org.apache.spark" % "spark-sql_2.13" % "4.0.1",
    publish / skip := true
  ).dependsOn(pluginspark4)