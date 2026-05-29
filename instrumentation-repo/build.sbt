import sbtassembly.AssemblyPlugin.autoImport._

// ---------------------------------------------------------------------------
// dataflint-instrumentation
//
// SQL-plan instrumentation for DataFlint, extracted from dataflint/spark.
// Depends on the published `io.dataflint:dataflint-common` artifact (Maven
// Central) for the INSTRUMENT_* config contract and api.ResolvedStageGroup,
// and is wired into Spark at runtime by dataflint-common's
// `registerInstrumentationExtension` (which loads
// `org.apache.spark.dataflint.DataFlintInstrumentationExtension` by name).
//
// Like the upstream dataflint/spark build, the version-portable sources in
// `core/src/main/scala` are shared into each version module via
// `unmanagedSourceDirectories` rather than a cross-built project dependency
// (a Scala-2.13-only module can't `dependsOn` a 2.12/2.13 cross-built one).
//
// Artifacts are published to GitHub Packages:
//   https://maven.pkg.github.com/dataflint/spark-instrumentation
// ---------------------------------------------------------------------------

lazy val versionNum: String = "0.1.0"

lazy val scala212 = "2.12.20"
lazy val scala213 = "2.13.16"
lazy val supportedScalaVersions = List(scala212, scala213)

// Version of the upstream dataflint-common artifact that ships the INSTRUMENT_*
// constants + registerInstrumentationExtension + api.ResolvedStageGroup.
// Must be a *released* version on Maven Central (not a -SNAPSHOT).
//
// 0.9.9 is the latest release at extraction time and already carries the
// contract — but it STILL bundles TimedExec/Accelerator. Bump this to the
// first dataflint/spark release cut *after* this extraction (>= 0.9.10), which
// drops those classes, so the plugin jar and this jar don't both carry them.
lazy val dataflintCommonVersion = "0.9.9"

ThisBuild / organization := "io.dataflint"
ThisBuild / version := versionNum

// ---- GitHub Packages publishing ----------------------------------------------
ThisBuild / publishMavenStyle := true
ThisBuild / licenses := Seq("APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / homepage := Some(url("https://github.com/dataflint/spark-instrumentation"))
ThisBuild / publishTo := Some(
  "GitHub Packages" at "https://maven.pkg.github.com/dataflint/spark-instrumentation"
)
// In GitHub Actions, GITHUB_ACTOR + GITHUB_TOKEN are provided automatically.
// Locally, export GITHUB_ACTOR and a PAT with write:packages as GITHUB_TOKEN.
ThisBuild / credentials += Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  sys.env.getOrElse("GITHUB_ACTOR", "dataflint"),
  sys.env.getOrElse("GITHUB_TOKEN", "")
)

// Shared assembly config: publish a fat JAR containing the instrumentation
// core + the version-specific extension. Scala and the provided deps
// (Spark, dataflint-common) are excluded — they are already on the user's
// classpath via Spark and the DataFlint plugin jar.
lazy val instrumentationAssembly = Seq(
  assembly / assemblyJarName := s"${name.value}_${scalaBinaryVersion.value}-${version.value}.jar",
  assembly / assemblyOption := (assembly / assemblyOption).value.withIncludeScala(false),
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
    case PathList("META-INF", xs @ _*)             => MergeStrategy.discard
    case "application.conf"                         => MergeStrategy.concat
    case "reference.conf"                           => MergeStrategy.concat
    case _                                          => MergeStrategy.first
  },
  // Publish the assembled JAR instead of the thin one.
  Compile / packageBin := assembly.value,
  // Bundle the shared, version-portable core sources (TimedExec, MetricsUtils,
  // Accelerator, GraphDurationAttribution) into this module's compile/jar.
  Compile / unmanagedSourceDirectories += (LocalRootProject / baseDirectory).value / "core" / "src" / "main" / "scala"
)

lazy val sparkTestJavaOptions = Seq(
  Test / fork := true,
  // Suites share a SparkSession via getOrCreate(); run sequentially so one
  // suite stopping the session doesn't NPE another.
  Test / parallelExecution := false,
  Test / javaOptions ++= {
    if (sys.props("java.specification.version").startsWith("1.")) Seq.empty
    else Seq(
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED"
    )
  }
)

lazy val root = (project in file("."))
  .aggregate(spark3, spark4)
  .settings(
    name := "dataflint-instrumentation",
    crossScalaVersions := Nil, // aggregate must be Nil for cross-building
    publish / skip := true
  )

// Spark 3.x SparkSessionExtensions + ColumnarRule, plus the full regression suite.
lazy val spark3 = (project in file("spark3"))
  .enablePlugins(AssemblyPlugin)
  .settings(instrumentationAssembly)
  .settings(sparkTestJavaOptions)
  .settings(
    name := "dataflint-instrumentation-spark3",
    crossScalaVersions := supportedScalaVersions,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core"       % "3.5.1"                 % "provided",
      "org.apache.spark" %% "spark-sql"        % "3.5.1"                 % "provided",
      "io.dataflint"     %% "dataflint-common" % dataflintCommonVersion  % "provided",
      // tests
      "org.scalatest"    %% "scalatest-funsuite"       % "3.2.17"        % Test,
      "org.scalatest"    %% "scalatest-shouldmatchers" % "3.2.17"        % Test,
      "org.apache.spark" %% "spark-core"       % "3.5.1"                 % Test,
      "org.apache.spark" %% "spark-sql"        % "3.5.1"                 % Test,
      "io.dataflint"     %% "dataflint-common" % dataflintCommonVersion  % Test
    )
  )

// Spark 4.x SparkSessionExtensions + ColumnarRule (Scala 2.13 only).
lazy val spark4 = (project in file("spark4"))
  .enablePlugins(AssemblyPlugin)
  .settings(instrumentationAssembly)
  .settings(
    name := "dataflint-instrumentation-spark4",
    scalaVersion := scala213,
    crossScalaVersions := List(scala213),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core"       % "4.0.1"                 % "provided",
      "org.apache.spark" %% "spark-sql"        % "4.0.1"                 % "provided",
      "io.dataflint"     %% "dataflint-common" % dataflintCommonVersion  % "provided"
    )
  )
