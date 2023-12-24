
// Your profile name of the sonatype account. The default is the same with the organization value
ThisBuild / sonatypeProfileName := "io.dataflint"

// To sync with Maven central, you need to supply the following information:
ThisBuild / publishMavenStyle := true

// Open-source license of your choice
ThisBuild / licenses := Seq("APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

import xerial.sbt.Sonatype._
ThisBuild / sonatypeProjectHosting := Some(GitHubHosting("menishmueli", "dataflint/spark", "menishmueli@gmail.com"))

ThisBuild / description := "Open Source Data-Application Performance Monitoring for Apache Spark"

// or if you want to set these fields manually
ThisBuild / homepage := Some(url("https://github.com/dataflint/spark"))
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/dataflint/spark"),
    "scm:git@github.com:dataflint/spark.git"
  )
)
ThisBuild / developers := List(
    Developer(
    id = "menishmueli",
    name = "Meni Shmueli",
    email = "menishmueli@gmail.com",
    url = url("http://dataflint.io")
  )
)
