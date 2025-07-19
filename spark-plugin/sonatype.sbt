
import xerial.sbt.Sonatype._

ThisBuild / sonatypeCredentialHost := "central.sonatype.com"

sonatypeProfileName := "io.dataflint"

ThisBuild / sonatypeProfileName := "io.dataflint"

ThisBuild / publishMavenStyle := true

ThisBuild / licenses := Seq("APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

ThisBuild / sonatypeProjectHosting := Some(GitHubHosting("menishmueli", "dataflint/spark", "menishmueli@gmail.com"))

ThisBuild / description := "Open Source Data-Application Performance Monitoring for Apache Spark"

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
