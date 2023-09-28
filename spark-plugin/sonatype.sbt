credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credentials")

// Your profile name of the sonatype account. The default is the same with the organization value
sonatypeProfileName := "io.dataflint"

// To sync with Maven central, you need to supply the following information:
publishMavenStyle := true

// Open-source license of your choice
licenses := Seq("APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

import xerial.sbt.Sonatype._
sonatypeProjectHosting := Some(GitHubHosting("menishmueli", "dataflint/spark", "menishmueli@gmail.com"))

// or if you want to set these fields manually
homepage := Some(url("http://dataflint.io"))
scmInfo := Some(
  ScmInfo(
    url("https://github.com/dataflint/spark"),
    "scm:git@github.com:dataflint/spark.git"
  )
)
developers := List(
    Developer(
    id = "menishmueli",
    name = "Meni Shmueli",
    email = "menishmueli@gmail.com",
    url = url("http://dataflint.io")
  )
)
