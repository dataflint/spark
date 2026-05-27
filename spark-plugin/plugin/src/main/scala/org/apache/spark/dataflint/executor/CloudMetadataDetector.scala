package org.apache.spark.dataflint.executor

import com.fasterxml.jackson.databind.ObjectMapper

import java.io.BufferedReader
import java.io.InputStreamReader
import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit
import scala.util.Try

object CloudMetadataDetector {

  case class CloudMetadata(
    cloudProvider: Option[String],
    instanceType: Option[String],
    lifecycleType: Option[String]
  )

  // AWS detection makes up to 3 curl calls (token + instance-type + lifecycle), each with --max-time 2s,
  // so worst case is ~6s. Keep a safety margin above that.
  private val COMMAND_TIMEOUT_MS = 10000L

  private val SYS_VENDOR_PATH = "/sys/class/dmi/id/sys_vendor"

  private val AWS_COMMAND =
    """TOKEN=$(curl -sf -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" --connect-timeout 1 --max-time 2 2>/dev/null)
      |if [ -n "$TOKEN" ]; then
      |  HEADER="-H X-aws-ec2-metadata-token:$TOKEN"
      |else
      |  HEADER=""
      |fi
      |IT=$(curl -sf $HEADER "http://169.254.169.254/latest/meta-data/instance-type" --connect-timeout 1 --max-time 2 2>/dev/null)
      |if [ -z "$IT" ]; then exit 1; fi
      |LC=$(curl -sf $HEADER "http://169.254.169.254/latest/meta-data/instance-life-cycle" --connect-timeout 1 --max-time 2 2>/dev/null)
      |echo "{\"cloudProvider\":\"aws\",\"instanceType\":\"$IT\",\"lifecycleType\":\"$LC\"}"
      |""".stripMargin

  private val GCP_COMMAND =
    """MT=$(curl -sf -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/machine-type" --connect-timeout 1 --max-time 2 2>/dev/null)
      |if [ -z "$MT" ]; then exit 1; fi
      |IT=$(echo "$MT" | rev | cut -d/ -f1 | rev)
      |PR=$(curl -sf -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/scheduling/preemptible" --connect-timeout 1 --max-time 2 2>/dev/null)
      |if [ "$PR" = "TRUE" ]; then LC="preemptible"; else LC="on-demand"; fi
      |echo "{\"cloudProvider\":\"gcp\",\"instanceType\":\"$IT\",\"lifecycleType\":\"$LC\"}"
      |""".stripMargin

  private val AZURE_COMMAND =
    """VS=$(curl -sf -H "Metadata: true" "http://169.254.169.254/metadata/instance/compute/vmSize?api-version=2021-02-01&format=text" --connect-timeout 1 --max-time 2 2>/dev/null)
      |if [ -z "$VS" ]; then exit 1; fi
      |PR=$(curl -sf -H "Metadata: true" "http://169.254.169.254/metadata/instance/compute/priority?api-version=2021-02-01&format=text" --connect-timeout 1 --max-time 2 2>/dev/null)
      |if [ "$PR" = "Spot" ]; then LC="spot"; else LC="on-demand"; fi
      |echo "{\"cloudProvider\":\"azure\",\"instanceType\":\"$VS\",\"lifecycleType\":\"$LC\"}"
      |""".stripMargin

  private val mapper = new ObjectMapper()

  def detect(): CloudMetadata = {
    detectCloudProvider() match {
      case provider @ Some("aws") => runCloudCommand(AWS_COMMAND, provider)
      case provider @ Some("gcp") => runCloudCommand(GCP_COMMAND, provider)
      case provider @ Some("azure") => runCloudCommand(AZURE_COMMAND, provider)
      case _ => CloudMetadata(None, None, None)
    }
  }

  private def detectCloudProvider(): Option[String] = {
    Try {
      val vendor = new String(Files.readAllBytes(Paths.get(SYS_VENDOR_PATH))).trim
      if (vendor.contains("Amazon")) Some("aws")
      else if (vendor.contains("Google")) Some("gcp")
      else if (vendor.contains("Microsoft")) Some("azure")
      else None
    }.getOrElse(None)
  }

  // When the metadata command fails, preserve the DMI-detected provider so consumers can
  // distinguish "detected provider but metadata endpoint blocked/timed out" from "not on cloud".
  private def runCloudCommand(command: String, detectedProvider: Option[String]): CloudMetadata = {
    val fallback = CloudMetadata(detectedProvider, None, None)
    Try {
      runBashCommand(command).flatMap(parseJson)
    }.getOrElse(None).getOrElse(fallback)
  }

  // Use `sh` rather than `bash` — the scripts above are POSIX-only and minimal / distroless
  // container images often ship without bash.
  private def runBashCommand(command: String): Option[String] = {
    val process = new ProcessBuilder("sh", "-c", command)
      .redirectErrorStream(false)
      .start()

    val completed = process.waitFor(COMMAND_TIMEOUT_MS, TimeUnit.MILLISECONDS)
    if (!completed) {
      process.destroyForcibly()
      // Brief blocking wait so a forcibly-killed shell can reap before we move on; without
      // this the process can linger as a zombie under some JVM/OS combinations.
      process.waitFor(500, TimeUnit.MILLISECONDS)
      return None
    }

    if (process.exitValue() != 0) {
      return None
    }

    val reader = new BufferedReader(new InputStreamReader(process.getInputStream))
    try {
      val output = reader.readLine()
      if (output != null && output.nonEmpty) Some(output.trim) else None
    } finally {
      reader.close()
    }
  }

  private[executor] def parseJson(json: String): Option[CloudMetadata] = {
    // Skip JSON null values — Jackson NullNode.asText() returns the literal string "null",
    // which would otherwise show up as instanceType="null" downstream.
    def fieldText(node: com.fasterxml.jackson.databind.JsonNode, field: String): Option[String] =
      Option(node.get(field)).filterNot(_.isNull).map(_.asText()).filter(_.nonEmpty)

    Try {
      val node = mapper.readTree(json)
      val cloudProvider = fieldText(node, "cloudProvider")
      val instanceType = fieldText(node, "instanceType")
      val lifecycleType = fieldText(node, "lifecycleType")
      if (instanceType.isDefined || cloudProvider.isDefined) {
        Some(CloudMetadata(cloudProvider, instanceType, lifecycleType))
      } else {
        None
      }
    }.getOrElse(None)
  }
}