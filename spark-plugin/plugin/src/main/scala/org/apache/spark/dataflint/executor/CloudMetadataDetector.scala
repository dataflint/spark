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

  private val COMMAND_TIMEOUT_MS = 5000L

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
      case Some("aws") => runCloudCommand(AWS_COMMAND)
      case Some("gcp") => runCloudCommand(GCP_COMMAND)
      case Some("azure") => runCloudCommand(AZURE_COMMAND)
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

  private def runCloudCommand(command: String): CloudMetadata = {
    Try {
      runBashCommand(command).flatMap(parseJson)
    }.getOrElse(None).getOrElse(CloudMetadata(None, None, None))
  }

  private def runBashCommand(command: String): Option[String] = {
    val process = new ProcessBuilder("bash", "-c", command)
      .redirectErrorStream(false)
      .start()

    val completed = process.waitFor(COMMAND_TIMEOUT_MS, TimeUnit.MILLISECONDS)
    if (!completed) {
      process.destroyForcibly()
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

  private def parseJson(json: String): Option[CloudMetadata] = {
    Try {
      val node = mapper.readTree(json)
      val cloudProvider = Option(node.get("cloudProvider")).map(_.asText()).filter(_.nonEmpty)
      val instanceType = Option(node.get("instanceType")).map(_.asText()).filter(_.nonEmpty)
      val lifecycleType = Option(node.get("lifecycleType")).map(_.asText()).filter(_.nonEmpty)
      if (instanceType.isDefined || cloudProvider.isDefined) {
        Some(CloudMetadata(cloudProvider, instanceType, lifecycleType))
      } else {
        None
      }
    }.getOrElse(None)
  }
}