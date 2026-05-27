package org.apache.spark.dataflint.executor

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import java.io.BufferedReader
import java.io.InputStreamReader
import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit
import scala.util.Try

object CloudMetadataDetector {

  case class CloudMetadata(
    cloudProvider: Option[String],
    instanceType: Option[String],
    lifecycleType: Option[String],
    region: Option[String]
  )

  // Worst case: AWS does 3 IMDS calls (token + identity doc + lifecycle), each --max-time 2s.
  // Give the JVM-level wait some headroom above that.
  private val COMMAND_TIMEOUT_MS = 10000L

  private val SYS_VENDOR_PATH = "/sys/class/dmi/id/sys_vendor"

  // AWS exposes the instance-identity document (instanceType + region in one JSON payload),
  // but lifecycle lives at a separate path. Concatenate both into a single envelope so the
  // shell side stays simple — Scala parses it.
  private val AWS_COMMAND =
    """TOKEN=$(curl -sf -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" --connect-timeout 1 --max-time 2 2>/dev/null)
      |if [ -n "$TOKEN" ]; then HDR="X-aws-ec2-metadata-token: $TOKEN"; else HDR="X-aws-ec2-metadata-token: "; fi
      |ID=$(curl -sf -H "$HDR" "http://169.254.169.254/latest/dynamic/instance-identity/document" --connect-timeout 1 --max-time 2 2>/dev/null)
      |if [ -z "$ID" ]; then exit 1; fi
      |LC=$(curl -sf -H "$HDR" "http://169.254.169.254/latest/meta-data/instance-life-cycle" --connect-timeout 1 --max-time 2 2>/dev/null)
      |printf '{"identity":%s,"lifecycle":"%s"}' "$ID" "$LC"
      |""".stripMargin

  // Single recursive call returns the entire instance metadata tree as JSON.
  private val GCP_COMMAND =
    """curl -sf -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/?recursive=true&alt=json" --connect-timeout 1 --max-time 2 2>/dev/null"""

  // Single call returns the full Azure instance metadata document (compute + network + ...).
  private val AZURE_COMMAND =
    """curl -sf -H "Metadata: true" "http://169.254.169.254/metadata/instance?api-version=2021-02-01" --connect-timeout 1 --max-time 2 2>/dev/null"""

  private val mapper = new ObjectMapper()

  def detect(): CloudMetadata = {
    detectCloudProvider() match {
      case provider @ Some("aws")   => runProvider(AWS_COMMAND, provider, parseAws)
      case provider @ Some("gcp")   => runProvider(GCP_COMMAND, provider, parseGcp)
      case provider @ Some("azure") => runProvider(AZURE_COMMAND, provider, parseAzure)
      case _                        => CloudMetadata(None, None, None, None)
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

  // Preserve the DMI-detected provider when the metadata endpoint fails so consumers can
  // distinguish "detected but blocked/timed out" from "not on cloud".
  private def runProvider(
    command: String,
    detectedProvider: Option[String],
    parse: String => Option[CloudMetadata]
  ): CloudMetadata = {
    val fallback = CloudMetadata(detectedProvider, None, None, None)
    Try(runBashCommand(command).flatMap(parse)).getOrElse(None).getOrElse(fallback)
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

    if (process.exitValue() != 0) return None

    val reader = new BufferedReader(new InputStreamReader(process.getInputStream))
    try {
      val sb = new StringBuilder
      var line = reader.readLine()
      while (line != null) {
        sb.append(line)
        line = reader.readLine()
      }
      val out = sb.toString.trim
      if (out.nonEmpty) Some(out) else None
    } finally {
      reader.close()
    }
  }

  // Skip JSON null values — Jackson NullNode.asText() returns the literal string "null",
  // which would otherwise propagate as instanceType="null" downstream.
  private def fieldText(node: JsonNode, field: String): Option[String] =
    Option(node.get(field)).filterNot(_.isNull).map(_.asText()).filter(_.nonEmpty)

  // The AWS instance-identity document includes `instanceType`, `region`, `availabilityZone`
  // and more; we wrap it together with the lifecycle string into a single envelope on the
  // shell side. An empty lifecycle is normal on non-spot instances.
  private[executor] def parseAws(raw: String): Option[CloudMetadata] = Try {
    val node = mapper.readTree(raw)
    val identity = node.path("identity")
    val instanceType = fieldText(identity, "instanceType")
    val region = fieldText(identity, "region")
    val lifecycle = fieldText(node, "lifecycle")
    if (instanceType.isDefined || region.isDefined)
      Some(CloudMetadata(Some("aws"), instanceType, lifecycle, region))
    else None
  }.toOption.flatten

  // GCP recursive returns the full instance object. `machineType` and `zone` come back as
  // fully-qualified resource paths (e.g. `projects/.../machineTypes/n2-standard-4`), strip
  // to the last segment. `region` is the zone with the trailing `-<az>` removed.
  private[executor] def parseGcp(raw: String): Option[CloudMetadata] = Try {
    val node = mapper.readTree(raw)
    val instanceType = fieldText(node, "machineType").map(_.split("/").last).filter(_.nonEmpty)
    val zone = fieldText(node, "zone").map(_.split("/").last).filter(_.nonEmpty)
    val region = zone.map(_.replaceAll("-[a-z]$", "")).filter(_.nonEmpty)
    // GCP's recursive response encodes booleans as uppercase strings ("TRUE"/"FALSE"), and
    // Jackson's `asBoolean()` only recognises lowercase. Handle both shapes explicitly.
    val preemptible = Option(node.get("scheduling"))
      .filterNot(_.isNull)
      .flatMap(s => Option(s.get("preemptible")).filterNot(_.isNull)).map { n =>
        if (n.isBoolean) n.asBoolean()
        else n.asText().equalsIgnoreCase("true")
      }
    val lifecycle = preemptible.map(if (_) "preemptible" else "on-demand")
    if (instanceType.isDefined || region.isDefined)
      Some(CloudMetadata(Some("gcp"), instanceType, lifecycle, region))
    else None
  }.toOption.flatten

  // Azure response has `compute.{vmSize, location, priority, ...}`.
  private[executor] def parseAzure(raw: String): Option[CloudMetadata] = Try {
    val node = mapper.readTree(raw)
    val compute = node.path("compute")
    val instanceType = fieldText(compute, "vmSize")
    val region = fieldText(compute, "location")
    val priority = fieldText(compute, "priority")
    val lifecycle = priority.map(p => if (p.equalsIgnoreCase("Spot")) "spot" else "on-demand")
    if (instanceType.isDefined || region.isDefined)
      Some(CloudMetadata(Some("azure"), instanceType, lifecycle, region))
    else None
  }.toOption.flatten
}