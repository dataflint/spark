package org.apache.spark.dataflint.executor

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CloudMetadataDetectorSpec extends AnyFunSuite with Matchers {

  // --- AWS ---

  test("parseAws - extracts instanceType + region + lifecycle from the envelope") {
    val json =
      """{"identity":{"instanceType":"m5.xlarge","region":"us-east-1","availabilityZone":"us-east-1a"},
        | "lifecycle":"spot"}""".stripMargin
    val r = CloudMetadataDetector.parseAws(json)
    r shouldBe defined
    r.get.cloudProvider shouldBe Some("aws")
    r.get.instanceType  shouldBe Some("m5.xlarge")
    r.get.lifecycleType shouldBe Some("spot")
    r.get.region        shouldBe Some("us-east-1")
  }

  test("parseAws - handles missing lifecycle (on-demand instances return empty string)") {
    val json = """{"identity":{"instanceType":"m5.large","region":"eu-west-1"},"lifecycle":""}"""
    val r = CloudMetadataDetector.parseAws(json).get
    r.instanceType  shouldBe Some("m5.large")
    r.region        shouldBe Some("eu-west-1")
    r.lifecycleType shouldBe None
  }

  test("parseAws - returns None when both instanceType and region are absent") {
    CloudMetadataDetector.parseAws("""{"identity":{},"lifecycle":"spot"}""") shouldBe None
  }

  test("parseAws - returns None on invalid JSON") {
    CloudMetadataDetector.parseAws("not-json") shouldBe None
  }

  test("parseAws - real IMDS envelope (captured from EKS m6g node)") {
    // Verbatim from /latest/dynamic/instance-identity/document and /latest/meta-data/instance-life-cycle
    // on a running prod EKS node. Regression guard against AWS adding/changing fields.
    val json =
      """{"identity":{
        |  "accountId" : "381492059942",
        |  "architecture" : "arm64",
        |  "availabilityZone" : "us-east-1b",
        |  "billingProducts" : null,
        |  "devpayProductCodes" : null,
        |  "marketplaceProductCodes" : null,
        |  "imageId" : "ami-02d72bffeff892341",
        |  "instanceId" : "i-0b750ff1177040959",
        |  "instanceType" : "m6g.xlarge",
        |  "kernelId" : null,
        |  "pendingTime" : "2026-05-23T08:39:32Z",
        |  "privateIp" : "10.0.4.202",
        |  "ramdiskId" : null,
        |  "region" : "us-east-1",
        |  "version" : "2017-09-30"
        |},"lifecycle":"on-demand"}""".stripMargin
    val r = CloudMetadataDetector.parseAws(json).get
    r.cloudProvider shouldBe Some("aws")
    r.instanceType  shouldBe Some("m6g.xlarge")
    r.region        shouldBe Some("us-east-1")
    r.lifecycleType shouldBe Some("on-demand")
  }

  // --- GCP ---

  test("parseGcp - strips path prefix from machineType and zone, derives region") {
    val json =
      """{"machineType":"projects/123/machineTypes/n2-standard-4",
        | "zone":"projects/123/zones/us-central1-a",
        | "scheduling":{"preemptible":true}}""".stripMargin
    val r = CloudMetadataDetector.parseGcp(json).get
    r.cloudProvider shouldBe Some("gcp")
    r.instanceType  shouldBe Some("n2-standard-4")
    r.region        shouldBe Some("us-central1")
    r.lifecycleType shouldBe Some("preemptible")
  }

  test("parseGcp - non-preemptible instance reports lifecycle on-demand") {
    val json =
      """{"machineType":"projects/x/machineTypes/e2-medium",
        | "zone":"projects/x/zones/europe-west4-b",
        | "scheduling":{"preemptible":false}}""".stripMargin
    val r = CloudMetadataDetector.parseGcp(json).get
    r.instanceType  shouldBe Some("e2-medium")
    r.region        shouldBe Some("europe-west4")
    r.lifecycleType shouldBe Some("on-demand")
  }

  test("parseGcp - handles missing scheduling block") {
    val json =
      """{"machineType":"projects/x/machineTypes/n1-standard-1",
        | "zone":"projects/x/zones/us-west1-a"}""".stripMargin
    val r = CloudMetadataDetector.parseGcp(json).get
    r.lifecycleType shouldBe None
  }

  test("parseGcp - returns None when neither machineType nor zone are usable") {
    CloudMetadataDetector.parseGcp("""{}""") shouldBe None
  }

  test("parseGcp - real recursive response (captured from us-central1-a Spot VM)") {
    // Verbatim slice of /computeMetadata/v1/instance/?recursive=true&alt=json on a real e2-micro
    // Spot VM. Note: scheduling.preemptible is a STRING ("TRUE") in the recursive response,
    // not a boolean — Jackson's asBoolean() handles it via Boolean.parseBoolean.
    val json =
      """{"attributes":{},"cpuPlatform":"AMD Rome","description":"",
        | "hostname":"dataflint-imds-probe.us-central1-a.c.dataflint-dataproc-simulator.internal",
        | "id":8124571877738488600,
        | "image":"projects/debian-cloud/global/images/debian-12-bookworm-v20260513",
        | "machineType":"projects/723643627235/machineTypes/e2-micro",
        | "maintenanceEvent":"NONE",
        | "name":"dataflint-imds-probe",
        | "preempted":"FALSE",
        | "scheduling":{"automaticRestart":"FALSE","onHostMaintenance":"TERMINATE","preemptible":"TRUE"},
        | "zone":"projects/723643627235/zones/us-central1-a"}""".stripMargin
    val r = CloudMetadataDetector.parseGcp(json).get
    r.cloudProvider shouldBe Some("gcp")
    r.instanceType  shouldBe Some("e2-micro")
    r.region        shouldBe Some("us-central1")
    r.lifecycleType shouldBe Some("preemptible")
  }

  // --- Azure ---

  test("parseAzure - extracts vmSize / location / priority from compute block") {
    val json =
      """{"compute":{"vmSize":"Standard_D4s_v3","location":"eastus","priority":"Spot"},
        | "network":{}}""".stripMargin
    val r = CloudMetadataDetector.parseAzure(json).get
    r.cloudProvider shouldBe Some("azure")
    r.instanceType  shouldBe Some("Standard_D4s_v3")
    r.region        shouldBe Some("eastus")
    r.lifecycleType shouldBe Some("spot")
  }

  test("parseAzure - regular priority maps to on-demand") {
    val json = """{"compute":{"vmSize":"Standard_D2s_v3","location":"westeurope","priority":"Regular"}}"""
    CloudMetadataDetector.parseAzure(json).get.lifecycleType shouldBe Some("on-demand")
  }

  test("parseAzure - missing priority leaves lifecycle empty") {
    val json = """{"compute":{"vmSize":"Standard_B2s","location":"centralus"}}"""
    CloudMetadataDetector.parseAzure(json).get.lifecycleType shouldBe None
  }

  test("parseAzure - returns None when compute block is empty") {
    CloudMetadataDetector.parseAzure("""{"compute":{}}""") shouldBe None
  }

  test("parseAzure - filters JSON null values (Jackson NullNode.asText == \"null\")") {
    val json = """{"compute":{"vmSize":"Standard_D2s_v3","location":null,"priority":null}}"""
    val r = CloudMetadataDetector.parseAzure(json).get
    r.instanceType  shouldBe Some("Standard_D2s_v3")
    r.region        shouldBe None
    r.lifecycleType shouldBe None
  }
}