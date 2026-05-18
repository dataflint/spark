package org.apache.spark.dataflint.executor

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CloudMetadataDetectorSpec extends AnyFunSuite with Matchers {

  test("parseJson - parses AWS metadata") {
    val json = """{"cloudProvider":"aws","instanceType":"m5.xlarge","lifecycleType":"spot"}"""
    val result = CloudMetadataDetector.parseJson(json)
    result shouldBe defined
    result.get.cloudProvider shouldBe Some("aws")
    result.get.instanceType shouldBe Some("m5.xlarge")
    result.get.lifecycleType shouldBe Some("spot")
  }

  test("parseJson - parses GCP metadata") {
    val json = """{"cloudProvider":"gcp","instanceType":"n2-standard-4","lifecycleType":"preemptible"}"""
    val result = CloudMetadataDetector.parseJson(json)
    result.get.cloudProvider shouldBe Some("gcp")
    result.get.instanceType shouldBe Some("n2-standard-4")
    result.get.lifecycleType shouldBe Some("preemptible")
  }

  test("parseJson - parses Azure metadata with on-demand lifecycle") {
    val json = """{"cloudProvider":"azure","instanceType":"Standard_D4s_v3","lifecycleType":"on-demand"}"""
    val result = CloudMetadataDetector.parseJson(json)
    result.get.cloudProvider shouldBe Some("azure")
    result.get.instanceType shouldBe Some("Standard_D4s_v3")
    result.get.lifecycleType shouldBe Some("on-demand")
  }

  test("parseJson - handles missing lifecycleType") {
    val json = """{"cloudProvider":"aws","instanceType":"m5.large"}"""
    val result = CloudMetadataDetector.parseJson(json)
    result.get.cloudProvider shouldBe Some("aws")
    result.get.instanceType shouldBe Some("m5.large")
    result.get.lifecycleType shouldBe None
  }

  test("parseJson - filters out empty fields") {
    val json = """{"cloudProvider":"aws","instanceType":"","lifecycleType":"spot"}"""
    val result = CloudMetadataDetector.parseJson(json)
    result.get.instanceType shouldBe None
    result.get.lifecycleType shouldBe Some("spot")
  }

  test("parseJson - returns None for empty object") {
    val result = CloudMetadataDetector.parseJson("{}")
    result shouldBe None
  }

  test("parseJson - returns None for invalid JSON") {
    val result = CloudMetadataDetector.parseJson("not-json")
    result shouldBe None
  }

  test("parseJson - returns CloudMetadata if at least cloudProvider or instanceType set") {
    val json = """{"cloudProvider":"aws"}"""
    val result = CloudMetadataDetector.parseJson(json)
    result shouldBe defined
    result.get.cloudProvider shouldBe Some("aws")
    result.get.instanceType shouldBe None
  }
}