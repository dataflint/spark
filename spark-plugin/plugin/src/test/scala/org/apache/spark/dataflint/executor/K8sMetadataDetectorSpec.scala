package org.apache.spark.dataflint.executor

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util

class K8sMetadataDetectorSpec extends AnyFunSuite with Matchers {

  private def confMap(entries: (String, String)*): util.Map[String, String] = {
    val m = new util.HashMap[String, String]()
    entries.foreach { case (k, v) => m.put(k, v) }
    m
  }

  test("detectWithEnv - reads metadata from configured env vars") {
    val conf = confMap(
      "k8s.instanceType.envVar" -> "NODE_INSTANCE_TYPE",
      "k8s.lifecycleType.envVar" -> "NODE_LIFECYCLE",
      "k8s.cloudProvider.envVar" -> "NODE_CLOUD"
    )
    val env = Map(
      "NODE_INSTANCE_TYPE" -> "m5.xlarge",
      "NODE_LIFECYCLE" -> "spot",
      "NODE_CLOUD" -> "aws"
    )
    val result = K8sMetadataDetector.detectWithEnv(conf, env.get)
    result.instanceType shouldBe Some("m5.xlarge")
    result.lifecycleType shouldBe Some("spot")
    result.cloudProvider shouldBe Some("aws")
  }

  test("detectWithEnv - returns None for fields when env var is configured but not set") {
    val conf = confMap("k8s.instanceType.envVar" -> "MISSING_VAR")
    val result = K8sMetadataDetector.detectWithEnv(conf, _ => None)
    result.instanceType shouldBe None
    result.lifecycleType shouldBe None
    result.cloudProvider shouldBe None
  }

  test("detectWithEnv - returns None for fields when config key is not set") {
    val conf = confMap()
    val env = Map("ANY_VAR" -> "value")
    val result = K8sMetadataDetector.detectWithEnv(conf, env.get)
    result.instanceType shouldBe None
    result.lifecycleType shouldBe None
    result.cloudProvider shouldBe None
  }

  test("detectWithEnv - filters empty env var values") {
    val conf = confMap("k8s.instanceType.envVar" -> "NODE_INSTANCE_TYPE")
    val env = Map("NODE_INSTANCE_TYPE" -> "")
    val result = K8sMetadataDetector.detectWithEnv(conf, env.get)
    result.instanceType shouldBe None
  }

  test("detectWithEnv - filters empty config values") {
    val conf = confMap("k8s.instanceType.envVar" -> "")
    val env = Map("" -> "value")
    val result = K8sMetadataDetector.detectWithEnv(conf, env.get)
    result.instanceType shouldBe None
  }

  test("detectWithEnv - reads only configured fields, leaves others as None") {
    val conf = confMap("k8s.instanceType.envVar" -> "NODE_INSTANCE_TYPE")
    val env = Map(
      "NODE_INSTANCE_TYPE" -> "n2-standard-4",
      "NODE_LIFECYCLE" -> "preemptible"
    )
    val result = K8sMetadataDetector.detectWithEnv(conf, env.get)
    result.instanceType shouldBe Some("n2-standard-4")
    result.lifecycleType shouldBe None
    result.cloudProvider shouldBe None
  }
}