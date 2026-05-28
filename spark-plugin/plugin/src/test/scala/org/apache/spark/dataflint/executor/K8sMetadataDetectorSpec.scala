package org.apache.spark.dataflint.executor

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.{File, FileOutputStream}
import java.util

class K8sMetadataDetectorSpec extends AnyFunSuite with Matchers {

  private def confMap(entries: (String, String)*): util.Map[String, String] = {
    val m = new util.HashMap[String, String]()
    entries.foreach { case (k, v) => m.put(k, v) }
    m
  }

  private def writeProps(content: String): util.Properties = {
    val tmp = File.createTempFile("dataflint-k8s-test-", ".properties")
    tmp.deleteOnExit()
    val out = new FileOutputStream(tmp)
    try out.write(content.getBytes("UTF-8")) finally out.close()
    val p = new util.Properties()
    val in = new java.io.FileInputStream(tmp)
    try p.load(in) finally in.close()
    p
  }

  // --- env-var path ---

  test("env vars: all four fields resolved from process env") {
    val conf = confMap(
      "k8s.instanceType.envVar"  -> "NODE_INSTANCE_TYPE",
      "k8s.lifecycleType.envVar" -> "NODE_LIFECYCLE",
      "k8s.cloudProvider.envVar" -> "NODE_CLOUD",
      "k8s.region.envVar"        -> "NODE_REGION"
    )
    val env = Map(
      "NODE_INSTANCE_TYPE" -> "m5.xlarge",
      "NODE_LIFECYCLE"     -> "spot",
      "NODE_CLOUD"         -> "aws",
      "NODE_REGION"        -> "us-east-1"
    )
    val r = K8sMetadataDetector.detectWithLookups(conf, env.get, _ => new util.Properties())
    r.instanceType  shouldBe Some("m5.xlarge")
    r.lifecycleType shouldBe Some("spot")
    r.cloudProvider shouldBe Some("aws")
    r.region        shouldBe Some("us-east-1")
  }

  test("env var configured but value not in env → field stays None") {
    val conf = confMap("k8s.instanceType.envVar" -> "MISSING_VAR")
    val r = K8sMetadataDetector.detectWithLookups(conf, _ => None, _ => new util.Properties())
    r.instanceType shouldBe None
    r.lifecycleType shouldBe None
    r.cloudProvider shouldBe None
    r.region shouldBe None
  }

  test("no configs at all → all None") {
    val r = K8sMetadataDetector.detectWithLookups(confMap(), Map.empty[String, String].get, _ => new util.Properties())
    r.instanceType shouldBe None
    r.lifecycleType shouldBe None
    r.cloudProvider shouldBe None
    r.region shouldBe None
  }

  test("env var empty / missing in env → ignored, no exception") {
    val conf = confMap("k8s.instanceType.envVar" -> "")
    val r = K8sMetadataDetector.detectWithLookups(conf, _ => Some("ignored"), _ => new util.Properties())
    r.instanceType shouldBe None
  }

  // --- file path ---

  test("props file: fields resolved when env var is configured but absent in env") {
    val conf = confMap(
      "k8s.file"                 -> "/some/path",  // path value irrelevant — loader is stubbed
      "k8s.instanceType.envVar"  -> "NODE_INSTANCE_TYPE",
      "k8s.lifecycleType.envVar" -> "NODE_LIFECYCLE",
      "k8s.cloudProvider.envVar" -> "NODE_CLOUD",
      "k8s.region.envVar"        -> "NODE_REGION"
    )
    val props = writeProps(
      """NODE_INSTANCE_TYPE=m5.xlarge
        |NODE_LIFECYCLE=spot
        |NODE_CLOUD=aws
        |NODE_REGION=us-east-1
        |""".stripMargin
    )
    val r = K8sMetadataDetector.detectWithLookups(conf, _ => None, _ => props)
    r.instanceType  shouldBe Some("m5.xlarge")
    r.lifecycleType shouldBe Some("spot")
    r.cloudProvider shouldBe Some("aws")
    r.region        shouldBe Some("us-east-1")
  }

  test("env var wins over props file when both have the same key") {
    val conf = confMap(
      "k8s.file"                -> "/some/path",
      "k8s.instanceType.envVar" -> "NODE_INSTANCE_TYPE"
    )
    val props = writeProps("NODE_INSTANCE_TYPE=from-file\n")
    val env = Map("NODE_INSTANCE_TYPE" -> "from-env")
    val r = K8sMetadataDetector.detectWithLookups(conf, env.get, _ => props)
    r.instanceType shouldBe Some("from-env")
  }

  test("env + file mix: per-field fallthrough") {
    val conf = confMap(
      "k8s.file"                 -> "/some/path",
      "k8s.instanceType.envVar"  -> "NODE_INSTANCE_TYPE",
      "k8s.lifecycleType.envVar" -> "NODE_LIFECYCLE",
      "k8s.region.envVar"        -> "NODE_REGION"
    )
    val props = writeProps(
      """NODE_LIFECYCLE=spot
        |NODE_REGION=us-east-1
        |""".stripMargin
    )
    val env = Map("NODE_INSTANCE_TYPE" -> "m5.xlarge")
    val r = K8sMetadataDetector.detectWithLookups(conf, env.get, _ => props)
    r.instanceType  shouldBe Some("m5.xlarge")  // from env
    r.lifecycleType shouldBe Some("spot")        // from file
    r.region        shouldBe Some("us-east-1")   // from file
    r.cloudProvider shouldBe None                // configured nowhere
  }

  test("file values are trimmed; empty file values are ignored") {
    val conf = confMap(
      "k8s.file"                 -> "/some/path",
      "k8s.instanceType.envVar"  -> "NODE_INSTANCE_TYPE",
      "k8s.region.envVar"        -> "NODE_REGION"
    )
    val props = writeProps(
      """NODE_INSTANCE_TYPE=   m5.xlarge
        |NODE_REGION=
        |""".stripMargin
    )
    val r = K8sMetadataDetector.detectWithLookups(conf, _ => None, _ => props)
    r.instanceType shouldBe Some("m5.xlarge")
    r.region       shouldBe None
  }

  test("k8s.file config unset → file path never consulted; env-only behavior") {
    var called = false
    val conf = confMap("k8s.instanceType.envVar" -> "NODE_INSTANCE_TYPE")
    val env = Map("NODE_INSTANCE_TYPE" -> "m5.xlarge")
    val r = K8sMetadataDetector.detectWithLookups(conf, env.get, _ => { called = true; new util.Properties() })
    r.instanceType shouldBe Some("m5.xlarge")
    called shouldBe false
  }

  test("envVar config not set → field not consulted in file either (no lookup key)") {
    val conf = confMap("k8s.file" -> "/some/path")
    val props = writeProps("NODE_INSTANCE_TYPE=m5.xlarge\n")
    val r = K8sMetadataDetector.detectWithLookups(conf, _ => None, _ => props)
    r.instanceType shouldBe None
  }
}