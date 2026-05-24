package org.apache.spark.dataflint.listener

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Pure unit test for the chunking logic — no SparkSession needed. Builds
 * targets of known string size and asserts that DataflintIOListener.chunkTargets
 * never drops a target, never produces an empty chunk, and respects the size
 * budget within one over-the-budget item's worth of slack.
 */
class DataflintIOListenerChunkingSpec extends AnyFunSuite with Matchers {

  private val listener = new DataflintIOListener

  private def target(pathSize: Int, nodeId: Int = 0): DataflintIOTarget =
    DataflintIOTarget(
      nodeId = nodeId,
      operation = "read",
      format = "Parquet",
      paths = Seq("a" * pathSize),
      tableName = None,
      saveMode = None,
      partitionColumns = Seq.empty,
      options = Map.empty
    )

  test("packs many small targets into a single chunk") {
    val targets = (1 to 50).map(_ => target(64))
    val chunks = listener.chunkTargets(targets)
    chunks should have size 1
    chunks.head should have size 50
  }

  test("splits when total estimated bytes exceed the budget") {
    // ~600 KB per target — three should fit in one 1 MB chunk, four should not.
    val targets = (1 to 10).map(_ => target(600 * 1024))
    val chunks = listener.chunkTargets(targets)
    chunks.size should be > 1
    chunks.flatten should contain theSameElementsInOrderAs targets
    // Each chunk respects the budget except possibly a single oversize item alone.
    chunks.foreach { chunk =>
      val sum = chunk.map(listener.estimateBytes).sum
      withClue(s"chunk size $sum, chunk len ${chunk.size}: ") {
        (chunk.size == 1 || sum <= 1024 * 1024) shouldBe true
      }
    }
  }

  test("an oversized target gets its own chunk rather than being dropped") {
    val huge = target(3 * 1024 * 1024)  // ~3 MB single target — bigger than budget
    val chunks = listener.chunkTargets(Seq(huge))
    chunks should have size 1
    chunks.head should have size 1
  }

  test("preserves order of targets across chunks") {
    val targets = (1 to 20).map(i => target(200 * 1024, nodeId = i))
    val chunks = listener.chunkTargets(targets)
    val nodeIds = chunks.flatten.map(_.nodeId)
    nodeIds should contain theSameElementsInOrderAs (1 to 20)
  }

  test("empty input produces empty output") {
    listener.chunkTargets(Seq.empty) shouldBe empty
  }
}
