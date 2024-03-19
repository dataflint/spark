package org.apache.spark.dataflint.saas

import org.apache.spark.JobExecutionStatus
import org.apache.spark.rdd.DeterministicLevel
import org.apache.spark.status.api.v1.StageStatus
import org.json4s.jackson.{JsonMethods, Serialization}
import org.json4s.{Formats, NoTypeHints}

import java.io.{File, PrintWriter}

object SparkMetadataSerializer {
  implicit val formats: Formats = Serialization.formats(NoTypeHints) + new JavaEnumNameSerializer[JobExecutionStatus]() + new JavaEnumNameSerializer[StageStatus]() + new EnumSerializer(DeterministicLevel)

  def serialize(data: SparkMetadataStore): String = {
    Serialization.write(data)
  }

  def deserialize(json: String): SparkMetadataStore = {
    JsonMethods.parse(json).extract[SparkMetadataStore]
  }

  def serializeAndSave(data: SparkMetadataStore, filePath: String): Unit = {
    val jsonData = serialize(data)
    val writer = new PrintWriter(new File(filePath))
    try {
      writer.write(jsonData)
    } finally {
      writer.close()
    }
  }
}
