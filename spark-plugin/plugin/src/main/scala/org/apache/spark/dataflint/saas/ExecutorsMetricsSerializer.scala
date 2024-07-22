package org.apache.spark.dataflint.saas

import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.metrics.ExecutorMetricType
import org.json4s.{CustomSerializer, JInt, JNull, JObject}
import org.json4s.JsonAST.JValue

class ExecutorsMetricsSerializer extends CustomSerializer[Option[ExecutorMetrics]](implicit format => (
  {
    case JNull => None
    case json: JValue =>
      val metricsMap = json.extract[Map[String, Long]]
      val metrics =  new ExecutorMetrics(metricsMap)
      Some(metrics)
  },
  {
    case Some(metrics: ExecutorMetrics) =>
      val metricsMap = ExecutorMetricType.metricToOffset.map { case (metric, _) =>
        metric -> metrics.getMetricValue(metric)
      }
      JObject(metricsMap.map { case (k, v) => k -> JInt(v) }.toList)
    case None => JNull
  }
))
