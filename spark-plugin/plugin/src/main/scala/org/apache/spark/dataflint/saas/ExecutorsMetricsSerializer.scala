package org.apache.spark.dataflint.saas

import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.metrics.ExecutorMetricType
import org.json4s.{CustomSerializer, JLong, JNull, JObject}
import org.json4s.JValue

class ExecutorsMetricsSerializer extends CustomSerializer[ExecutorMetrics](implicit format => (
  {
    case json: JValue =>
      val metricsMap = json.extract[Map[String, Long]]
      val metrics =  new ExecutorMetrics(metricsMap)
      metrics
  },
  {
    case Some(metrics: ExecutorMetrics) =>
      val metricsMap = ExecutorMetricType.metricToOffset.map { case (metric, _) =>
        metric -> metrics.getMetricValue(metric)
      }
      val metricsObj = JObject(metricsMap.map { case (k, v) => k -> JLong(v) }.toList)
      metricsObj
    case None => JNull
  }
))
