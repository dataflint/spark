package com.anecdota.spark

import org.apache.spark.{SparkContext, SparkUILoader}

object SparkAnecdota {
  def upgrade(context: SparkContext): Unit = {
      SparkUILoader.load(context)
  }
}
