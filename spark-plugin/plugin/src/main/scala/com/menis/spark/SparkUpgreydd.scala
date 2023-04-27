package com.menis.spark

import org.apache.spark.{SparkContext, SparkUILoader}

object SparkUpgreydd {
  def upgrade(context: SparkContext): Unit = {
      SparkUILoader.load(context)
  }
}
