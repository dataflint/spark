package com.upgraydd.spark

import org.apache.spark.{SparkContext, SparkUILoader}

object SparkUpgraydd {
  def upgrade(context: SparkContext): Unit = {
      SparkUILoader.load(context)
  }
}
