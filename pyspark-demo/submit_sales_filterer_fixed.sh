spark-submit \
--packages io.dataflint:spark_2.12:0.0.3-SNAPSHOT \
--repositories https://s01.oss.sonatype.org/content/repositories/snapshots \
--conf spark.plugins=io.dataflint.spark.SparkDataflintPlugin \
/Users/menishmueli/Documents/GitHub/dataflint/spark/pyspark-demo/sales_filterer_fixed.py