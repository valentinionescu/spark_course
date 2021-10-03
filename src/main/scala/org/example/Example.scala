package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Example {
  def interactive(spark: SparkSession): Unit = {
    val df = spark.sql("select * from web_logs")
    df.printSchema()
    val df2 = df.groupBy("region_code").count
    df2.show(200,false)
  }
}
