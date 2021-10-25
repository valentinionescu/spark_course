package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, concat, sha2}


object Example {
  def interactive(spark: SparkSession): Unit = {
//For Hive3
/*
show create table web_logs

CREATE external TABLE `web_logs_nt`(
`_version_` bigint,
`app` string,
`bytes` int,
`city` string,
`client_ip` string,
`code` smallint,
`country_code` string,
`country_code3` string,
`country_name` string,
`device_family` string,
`extension` string,
`latitude` float,
`longitude` float,
`method` string,
`os_family` string,
`os_major` string,
`protocol` string,
`record` string,
`referer` string,
`region_code` string,
`request` string,
`subapp` string,
`time` string,
`url` string,
`user_agent` string,
`user_agent_family` string,
`user_agent_major` string,
`id` string,
 `date` string)
stored as parquet
LOCATION
'hdfs://vi-spark3-1.gce.cloudera.com:8020/user/hdfs/web_logs_nt'

insert overwrite table web_logs_nt  select * from web_logs

*/

    //4.2. Spark UI / History Server
    val df = spark.sql("select * from web_logs_nt")
    df.printSchema()
    val df2 = df.groupBy("region_code").count
    df2.show(200,false)
    df.write.mode("overwrite").parquet("web_logs")

    //5.4 Loading Spark Partitions

    def gen(name:String, start:Int, end:Int, numPartitions:Int, blockSize:Int=128, noHash:Boolean=false, mode:String="append")={
      val df3 = spark.read.parquet("web_logs")
      import spark.implicits._
      import scala.collection.immutable
      val df4 = immutable.List.range(start, end+1).toDF
      val df5 = df3.crossJoin(df4)
      val df6 = df5.withColumn("id2", concat($"value", $"_version_"))
      import org.apache.spark.sql.functions._
      var df7 = df6
      if (!noHash)
        df7 = df6.withColumn("myhash", sha2($"id2", 512))

      spark.conf.set("parquet.block.size", blockSize * 1024 * 1024)
      var df8 = df7
      if(numPartitions!=0 && df7.rdd.getNumPartitions != numPartitions)
        df8 = df7.repartition(numPartitions)
      df8.write.mode(mode).parquet(name)
      df8
    }
    gen("example_128m_string_join2",1,1000,1)

    gen("example_128m_nohash",1,1000,1, noHash=true, mode="overwrite")
    gen("example_128m_nohash",1001,2000,10, noHash=true)
    gen("example_128m_nohash",2001,3000,100, noHash=true)
    gen("example_128m_bin",1,1000,1,mode="overwrite")
    gen("example_128m_bin",1001,2000,10)
    gen("example_128m_bin",2001,3000,100)


    gen("example_8m_string",1001,2000,10, blockSize=8)
    gen("example_8m_string",2001,3000,100, blockSize=8)
    gen("example_8m_string_big",3001,10000,7, blockSize=8)

    spark.conf.set("spark.sql.files.maxPartitionBytes", 8 * math.pow(1024, 2).toInt)
    val df6 = spark.read.parquet("example_8m_string").repartition(2)
    import org.apache.spark.sql.functions._
    //execute
    val df3 = spark.read.parquet("web_logs")
    //5.5 Spark Partitioners
    //spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    val df7 = df6.join(df3, df6("_version_") === df3("_version_"))
    spark.conf.set("spark.sql.shuffle.partitions", 200)
    df7.select(df6("*")).write.mode("overwrite").parquet("example")




    import org.apache.spark.sql.functions._

    val df_1 = spark.range(100000000).withColumn("id", lit("x"))
    val extravalues = spark.range(4).withColumn("id", lit("y"))
    val moreextravalues = spark.range(4).withColumn("id", lit("z"))

    val df_2 = df_1.union(extravalues).union(moreextravalues)

    import spark.implicits._

    val df_3 = spark.range(10).withColumn("id", lit("x"))
    val morevalues = Seq(("y"), ("z")).toDF
    val df_4 = df_3.union(morevalues)

    //spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("spark.sql.shuffle.partitions", "10")
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes",1*1024*1024)
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor",3)
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes",2*1024*1024)

    val df_5 = df_2.join(df_4, df_2.col("id") <=> df_4.col("id"))
    df_5.select(df_2("id")).groupBy(df_2("id")).agg(count("*")).show(100, false)


   }
}
