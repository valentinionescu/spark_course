package org.example.notes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{concat, sha2, spark_partition_id}

/**
 * @author ${user.name}
 */
object SparkApp {

  def interactive(spark: SparkSession): Unit = {
    spark.conf.set("parquet.block.size", 8 * 1024 * 1024)
    import org.apache.spark.storage.StorageLevel
    import spark.implicits._

    import scala.collection._
    val df = spark.sql("select * from default.web_logs").repartition($"region_code") //.repartition(3)//
    val df2 = immutable.List.range(1, 100001).toDF.repartition(44)
    val df3 = df.crossJoin(df2)
    //df3.withColumn("partno",spark_partition_id()).groupBy("partno", "region_code").count.sort($"partno".desc).show(200)
    df3.write.mode("overwrite").parquet("example6")
    df3.persist(StorageLevel.DISK_ONLY)
    df3.unpersist

    // When debugging/developing use 1 Core per Executor
    //The number of bytes per partition is:
    //numberOfBytesPerPartition = Minimum(maxPartitionBytes, bytesPerCore)

    // spark.default.parallelism can only be set when creating the SparkSession
    //--conf spark.default.parallelism=no of cores. This will give the bytesPerCore.
    // Use this only if you are not using RDDs for JOINS and REDUCEbyKEY
    //bytesPerCore = (Sum(data files) + FileNo * openCostInBytes) / parallelism

    // maxPartitionBytes can be changed dynamically after the SparkSession was created
    // spark.conf.set("spark.sql.files.maxPartitionBytes", 128*math.pow(1024,2).toInt)

    //For each DATASET and STEP in your processing pipe you need to know the maxPartitionBytes that you can handle per partition,
    //assuming a certain amount of memory and cores per executor.
    //Take into account that 128 MB read from a parquet/snappy are not the same as 128 read from a CSV
    //spark.conf.set("spark.sql.files.maxPartitionBytes", 128*math.pow(1024,2).toInt)
    spark.conf.set("spark.sql.files.maxPartitionBytes", 32 * math.pow(1024, 2).toInt)
    val df4 = spark.read.parquet("example_8m_parquet_block")
    //val df4 = spark.read.parquet("example_8m_parquet_block_hash")
    val df4_1 = df4.repartition(22)
    df4.groupBy("region_code").count.show(100)
    df4.persist(StorageLevel.DISK_ONLY)
    df4.count()
    //Spark will try to split the dataset in equal partitions of your desired size when loading the data from Storage

    //TEST1: Check if partitions are equal
    val totalPartNum = df4.rdd.getNumPartitions
    val df4_test1 = df4.withColumn("partno", spark_partition_id()).groupBy("partno").count.sort($"count".desc)
    df4_test1.show(totalPartNum, false)

    //Assuming the distribution of records per partition is (heavily)skewed perform a second test.
    // Skew can be because of datasets where the size of records is not homogenous e.g. string fields that are null for a certain HIVE partition.
    // or just because file sizes are not homogenous

    //TEST2: Check why the partitions are not equal
    //Force spark to make partitions with equal number of records, not of bytes
    val df4_test2 = df4.repartition(totalPartNum)
    //write the result
    df4_test2.write.mode("overwrite").parquet("df4_test2")
    //check on HDFS if files are of similar size

    //Assuming the TEST2 proved that records size is homogenous and Spark
    //For each DATASET and STEP in your processing pipe you need to know the Number of Records that you can handle per partition,
    //assuming a certain amount of memory and cores per executor.
    val maxPartitionRecords: Double = 1000000
    val totalNoOfRecords: Double = df4.count
    val targetPartNum = (totalNoOfRecords / maxPartitionRecords).ceil.toInt
    //val df5 = df4.repartition(targetPartNum)
    val df5 = df4 //.repartition(32)
    val df6 = df5.withColumn("id2", concat($"value", $"_version_"))
    val df7 = df6.withColumn("myhash", sha2($"id2", 512))
    val df8 = df7.withColumn("binhash", $"myhash".cast("binary"))
    df8.write.mode("overwrite").parquet("example7")

    spark.conf.set("spark.sql.files.maxPartitionBytes", 16 * math.pow(1024, 2).toInt)
    val df9 = spark.read.parquet("example6") //.repartition(36)
    //val df10 = df9.sample(0.1)
    //df10.write.mode("overwrite").parquet("sample1")
    //val df11 = spark.read.parquet("sample1")//.repartition(36)
    spark.conf.set("spark.sql.shuffle.partitions", 50)
    val df12 = df9.join(df9, "myhash")
    df12.select("myhash").write.mode("overwrite").parquet("example4")
    //TEST1: Check if partitions are equal
    val df12_totalPartNum = df12.rdd.getNumPartitions
    val df12_test1 = df12.withColumn("partno", spark_partition_id()).groupBy("partno").count.sort($"count".desc)
    df12_test1.show(df12_totalPartNum, false)

    //
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    val df13 = spark.read.parquet("example_8m_parquet_block").sample(0.1) //.repartition($"region_code")
    val df14 = spark.sql("select * from default.web_logs").sample(0.01)
    val df15 = df14.join(df13, df14("region_code") === df13("region_code"))
    //SIGTERM
    // ERROR cluster.YarnScheduler: Lost executor 7 on valentin-ionescu-3.gce.cloudera.com: Container marked as failed: container_1632654247912_0027_01_000014 on host: valentin-ionescu-3.gce.cloudera.com. Exit status: 143. Diagnostics: [2021-09-30 09:15:49.059]Container killed on request. Exit code is 143
    //[2021-09-30 09:15:49.059]Container exited with a non-zero exit code 143.
    //[2021-09-30 09:15:49.060]Killed by external signal

    // in the stderr log of the executor
    // java.lang.OutOfMemoryError: Java heap space


    //SIGKILL
    //ERROR cluster.YarnScheduler: Lost executor 16 on valentin-ionescu-2.gce.cloudera.com: Container marked as failed: container_1632654247912_0026_01_000025 on host: valentin-ionescu-2.gce.cloudera.com. Exit status: 137. Diagnostics: [2021-09-30 08:54:10.142]Container killed on request. Exit code is 137
    //[2021-09-30 08:54:10.186]Container exited with a non-zero exit code 137.
    //[2021-09-30 08:54:10.187]Killed by external signal
    //  .......
    //java.lang.OutOfMemoryError: GC Overhead Limit Exceeded  in the driver

    // in the stdout log of the executor
    //# There is insufficient memory for the Java Runtime Environment to continue.
    //# Native memory allocation (mmap) failed to map 349700096 bytes for committing reserved memory.
    //# An error report file with more information is saved as:
    //# /yarn/nm/usercache/hdfs/appcache/application_1632654247912_0026/container_1632654247912_0026_01_000014/hs_err_pid30814.log


    df4.write.bucketBy(10, "_version_").sortBy("_version_").mode("overwrite").saveAsTable("example3")
    df4.limit(0).write.mode("overwrite").saveAsTable("example2")
    // Hive:  ALTER TABLE example2 CLUSTERED BY (region_code) SORTED BY (region_code) INTO 10 BUCKETS
    df4.createOrReplaceTempView("df4")
    df4.write.bucketBy(10, "region_code").sortBy("region_code").mode("overwrite").insertInto("example2")
    spark.sql("INSERT INTO TABLE example2_1 SELECT * FROM example3_1")

    spark.conf.set("spark.sql.files.maxPartitionBytes", 32 * math.pow(1024, 2).toInt)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    val df16 = spark.sql("select * from example2")
    val df17 = spark.sql("select * from example2_1")
    val df18 = df16.join(df17, df16("region_code") === df17("region_code")) //.foreach(_ => None)
    df18.select(df16("*")).write.mode("overwrite").parquet("example2")

    import org.apache.spark.sql.SaveMode
    spark.range(10e4.toLong).
      write.bucketBy(4, "id")
      .sortBy("id")
      .mode(SaveMode.Overwrite)
      .saveAsTable("bucketed_4_10e4")
    spark.range(10e6.toLong)
      .write
      .bucketBy(4, "id")
      .sortBy("id")
      .mode(SaveMode.Overwrite)
      .saveAsTable("bucketed_4_10e6")

    val bucketed_4_10e4 = spark.table("bucketed_4_10e4")
    val bucketed_4_10e6 = spark.table("bucketed_4_10e6")

    //partition by: Join and filter columns
    //bucket by: high cardinality columns
    //sort by: group and sort columns
  }

}
