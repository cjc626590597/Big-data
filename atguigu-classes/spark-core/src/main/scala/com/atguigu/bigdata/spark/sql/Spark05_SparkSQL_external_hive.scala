package com.atguigu.bigdata.spark.sql

import java.util.{Properties, UUID}

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object Spark05_SparkSQL_external_hive {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[12]")
      .config("spark.sql.warehouse.dir", "hdfs://hadoop102:8020/user/hive/warehouse")
      .config("spark.shuffle.file.buffer","96K")
      .config("spark.reducer.maxSizeInFlight","96M")
      .config("spark.executor.heartbeatInterval","30000ms")
      .config("spark.driver.memory", "4G")
      .config("spark.executor.memory", "8G")
      .enableHiveSupport()
      .getOrCreate();

//    spark.sql("show tables").show
//    spark.sql("insert into test values(3)").show();
    spark.sql("select * from test").show();
//    spark.sql("select * from mobike.logs").show();
  }

}
