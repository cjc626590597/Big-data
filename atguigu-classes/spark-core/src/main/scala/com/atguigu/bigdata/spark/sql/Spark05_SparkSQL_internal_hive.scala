package com.atguigu.bigdata.spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

object Spark05_SparkSQL_internal_hive {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[12]")
      .config("spark.sql.warehouse.dir","file:///")
      .config("spark.shuffle.file.buffer","96K")
      .config("spark.reducer.maxSizeInFlight","96M")
      .config("spark.executor.heartbeatInterval","30000ms")
      .config("spark.driver.memory", "4G")
      .config("spark.executor.memory", "8G")
      .enableHiveSupport()
      .getOrCreate();

//    spark.sql("create database testdb").show
//    spark.sql("show databases").show
    spark.sql("use testdb")
    //    spark.sql("create table `test`(`id` bigint)")
    //    spark.sql("insert into `test` values(3)").show();
    val frame: DataFrame = spark.sql("select * from `test`");
    val rdd = frame.rdd.collect()
    rdd.foreach(println)
//    spark.sql("select * from mobike.logs").show();
  }

}
