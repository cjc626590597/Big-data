package com.atguigu.bigdata.spark.sql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Spark04_SparkSQL_mysql {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new
        SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    //创建 SparkSession 对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    //方式 1：通用的 load 方法读取
    spark.read.format("jdbc")
      .option("url", "jdbc:mysql://linux1:3306/spark-sql")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123123")
      .option("dbtable", "user")
      .load().show
    //方式 2:通用的 load 方法读取 参数另一种形式
    spark.read.format("jdbc")
      .options(Map("url"->"jdbc:mysql://linux1:3306/spark-sql?user=root&password=123123","dbtable"->"user","driver"->"com.mysql.jdbc.Driver")).load().show
    //方式 3:使用 jdbc 方法读取
    val props: Properties = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "123123")
    val df: DataFrame = spark.read.jdbc("jdbc:mysql://linux1:3306/spark-sql",
      "user", props)
    df.show
    //释放资源
    spark.stop()
  }

}
