package com.atguigu.bigdata.spark.sql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object Spark04_SparkSQL_mysql_write {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new
        SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    //创建 SparkSession 对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val rdd: RDD[User2] = spark.sparkContext.makeRDD(List(User2("lisi", 20),
      User2("zs", 30)))
    val ds: Dataset[User2] = rdd.toDS
    //方式 1：通用的方式 format 指定写出类型
    ds.write
      .format("jdbc")
      .option("url", "jdbc:mysql://linux1:3306/spark-sql")
      .option("user", "root")
      .option("password", "123123")
      .option("dbtable", "user")
      .mode(SaveMode.Append)
      .save()
    //方式 2：通过 jdbc 方法
    val props: Properties = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "123123")
    ds.write.mode(SaveMode.Append).jdbc("jdbc:mysql://linux1:3306/spark-sql",
      "user", props)
    //释放资源
    spark.stop()
  }

  case class User2(name: String, age: Long)
}
