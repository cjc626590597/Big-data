package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark02_SparkSQL_UDF {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate();
    import spark.implicits._

    val df = spark.read.json("data/user.json")
    df.createOrReplaceTempView("user")
    spark.udf.register("prefix", (username:String)=>{
      "Name:" + username
    })

    spark.sql("select age, prefix(username) from user").show

    spark.close()
  }

  case class User(id:Int, name:String, age: Int){}
}
