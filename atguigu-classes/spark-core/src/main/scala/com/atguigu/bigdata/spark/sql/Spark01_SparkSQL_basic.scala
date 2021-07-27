package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark01_SparkSQL_basic {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate();
    import spark.implicits._

//    val df = spark.read.json("data/user.json")
//    df.createOrReplaceTempView("user")
//    spark.sql("select * from user").show()
//
//    df.select("username").show()
//    df.select('age+1).show()

//    val seq = Seq(1,2,3,4)
//    val ds = seq.toDS()
//    ds.show()
    val rdd = spark.sparkContext.makeRDD(List((1,"zhangsan", 20),(2,"lisi", 30)))
    val df = rdd.toDF("id", "name", "age")
    val ds = df.as[User]

    val ds1 = rdd.map {
      case (id, name, age) => new User(id, name, age)
    }.toDS()
    val rdd1 = ds1.rdd

    spark.close()
  }

  case class User(id:Int, name:String, age: Int){}
}
