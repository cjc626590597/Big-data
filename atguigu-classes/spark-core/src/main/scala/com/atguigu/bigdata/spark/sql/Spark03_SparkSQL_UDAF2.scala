package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession, functions}

object Spark03_SparkSQL_UDAF2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate();

    val df = spark.read.json("data/user.json")
    df.createOrReplaceTempView("user")
    import spark.implicits._
    val ds: Dataset[User] = df.as[User]

    val udf = new MyAvgUDAF
    val column = udf.toColumn

    ds.select(column).show()

    spark.close()
  }

  case class User(age:Long, username:String)
  case class Buff(var total:Long, var count:Long)
  case class MyAvgUDAF() extends Aggregator[User, Buff, Long]{
    override def zero: Buff = {
      Buff(0L, 0L)
    }

    override def reduce(b: Buff, a: User): Buff = {
      b.total = b.total + a.age
      b.count = b.count + 1
      b
    }

    override def merge(b1: Buff, b2: Buff): Buff = {
      b1.total = b1.total + b2.total
      b1.count = b1.count + b2.count
      b1
    }

    override def finish(reduction: Buff): Long = {
      reduction.total/reduction.count
    }

    override def bufferEncoder: Encoder[Buff] = Encoders.product

    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }
}
