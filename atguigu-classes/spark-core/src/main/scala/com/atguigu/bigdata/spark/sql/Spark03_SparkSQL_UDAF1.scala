package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession, functions}

object Spark03_SparkSQL_UDAF1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate();

    val df = spark.read.json("data/user.json")
    df.createOrReplaceTempView("user")
    spark.udf.register("ageAvg", functions.udaf(new MyAvgUDAF))

    spark.sql("select ageAvg(age) from user").show

    spark.close()
  }

  case class Buff(var total:Long, var count:Long)
  case class MyAvgUDAF() extends Aggregator[Long, Buff, Long]{
    override def zero: Buff = {
      Buff(0L, 0L)
    }

    override def reduce(b: Buff, a: Long): Buff = {
      b.total = b.total + a
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
