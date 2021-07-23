package com.atguigu.bigdata.spark.core.acc

import java.util.concurrent.atomic.LongAccumulator

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_acc {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val value = sc.makeRDD(List(1, 2, 3, 4))

    var sum = sc.longAccumulator("sum")
    value.foreach(
      num => {
        sum.add(num)
      }
    )
    println(sum.value)
  }
}
