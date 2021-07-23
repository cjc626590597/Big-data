package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_acc {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val value = sc.makeRDD(List(1, 2, 3, 4))

    var sum:Int = 0
    value.foreach(
      num => {
        sum += num
      }
    )
    println(sum)
  }
}
