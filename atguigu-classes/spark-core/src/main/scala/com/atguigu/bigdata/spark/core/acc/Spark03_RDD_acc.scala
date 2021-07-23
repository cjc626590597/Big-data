package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_acc {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val value = sc.makeRDD(List(1, 2, 3, 4))

    var sum = sc.longAccumulator("sum")
    val value1 = value.map(
      num => {
        sum.add(num)
        sum
      }
    )

    value1.collect()
    value1.collect()
    println(sum.value)
  }
}
