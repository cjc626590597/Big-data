package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark23_RDD_Req {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.textFile("data/agent.log")

    val rdd2 = rdd1.map(
      word => {
        val data = word.split(" ")
        ((data(1), data(4)), 1)
      }
    )

    val rdd3 = rdd2.reduceByKey(_ + _)

    val rdd4 = rdd3.map {
      case ((prov, ad), sum) => (prov, (ad, sum))
    }

    val rdd5 = rdd4.groupByKey()

    val rdd6 = rdd5.mapValues(
      tuple => {
        tuple.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
    )

    rdd6.collect().foreach(println)

    sc.stop()
  }
}
