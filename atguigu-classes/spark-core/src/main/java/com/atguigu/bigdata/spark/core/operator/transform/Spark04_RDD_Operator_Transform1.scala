package com.atguigu.bigdata.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List("hello scala","hello spark"))

    val mapRDD = rdd.flatMap(
      str => {
        str.split(" ")
      }
    )

    mapRDD.collect().foreach(println)
    sc.stop()
  }
}
