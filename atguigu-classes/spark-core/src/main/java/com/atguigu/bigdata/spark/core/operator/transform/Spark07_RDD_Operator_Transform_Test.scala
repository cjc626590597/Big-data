package com.atguigu.bigdata.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.textFile("data/apache.log")

    val mapRDD = rdd.filter(line => {
      val date = line.split(" ")(3)
      date.startsWith("17/05/2015")
    })

    mapRDD.collect().foreach(println)
    sc.stop()
  }
}
