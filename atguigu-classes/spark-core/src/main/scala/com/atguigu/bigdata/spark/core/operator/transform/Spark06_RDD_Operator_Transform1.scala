package com.atguigu.bigdata.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List("Hello","Spark","Helo","Scala"))

    val mapRDD = rdd.groupBy(_.charAt(0))

    mapRDD.collect().foreach(println)
    sc.stop()
  }
}
