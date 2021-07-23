package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

    val mapRDD: RDD[Int] = rdd.mapPartitions(
      iterator => {
        println(">>>>>>>>>>>>>>>")
        List(iterator.max).iterator
      }
    )

    mapRDD.collect().foreach(println)
  }
}
