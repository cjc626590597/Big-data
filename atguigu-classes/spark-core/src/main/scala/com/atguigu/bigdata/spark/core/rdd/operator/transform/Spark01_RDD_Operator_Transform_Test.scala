package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

//    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4), 1)
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))


    val mapRDD: RDD[Int] = rdd.map( num => {
      println("================" + num)
      num
    })

    val mapRDD1: RDD[Int] = mapRDD.map( num => {
      println(">>>>>>>>>>>>>>" + num)
      num
    })

    mapRDD1.collect().foreach(println)
  }
}
