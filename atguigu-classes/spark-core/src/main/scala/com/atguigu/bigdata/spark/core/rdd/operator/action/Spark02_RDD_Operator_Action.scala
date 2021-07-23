package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark02_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    println(rdd.reduce(_ + _))

    println(rdd.collect().mkString(","))

    println(rdd.count())

    println(rdd.first())

    println(rdd.take(3).mkString(","))

    val rdd1: RDD[Int] = sc.makeRDD(List(4,3,2,1))
    println(rdd1.takeOrdered(3).mkString(","))
  }
}
