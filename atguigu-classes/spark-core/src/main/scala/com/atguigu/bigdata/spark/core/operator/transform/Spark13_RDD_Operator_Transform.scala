package com.atguigu.bigdata.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark13_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.makeRDD(List(1,2,3,4))
    val rdd2 = sc.makeRDD(List(3,4,5,6))

    val mapRDD1 = rdd1.intersection(rdd2)
    println(mapRDD1.collect().mkString(","))

    val mapRDD2 = rdd1.union(rdd2)
    println(mapRDD2.collect().mkString(","))

    val mapRDD3 = rdd1.subtract(rdd2)
    println(mapRDD3.collect().mkString(","))

    val mapRDD4 = rdd1.zip(rdd2)
    println(mapRDD4.collect().mkString(","))

    sc.stop()
  }
}
