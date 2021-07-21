package com.atguigu.bigdata.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark11_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1,2,3,4,5,6),2)

//    val mapRDD = rdd.coalesce(2)
    val mapRDD = rdd.repartition(3)

    mapRDD.saveAsTextFile("output")

    sc.stop()
  }
}
