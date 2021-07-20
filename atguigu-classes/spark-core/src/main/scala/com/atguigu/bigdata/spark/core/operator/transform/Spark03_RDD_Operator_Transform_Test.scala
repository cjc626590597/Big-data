package com.atguigu.bigdata.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    val mapRDD = rdd.mapPartitionsWithIndex(
      (index, iterator) => {
        iterator.map( word => (index, word))
      }
    )

    mapRDD.collect().foreach(println)
    sc.stop()
  }
}
