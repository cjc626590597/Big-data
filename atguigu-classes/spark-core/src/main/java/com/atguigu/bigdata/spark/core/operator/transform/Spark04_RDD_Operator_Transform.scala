package com.atguigu.bigdata.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(List(1,2),List(3,4)))

    val mapRDD = rdd.flatMap(
      list => {
        list
      }
    )

    mapRDD.collect().foreach(println)
    sc.stop()
  }
}
