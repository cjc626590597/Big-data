package com.atguigu.bigdata.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1,2,3,4), 2)

    def mod(num: Int): Int={
      num % 2
    }

    val mapRDD = rdd.groupBy(mod)

    mapRDD.collect().foreach(println)
    sc.stop()
  }
}
