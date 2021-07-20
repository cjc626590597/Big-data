package com.atguigu.bigdata.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(List(1,2),3,List(3,4)))

    val mapRDD = rdd.flatMap(
      data => {
        data match {
          case list: List[_] => list
          case dat: Int => List(dat)
        }
      }
    )

    mapRDD.collect().foreach(println)
    sc.stop()
  }
}
