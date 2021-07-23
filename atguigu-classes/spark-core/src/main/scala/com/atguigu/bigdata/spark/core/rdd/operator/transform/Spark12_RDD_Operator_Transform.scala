package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

//    val rdd = sc.makeRDD(List(6,5,1,3,2,4),2)
    val rdd = sc.makeRDD(List(("1",1),("11",11),("2",2)),2)

//    val mapRDD = rdd.coalesce(2)
//    val mapRDD = rdd.sortBy(num => num._1)
    val mapRDD = rdd.sortBy(num => num._1.toInt)

    mapRDD.collect().foreach(println)

    sc.stop()
  }
}
