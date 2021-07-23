package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark16_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(("a",1),("a",2),("a",3),("b",3)))

    val newRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    newRDD.collect().foreach(println)

    val newRDD1: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)
    newRDD1.collect().foreach(println)

    sc.stop()
  }
}
