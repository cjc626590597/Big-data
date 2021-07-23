package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark23_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.makeRDD(List(("a",1),("b",2)))
    val rdd2 = sc.makeRDD(List(("a",4),("c",5),("c",6)))

    val newRDD = rdd1.cogroup(rdd2)

    newRDD.collect().foreach(println)

    sc.stop()
  }
}
