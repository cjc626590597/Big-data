package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark21_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.makeRDD(List(("a",1),("b",2),("c",3)))
    val rdd2 = sc.makeRDD(List(("c",1),("a",2),("a",3)))

    val newRDD = rdd1.join(rdd2)

    newRDD.collect().foreach(println)

    sc.stop()
  }
}
