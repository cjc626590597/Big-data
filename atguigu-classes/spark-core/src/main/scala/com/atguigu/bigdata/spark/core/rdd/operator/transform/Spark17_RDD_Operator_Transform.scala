package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark17_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(("a",1),("a",2),("a",3),("a",4)), 2)

    val newRDD = rdd.aggregateByKey(0)(
      (a, b) => math.max(a, b),
      (a, b) => a+b
    )
    newRDD.collect().foreach(println)

    sc.stop()
  }
}
