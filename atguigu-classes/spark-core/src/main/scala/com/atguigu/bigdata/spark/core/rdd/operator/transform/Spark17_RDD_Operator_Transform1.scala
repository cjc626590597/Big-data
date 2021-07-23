package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark17_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(("a",1),("a",2),("b",3),("b",4)), 2)

//    val newRDD = rdd.aggregateByKey(0)(
//      (a, b) => a+b,
//      (a, b) => a+b
//    )
//    val newRDD = rdd.aggregateByKey(0)(_+_, _+_)
//    newRDD.collect().foreach(println)

    val newRDD = rdd.foldByKey(0)(_+_)
    newRDD.collect().foreach(println)
    sc.stop()
  }
}
