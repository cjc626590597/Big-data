package com.atguigu.bigdata.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark19_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(("a",1),("a",2),("a",3),("b",4),("b",5),("b",6)), 2)

    val newRDD = rdd.combineByKey(
      (_, 1),
      (u:(Int,Int), t:Int) => (u._1+t, u._2+1),
      (p1:(Int,Int), p2:(Int,Int)) => (p1._1+p2._1, p1._2+p2._2)
    )

    newRDD.mapValues {
      case (num, count) => num / count
    }.collect().foreach(println)

    sc.stop()
  }
}
