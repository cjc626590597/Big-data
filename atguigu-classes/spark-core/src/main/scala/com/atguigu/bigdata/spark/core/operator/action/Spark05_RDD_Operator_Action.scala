package com.atguigu.bigdata.spark.core.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.makeRDD(List(("a",1),("a",1),("a",3),("b",2)))

    rdd1.saveAsTextFile("output")
    rdd1.saveAsObjectFile("output1")
    rdd1.saveAsSequenceFile("output2")
  }
}
