package com.atguigu.bigdata.spark.core.rdd.operator.io

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_load {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("Save")
    val sc = new SparkContext(sparkConf)

    sc.textFile("output").collect().foreach(println)
//    sc.objectFile("output1").collect().foreach(println)
//    sc.sequenceFile[String,Int]("output2").collect().foreach(println)


  }
}
