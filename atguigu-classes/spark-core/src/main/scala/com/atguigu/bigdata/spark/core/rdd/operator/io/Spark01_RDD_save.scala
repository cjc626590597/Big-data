package com.atguigu.bigdata.spark.core.rdd.operator.io

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_save {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("Save")
    val sc = new SparkContext(sparkConf)

    val value = sc.makeRDD(List(
      ("a", 1),
      ("b", 2),
      ("c", 3)
    ))

    value.saveAsTextFile("output")
    value.saveAsObjectFile("output1")
    value.saveAsSequenceFile("output2")
  }
}
