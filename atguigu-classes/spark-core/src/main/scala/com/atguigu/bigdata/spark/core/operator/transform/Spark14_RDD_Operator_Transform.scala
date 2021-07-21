package com.atguigu.bigdata.spark.core.operator.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark14_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1,2,3,4))
    val RDD = rdd.map((_, 1))

    val newRDD = RDD.partitionBy(new HashPartitioner(2))
    newRDD.saveAsTextFile("output")

    sc.stop()
  }
}
