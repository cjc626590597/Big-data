package com.atguigu.bigdata.spark.core.operator.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Persist {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val value1: RDD[String] = sc.makeRDD(List("hello spark", "hello scala"))
    val value2: RDD[String] = value1.flatMap(_.split(" "))
    val value3: RDD[(String, Int)] = value2.map(word => (word, 1))
    val value4: RDD[(String, Int)] = value3.reduceByKey(_ + _)
    val value5: Array[(String, Int)] = value4.collect()
    value5.foreach(println)

    val rdd1: RDD[String] = sc.makeRDD(List("hello spark", "hello scala"))
    val rdd2: RDD[String] = rdd1.flatMap(_.split(" "))
    val rdd3: RDD[(String, Int)] = rdd2.map(word => (word, 1))
    val rdd4 = rdd3.groupByKey()
    rdd4.foreach(println)

    sc.stop()
  }
}
