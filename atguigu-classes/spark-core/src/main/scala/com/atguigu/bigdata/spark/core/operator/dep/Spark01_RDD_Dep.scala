package com.atguigu.bigdata.spark.core.operator.dep

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark01_RDD_Dep {
  def main(args: Array[String]): Unit = {

    // 1.建立连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    // 2. 业务
    val value1: RDD[String] = sc.makeRDD(List("hello spark hello scala"))
    println(value1.toDebugString)
    val value2: RDD[String] = value1.flatMap(_.split(" "))
    println(value2.toDebugString)
    val value3: RDD[(String, Int)] = value2.map(word => (word, 1))
    println(value3.toDebugString)
    val value4: RDD[(String, Int)] = value3.reduceByKey(_ + _)
    println(value4.toDebugString)
    val value5: Array[(String, Int)] = value4.collect()
    value5.foreach(println)

    // 3. 停止
    sc.stop()
  }
}
