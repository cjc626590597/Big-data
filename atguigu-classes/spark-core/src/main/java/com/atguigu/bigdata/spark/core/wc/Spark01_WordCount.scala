package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {
  def main(args: Array[String]): Unit = {

    // 1.建立连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    // 2. 业务
    val value1: RDD[String] = sc.textFile("data\\*")
    val value2: RDD[String] = value1.flatMap(_.split(" "))
    val value3: RDD[(String, Iterable[String])] = value2.groupBy( word => word)
    val value4: RDD[(String, Int)] = value3.map {
      case (word, list) => {
        (word, list.size)
      }
    }
    val value5: Array[(String, Int)] = value4.collect()
    value5.foreach(println)

    // 3. 停止
    sc.stop()
  }
}
