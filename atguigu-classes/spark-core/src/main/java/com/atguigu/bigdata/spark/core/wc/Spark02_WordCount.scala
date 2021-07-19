package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount {
  def main(args: Array[String]): Unit = {

    // 1.建立连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    // 2. 业务
    val value1: RDD[String] = sc.textFile("data\\*")
    val value2: RDD[String] = value1.flatMap(_.split(" "))
    val value3: RDD[(String, Int)] = value2.map(word => (word, 1))
    val value4: RDD[(String, Iterable[(String, Int)])] = value3.groupBy(t => t._1)
    val value5 = value4.map {
      case (word, list) => {
        list.reduce((t1, t2) =>{
          (t1._1, t1._2 + t2._2)
        })
      }
    }
    val value6: Array[(String, Int)] = value5.collect()
    value6.foreach(println)

    // 3. 停止
    sc.stop()
  }
}
