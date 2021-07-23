package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark06_RDD_acc {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(("a",1),("b",2),("c",3)))
    val value2 = mutable.Map(("a",4),("b",5),("c",6))

    val bc = sc.broadcast(value2)

    val rdd1 = rdd.map {
      case (word, count) => {
        val newCount = bc.value.getOrElse(word, 0) + count
        (word, newCount)
      }
    }

    rdd1.collect().foreach(println)
  }
}
