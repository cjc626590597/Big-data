package com.atguigu.bigdata.spark.core.wc

import org.apache.commons.lang.mutable.Mutable
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark03_WordCount1 {
  def main(args: Array[String]): Unit = {

    // 1.建立连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    println(wordCount8(sc))

    // 3. 停止
    sc.stop()
  }

  def wordCount(sc: SparkContext) = {
    val rdd1 = sc.makeRDD(List("Hello Spark", "Hello Scala"))
    val rdd2 = rdd1.flatMap(_.split(" "))
    val rdd3 = rdd2.groupBy(word => word)
    val rdd4 = rdd3.mapValues(list => list.size)
  }

  def wordCount1(sc: SparkContext) = {
    val rdd1 = sc.makeRDD(List("Hello Spark", "Hello Scala"))
    val rdd2 = rdd1.flatMap(_.split(" "))
    val rdd3 = rdd2.map(word => (word, 1))
    val rdd4 = rdd3.groupByKey()
    val rdd5 = rdd4.mapValues(list => list.size)
  }

  def wordCount2(sc: SparkContext) = {
    val rdd1 = sc.makeRDD(List("Hello Spark", "Hello Scala"))
    val rdd2 = rdd1.flatMap(_.split(" "))
    val rdd3 = rdd2.map(word => (word, 1))
    val rdd4 = rdd3.reduceByKey(_+_)
  }

  def wordCount3(sc: SparkContext) = {
    val rdd1 = sc.makeRDD(List("Hello Spark", "Hello Scala"))
    val rdd2 = rdd1.flatMap(_.split(" "))
    val rdd3 = rdd2.map(word => (word, 1))
    val rdd4 = rdd3.aggregateByKey(0)(_+_, _+_)
  }

  def wordCount4(sc: SparkContext) = {
    val rdd1 = sc.makeRDD(List("Hello Spark", "Hello Scala"))
    val rdd2 = rdd1.flatMap(_.split(" "))
    val rdd3 = rdd2.map(word => (word, 1))
    val rdd4 = rdd3.foldByKey(0)(_+_)
  }

  def wordCount5(sc: SparkContext) = {
    val rdd1 = sc.makeRDD(List("Hello Spark", "Hello Scala"))
    val rdd2 = rdd1.flatMap(_.split(" "))
    val rdd3 = rdd2.map(word => (word, 1))
    val rdd4 = rdd3.combineByKey(
      v=>v,
      (x:Int,y:Int) => x+y,
      (x:Int,y:Int) => x+y,
    )
  }

  def wordCount6(sc: SparkContext) = {
    val rdd1 = sc.makeRDD(List("Hello Spark", "Hello Scala"))
    val rdd2 = rdd1.flatMap(_.split(" "))
    val rdd3 = rdd2.map(word => (word, 1))
    val rdd4 = rdd3.countByKey()
  }

  def wordCount7(sc: SparkContext) = {
    val rdd1 = sc.makeRDD(List("Hello Spark", "Hello Scala"))
    val rdd2 = rdd1.flatMap(_.split(" "))
    val rdd3 = rdd2.countByValue()
    rdd3
  }

  def wordCount8(sc: SparkContext) = {
    val rdd1 = sc.makeRDD(List("Hello Spark", "Hello Scala"))
    val rdd2 = rdd1.flatMap(_.split(" "))
    val rdd3 = rdd2.map(
      word => mutable.Map[String, Long]((word,1))
    )
    val rdd4 = rdd3.reduce(
      (map1, map2) => {
        map2.foreach {
          case (word, count) => {
            val newCount = map1.getOrElse(word, 0L) + count
            map1.update(word, newCount)
          }
        }
        map1
      }
    )
    rdd4
  }

}
