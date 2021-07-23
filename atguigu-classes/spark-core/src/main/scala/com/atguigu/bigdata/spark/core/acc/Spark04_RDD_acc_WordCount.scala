package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark04_RDD_acc_WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val value = sc.makeRDD(List("Hello", "Spark", "Hello"))

    val accumulater = new MyAccumulater()

    sc.register(accumulater, "WordCount")

    value.map(
      str => {
        accumulater.add(str)
      }
    ).collect()

    println(accumulater.value)
  }

  class MyAccumulater extends AccumulatorV2[String, mutable.Map[String, Long]]{
    val myMap = mutable.HashMap[String, Long]()

    override def isZero: Boolean = myMap.isEmpty

    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = new MyAccumulater()

    override def reset(): Unit = myMap.clear()

    override def add(word: String): Unit = {
      val newV = myMap.getOrElse(word, 0L) + 1
      myMap.update(word, newV)
    }

    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val map = this.myMap
      val otherMap = other.value

      otherMap.foreach {
        case (word, num) => {
          val newNum = myMap.getOrElse(word, 0L) + num
          myMap.update(word, newNum)
        }
      }
    }

    override def value: mutable.Map[String, Long] = myMap
  }
}
