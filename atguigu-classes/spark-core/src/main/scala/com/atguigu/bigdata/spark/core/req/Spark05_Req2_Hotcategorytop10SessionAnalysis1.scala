package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_Req2_Hotcategorytop10SessionAnalysis1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Analysis")
    val sc = new SparkContext(sparkConf)

//    一次性统计每个品类点击的次数，下单的次数和支付的次数：
    val rdd = sc.textFile("data/user_visit_action.txt")
    rdd.cache()
    val top10Ids = top10(rdd).map(_._1)

    val rdd1 = rdd.filter(
      line => {
        val data = line.split("_")
        if (data(6) != "-1") {
          top10Ids.contains(data(6))
        } else {
          false
        }
      }
    )

    val rdd2 = rdd1.map(
      line => {
        val data = line.split("_")
        ((data(6), data(2)), 1)
      }
    ).reduceByKey(_+_)

    val rdd3 = rdd2.map {
      case ((cid, sid), num) => {
        (cid, (sid, num))
      }
    }

    val rdd4 = rdd3.groupByKey()
    val rdd5 = rdd4.mapValues(
      value => {
        value.toList.sortBy(_._2)(Ordering.Int.reverse)
      }
    ).take(10)
    rdd5.foreach(println)
  }

  def top10(rdd: RDD[String]):Array[(String, (Int, Int, Int))] ={
    //    （品类，（点击总数，下单总数，支付总数）
    val resultRdd= rdd.flatMap(
      line => {
        val data = line.split("_")
        if (data(6) != "-1") {
          List((data(6), (1, 0, 0)))
        } else if (data(8) != "null") {
          val cids = data(8).split(",")
          cids.map(cid => (cid, (0, 1, 0)))
        } else if (data(10) != "null") {
          val cids = data(10).split(",")
          cids.map(cid => (cid, (0, 0, 1)))
        } else {
          Nil
        }
      }
    )

    val resultValue = resultRdd.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )

    val tuples: Array[(String, (Int, Int, Int))] = resultValue.sortBy(_._2, false).take(10)
    tuples
  }
}
