package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Req_Hotcategorytop10Analysis2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Analysis")
    val sc = new SparkContext(sparkConf)

//    一次性统计每个品类点击的次数，下单的次数和支付的次数：
    val rdd = sc.textFile("data/user_visit_action.txt")

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

    resultValue.sortBy(_._2, false).take(10).foreach(println)
  }
}
