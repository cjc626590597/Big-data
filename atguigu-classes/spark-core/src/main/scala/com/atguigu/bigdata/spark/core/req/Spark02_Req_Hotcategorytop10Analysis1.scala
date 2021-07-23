package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Req_Hotcategorytop10Analysis1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Analysis")
    val sc = new SparkContext(sparkConf)

//    一次性统计每个品类点击的次数，下单的次数和支付的次数：
    val rdd = sc.textFile("data/user_visit_action.txt")

//    （品类，（点击总数，下单总数，支付总数）
    val clickRdd = rdd.filter(
      line => {
        val data = line.split("_")
        data(6) != "-1"
      }
    )

    val clickCountRdd = clickRdd.map(
      line => {
        val data = line.split("_")
        (data(6),1)
      }
    ).reduceByKey(_+_)

    val orderRdd = rdd.filter(
      line => {
        val data = line.split("_")
        data(8) != "null"
      }
    )

    val orderCountRdd = orderRdd.flatMap(
      line => {
        val data = line.split("_")
        val cid = data(8)
        val cids = cid.split(",")
        cids.map((_,1))
      }
    ).reduceByKey(_+_)


    val paymentRdd = rdd.filter(
      line => {
        val data = line.split("_")
        data(10) != "null"
      }
    )

    val paymentCountRdd = paymentRdd.flatMap(
      line => {
        val data = line.split("_")
        val cid = data(10)
        val cids = cid.split(",")
        cids.map((_,1))
      }
    ).reduceByKey(_+_)

    val rdd1 = clickCountRdd.mapValues {
      cid => {
        (cid, 0, 0)
      }
    }
    val rdd2 = orderCountRdd.mapValues {
      cid => {
        (0, cid, 0)
      }
    }
    val rdd3 = paymentCountRdd.mapValues {
      cid => {
        (0, 0, cid)
      }
    }
    val resultRdd: RDD[(String, (Int, Int, Int))] = rdd1.union(rdd2).union(rdd3)
    val resultValue = resultRdd.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )

    resultValue.sortBy(_._2, false).take(10).foreach(println)
  }
}
