package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Req_Hotcategorytop10Analysis {
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

    val resultRdd = clickCountRdd.cogroup(orderCountRdd, paymentCountRdd)
    val resultValue = resultRdd.mapValues {
      case (clickct, orderct, payct) => {
        var click = 0
        val iter1 = clickct.iterator
        while (iter1.hasNext) {
          click = iter1.next()
        }
        var order = 0
        val iter2 = orderct.iterator
        while (iter2.hasNext) {
          order = iter2.next()
        }
        var pay = 0
        val iter3 = payct.iterator
        while (iter3.hasNext) {
          pay = iter3.next()
        }
        (click, order, pay)
      }
    }

    resultValue.sortBy(_._2, false).take(10).foreach(println)
  }
}
