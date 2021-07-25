package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_Req3_PageFlow2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Analysis")
    val sc = new SparkContext(sparkConf)

    val ids = List[Long](1,2,3,4,5,6,7)
    val okpageids = ids.zip(ids.tail)

    val rdd: RDD[String] = sc.textFile("data/user_visit_action.txt")
    //    一次性统计每个品类点击的次数，下单的次数和支付的次数：

    val rdd1 = rdd.map(
      line => {
        val data = line.split("_")
        new UserVisitAction(
          data(0),
          data(1).toLong,
          data(2),
          data(3).toLong,
          data(4),
          data(5),
          data(6).toLong,
          data(7).toLong,
          data(8),
          data(9),
          data(10),
          data(11),
          data(12).toLong
        )
      }
    )
    rdd1.cache()

    val rdd2: Map[Long, Long] = rdd1.filter(action => (ids.contains(action.page_id)))
      .map(action => (action.page_id, 1L)).reduceByKey(_ + _).collect().toMap

    val rdd3: RDD[(String, Iterable[UserVisitAction])] = rdd1.groupBy(_.session_id)
    val rdd4: RDD[(String, List[((Long, Long), Long)])] = rdd3.mapValues {
      iter => {
        val sortedList = iter.toList.sortBy(_.date)
        val flowids = sortedList.map(_.page_id)
        val flowPageids = flowids.zip(flowids.tail)
        flowPageids.map((_, 1L))
      }
    }
    val rdd5: RDD[List[((Long, Long), Long)]] = rdd4.map(_._2)
    val rdd6: RDD[((Long, Long), Long)] = rdd5.flatMap(list => list).filter(action => okpageids.contains(action._1))

    val rdd7 = rdd6.reduceByKey(_ + _)
    rdd7.foreach {
      case ((pageid1, pageid2), sum) => {
        val total:Long = rdd2.getOrElse(pageid1, 0L)
        println(s"The pageflow of page from ${pageid1} to ${pageid2} is "+sum.toDouble/total)
      }
    }

  }

  case class UserVisitAction(
    date: String,//用户点击行为的日期
    user_id: Long,//用户的 ID
    session_id: String,//Session 的 ID
    page_id: Long,//某个页面的 ID
    action_time: String,//动作的时间点
    search_keyword: String,//用户搜索的关键词
    click_category_id: Long,//某一个商品品类的 ID
    click_product_id: Long,//某一个商品的 ID
    order_category_ids: String,//一次订单中所有品类的 ID 集合
    order_product_ids: String,//一次订单中所有商品的 ID 集合
    pay_category_ids: String,//一次支付中所有品类的 ID 集合
    pay_product_ids: String,//一次支付中所有商品的 ID 集合
    city_id: Long
  )//城市 id
}
