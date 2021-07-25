package com.atguigu.bigdata.spark.core.req

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark04_Req_Hotcategorytop10Analysis3 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Analysis")
    val sc = new SparkContext(sparkConf)

//    一次性统计每个品类点击的次数，下单的次数和支付的次数：
    val rdd = sc.textFile("data/user_visit_action.txt")
    val acc = new CategoryAccumulator()
    sc.register(acc, "CategoryAcc")

    //    （品类，（点击总数，下单总数，支付总数）
    val resultRdd= rdd.foreach(
      line => {
        val data = line.split("_")
        if (data(6) != "-1") {
          acc.add(data(6), "click")
        } else if (data(8) != "null") {
          val cids = data(8).split(",")
          cids.foreach(cid => acc.add(cid, "order"))
        } else if (data(10) != "null") {
          val cids = data(10).split(",")
          cids.foreach(cid => acc.add(cid, "pay"))
        } else {
          Nil
        }
      }
    )


    val resultValue1 = acc.value
    val resultValue = resultValue1.map(_._2)
    val result = resultValue.toList.sortWith(
      (left, right) => {
        if (left.clickCt > right.clickCt) true
        else if (left.clickCt == right.clickCt) {
          if (left.orderCt > right.orderCt) true
          else if (left.orderCt == right.orderCt) {
            if (left.payCt > right.payCt) true else false
          }
          else false
        }
        else false
      }
    )
    result.take(10).foreach(println)
  }

  case class Category(cid:String, var clickCt:Int, var orderCt:Int, var payCt:Int){

  }

  class CategoryAccumulator extends AccumulatorV2[(String,String), mutable.Map[String, Category]]{
    val map = mutable.HashMap[String, Category]()

    override def isZero: Boolean = map.isEmpty

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, Category]] = new CategoryAccumulator

    override def reset(): Unit = map.clear()

    override def add(v: (String, String)): Unit = {
      val cid = v._1
      val category = map.getOrElse(cid, new Category(cid, 0, 0, 0))
      val actionType = v._2
      if (actionType == "click"){
        category.clickCt += 1
      }else if(actionType == "order"){
        category.orderCt += 1
      }else if(actionType == "pay"){
        category.payCt += 1
      }
      map.update(cid, category)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, Category]]): Unit = {
      val myMap = map
      val otherMap = other.value
      otherMap.foreach(
        kv => {
          val category = myMap.getOrElse(kv._1, new Category(kv._1, 0, 0, 0))
          category.clickCt += kv._2.clickCt
          category.orderCt += kv._2.orderCt
          category.payCt += kv._2.payCt
          myMap.update(kv._1, category)
        }
      )
    }

    override def value: mutable.Map[String, Category] = map
  }
}
