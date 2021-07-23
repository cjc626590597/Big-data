package com.atguigu.bigdata.spark.core.rdd.operator.transform

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Transform_Text {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.textFile("data/apache.log")

    val groupRDD: RDD[(String, Iterable[(String, Int)])] = rdd.map(line => {
      val strings: Array[String] = line.split(" ")
      val day = strings(3)
      val format: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val date = format.parse(day)
      val sdf = new SimpleDateFormat("HH")
      val str = sdf.format(date)
      (str, 1)
    }).groupBy(_._1)

    val mapRDD = groupRDD.map {
      case (date, list) => (date, list.size)
    }

    mapRDD.collect().foreach(println)
    sc.stop()
  }
}
