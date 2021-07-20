package com.atguigu.bigdata.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

    val mapRDD: RDD[Int] = rdd.mapPartitionsWithIndex(
      (index, iterator) => {
        println(">>>>>>>>>>>>>>>")
        if(index == 1){
          iterator
        }else{
          Nil.iterator
        }
      }
    )

    mapRDD.collect().foreach(println)
    sc.stop()
  }
}
