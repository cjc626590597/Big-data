package com.atguigu.bigdata.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark01_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    def product(a:Int): Int = {
      a * 2
    }

//    val mapRDD: RDD[Int] = rdd.map((a:Int) => { a*2 })
//    val mapRDD: RDD[Int] = rdd.map((a:Int) =>  a*2 )
//    val mapRDD: RDD[Int] = rdd.map((a) =>  a*2 )
    val mapRDD: RDD[Int] = rdd.map( _*2 )

    mapRDD.collect().foreach(println)
  }
}
