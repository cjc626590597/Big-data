package com.atguigu.bigdata.spark.core.rdd.operator.serial

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Serival {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.makeRDD(List(1,2,3,4))

    val person = new Person()
    rdd1.foreach(
      num => {
        println(person.age + num)
      }
    )
  }

//  class Person extends Serializable {
//    val age = 30
//  }
  case class Person(){
    val age = 30
  }
}
