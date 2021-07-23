package com.atguigu.bigdata.spark.core.rdd.operator.dep

import org.apache.spark.{HashPartitioner, Partition, Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark01_RDD_Dep {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val value1 = sc.makeRDD(List(
      ("mba","xxxxxx"),
      ("wmba","xxxxxx"),
      ("cba","xxxxxx"),
      ("dba","xxxxxx")
    ), 3)

    val value2 = value1.partitionBy(new MyPartition)
    val value3 = value2.saveAsTextFile("output")

    sc.stop()
  }

  class MyPartition extends Partitioner{
    def numPartitions : scala.Int = 3

    def getPartition(key : scala.Any) : scala.Int = {
      key match {
        case "mba" => 0
        case "cba" => 1
        case _ => 2
      }
    }

  }
}
