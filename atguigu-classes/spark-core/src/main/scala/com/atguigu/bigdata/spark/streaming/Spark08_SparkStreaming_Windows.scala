package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Spark08_SparkStreaming_Windows {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("cp")

    val lines = ssc.socketTextStream("localhost", 9999)

    val words = lines.flatMap(_.split(" "))

    val rdd = words.map(word => (word, 1))

    val result = rdd.reduceByKeyAndWindow((a:Int,b:Int) => a+b, Seconds(12), Seconds(6))

    result.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
