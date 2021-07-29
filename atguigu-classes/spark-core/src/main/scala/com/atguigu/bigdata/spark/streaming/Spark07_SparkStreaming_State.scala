package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Spark07_SparkStreaming_State {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("cp")

    val lines = ssc.socketTextStream("localhost", 9999)

    val words = lines.flatMap(_.split(" "))

    val rdd = words.map(word => (word, 1))

    val result = rdd.updateStateByKey[Int]((values: Seq[Int], buff: Option[Int]) => {
      val current = values.foldLeft(0)(_ + _)
      val pre = buff.getOrElse(0)
      Some(current + pre)
    })

    result.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
