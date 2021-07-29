package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Spark06_SparkStreaming_Join {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines1 = ssc.socketTextStream("localhost", 9999)
    val lines2 = ssc.socketTextStream("localhost", 7777)

    val words1 = lines1.flatMap(_.split(" ")).map((_,1))
    val words2 = lines2.flatMap(_.split(" ")).map((_,2))

    val words: DStream[(String, (Int, Int))] = words1.join(words2)

//    words1.print()
//    words2.print()
    words.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
