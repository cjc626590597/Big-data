package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

object Spark10_SparkStreaming_Resume {
  def main(args: Array[String]): Unit = {

    val ssc = StreamingContext.getActiveOrCreate(() => {
      val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
      val ssc = new StreamingContext(sparkConf, Seconds(3))

      val lines = ssc.socketTextStream("localhost", 9999)

      val words = lines.flatMap(_.split(" "))

      val rdd = words.map(word => (word, 1))
      val result = rdd.reduceByKey(_ + _)
      result.print()

      ssc
    })

    ssc.checkpoint("cp")

    ssc.start()
    ssc.awaitTermination()
  }
}
