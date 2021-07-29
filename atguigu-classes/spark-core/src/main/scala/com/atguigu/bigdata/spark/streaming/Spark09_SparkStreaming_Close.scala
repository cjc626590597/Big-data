package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

object Spark09_SparkStreaming_Close {
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

    new Thread(new Runnable {
      override def run(): Unit = {
//        while (true){
//          if (true){
//            val state = ssc.getState()
//            if (state == StreamingContextState.ACTIVE){
//              ssc.stop(true, true)
//            }
//          }
//          Thread.sleep(5000)
//        }
//        System.exit(-1)

        Thread.sleep(5000)
        val state = ssc.getState()
        if (state == StreamingContextState.ACTIVE){
          ssc.stop(true, true)
        }
      }
    }).start()

    ssc.awaitTermination()
  }
}
