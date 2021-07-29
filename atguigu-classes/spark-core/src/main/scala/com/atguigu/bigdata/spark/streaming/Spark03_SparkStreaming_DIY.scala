package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

object Spark03_SparkStreaming_DIY {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val receiveDS = ssc.receiverStream(new MyReceiver)
    receiveDS.print()

    ssc.start()
    ssc.awaitTermination()
  }

  class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY){
    private var flag = true
    override def onStart(): Unit = {
      new Thread(
        new Runnable {
          override def run(): Unit = {
            while(flag){
              val str = "采集的数据为: " + Random.nextInt(10).toString
              store(str)
              Thread.sleep(500)
            }
          }
        }
      ).start()
    }

    override def onStop(): Unit = {
      flag = false
    }
  }
}
