package com.atguigu.bigdata.spark.streaming

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object Spark11_SparkStreaming_MockData {
  def main(args: Array[String]): Unit = {
    val prop = new Properties()
    // 添加配置
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    // 根据配置创建 Kafka 生产者
    val producer = new KafkaProducer[String, String](prop)

    while (true){
      generateData().foreach(
        data => {
          val record = new ProducerRecord[String, String]("atguigu", data)
          producer.send(record)
          println(data)
        }
      )

      Thread.sleep(5000)
    }
  }

  def generateData(): ArrayBuffer[String] ={
    val array = ArrayBuffer[String]()
    val areaArray = Array("华东","华南","华西","华北")
    val cityArray = Array("上海","深圳","西安","北京")
    val random = Random
    for( i <- 1 to 50){
      val time = System.currentTimeMillis()
      val area = areaArray(random.nextInt(4))
      val city = cityArray(random.nextInt(4))
      val uid = random.nextInt(5) + 1
      val aid = random.nextInt(5) + 1
      array += time + " " + area + " " + city + " " + uid + " " + aid
    }
    array
  }
}
