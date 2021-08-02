package com.atguigu.bigdata.spark.streaming

import java.io.{File, FileWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object Spark12_SparkStreaming_Req31 {
  def main(args: Array[String]): Unit = {
    //1.创建 SparkConf
    val sparkConf: SparkConf = new
        SparkConf().setAppName("ReceiverWordCount").setMaster("local[*]")
    //2.创建 StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    //3.定义 Kafka 参数
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG ->
        "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "root",
      "key.deserializer" ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" ->
        "org.apache.kafka.common.serialization.StringDeserializer"
    )
    //4.读取 Kafka 数据创建 DStream
    val kafkaDStream:  InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Set("atguigu"), kafkaPara))
    //5.将每条消息的 KV 取出
    val aclickData: DStream[AclickData] = kafkaDStream.map(
      kafkadata => {
        val data = kafkadata.value
        val datas = data.split(" ")
        AclickData(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )

    val ds = aclickData.map(
        aclickData => {
            val day = (aclickData.time.toLong)/10000*10000
            (day, 1)
        }
    ).reduceByKeyAndWindow((a:Int,b:Int)=> a+b, Seconds(60),Seconds(10))

//    ds.print()
    ds.foreachRDD(
      rdd => {
        val list = ListBuffer[String]()

        val data = rdd.sortByKey().collect()
        data.foreach{
          case(time, sum) => {
            val timef = new SimpleDateFormat("mm:ss").format(new Date(time))
            list.append(s"""{ "xtime":"${timef}", "yval":"${sum}" }""")
          }
        }
        println("{"+ list.mkString(",") +"}")
        val out = new PrintWriter(new FileWriter(new File("C:\\Users\\suntek\\Desktop\\数据开发\\Big-data\\atguigu-classes\\data\\adclick\\adclick.json")))
        out.print("["+ list.mkString(",") +"]")
        out.flush()
        out.close()
      }
    )

    //7.开启任务
    ssc.start()
    ssc.awaitTermination()
  }

  case class AclickData(time:String, area:String, city:String, uid:String, aid:String)
}
