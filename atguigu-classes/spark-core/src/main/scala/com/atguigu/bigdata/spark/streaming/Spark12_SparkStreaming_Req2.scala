package com.atguigu.bigdata.spark.streaming

import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.bigdata.spark.streaming.Spark12_SparkStreaming_Req1_black_list.AclickData
import com.atguigu.bigdata.spark.util.JDBCUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object Spark12_SparkStreaming_Req2 {
  def main(args: Array[String]): Unit = {
    //1.创建 SparkConf
    val sparkConf: SparkConf = new
        SparkConf().setAppName("ReceiverWordCount").setMaster("local[*]")
    //2.创建 StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(1))
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

    val ds: DStream[((String, String, String, String), Int)] = aclickData.transform(
      rdd => {
        rdd.map(
          aclickData => {
            val sdf = new SimpleDateFormat("yyyy-MM-dd")
            val day = sdf.format(new Date(aclickData.time.toLong))
            val area = aclickData.area
            val city = aclickData.city
            val ad = aclickData.aid
            ((day, area, city, ad), 1)
          }
        ).reduceByKey(_ + _)
      }
    )

    ds.foreachRDD(
      rdd =>{
        rdd.foreachPartition(
          iter => {
            val conn = JDBCUtils.getConnection
            iter.foreach{
              case((day, area, city, ad), sum) => {
                val sql =
                  """
                    |INSERT INTO area_city_ad_count
                    |VALUES(?,?,?,?,?)
                    |ON DUPLICATE KEY
                    |UPDATE count = count +?
                    |""".stripMargin
                val pstmt = conn.prepareStatement(sql)
                pstmt.setString(1, day)
                pstmt.setString(2, area)
                pstmt.setString(3, city)
                pstmt.setString(4, ad)
                pstmt.setInt(5, sum)
                pstmt.setInt(6, sum)
                pstmt.executeUpdate()
                pstmt.close()
              }
            }
            conn.close()
          }

        )
      }
    )

    //6.计算 WordCount

    //7.开启任务
    ssc.start()
    ssc.awaitTermination()
  }

  case class AclickData(time:String, area:String, city:String, uid:String, aid:String)
}
