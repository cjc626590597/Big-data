package com.atguigu.bigdata.spark.streaming

import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.bigdata.spark.util.JDBCUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object Spark12_SparkStreaming_Req1_black_list1 {
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

    val ds: DStream[((String, String, String), Int)] = aclickData.transform(
      rdd => {
        val listBuffer = ListBuffer[String]()
        val conn = JDBCUtils.getConnection
        val pstmt = conn.prepareStatement("select userid from black_list")
        val rs = pstmt.executeQuery()
        while (rs.next()) {
          listBuffer.append(rs.getString(1))
        }
        rs.close()
        pstmt.close()
        conn.close()
        val filterRDD: RDD[AclickData] = rdd.filter(
          data => {
            !listBuffer.contains(data.uid)
          }
        )
        filterRDD.map(
          aclickData => {
            val sdf = new SimpleDateFormat("yyyy-MM-dd")
            val day = sdf.format(new Date(aclickData.time.toLong))
            val user = aclickData.uid
            val ad = aclickData.aid
            ((day, user, ad), 1)
          }
        ).reduceByKey(_ + _)
      }
    )


    ds.foreachRDD(
      rdd => {
          rdd.foreach {
            case ((day, user, ad), count) => {
              println(s"(($day, $user, $ad), $count)")
              val conn = JDBCUtils.getConnection
              if(count >= 30){
                //超过阈值，加入黑名单
                val sql = """
                            |INSERT INTO black_list (userid)
                            |VALUES(?)
                            |ON DUPLICATE KEY
                            |UPDATE userid=?
                            |""".stripMargin
                JDBCUtils.executeUpdate(conn, sql, Array(user, user))
              }else{
                //没超过阈值
                val sql2 = """
                             |SELECT *
                             |FROM user_ad_count
                             |WHERE dt=? AND userid=? AND adid =?
                             |""".stripMargin
                val flag = JDBCUtils.isExist(conn, sql2, Array(day, user, ad))
                //存在旧值，需要更新
                if(flag){
                  val sql1 = """
                               |UPDATE user_ad_count
                               |SET count=count+?
                               |WHERE dt=? AND userid=? AND adid =?
                               |""".stripMargin
                  JDBCUtils.executeUpdate(conn, sql1, Array(count, day, user, ad))

                  //更新值后，判断是否超过阈值
                  val sql3 = """
                               |SELECT *
                               |FROM user_ad_count
                               |WHERE dt=? AND userid=? AND adid =? AND count>=30
                               |""".stripMargin
                  val flag1 = JDBCUtils.isExist(conn, sql3, Array(day, user, ad))

                  //如果存在超过阈值，加入黑名单
                  if (flag1){
                    val sql4 = """
                                 |INSERT INTO black_list
                                 |VALUES(?)
                                 |ON DUPLICATE KEY
                                 |UPDATE userid=?
                                 |""".stripMargin
                    JDBCUtils.executeUpdate(conn, sql4, Array(user, user))
                  }

                }else{
                  //不存在旧值，插入新数据
                  val sql5 = """
                               |INSERT INTO user_ad_count (dt,userid,adid,count)
                               |VALUES (?,?,?,?)
                               |""".stripMargin
                  JDBCUtils.executeUpdate(conn, sql5, Array(day, user, ad, count))
                }
                conn.close()
              }
            }
          }
      }
    )

    //6.计算 WordCount

    //7.开启任务
    ssc.start()
    ssc.awaitTermination()
  }

  case class AclickData(time:String, are:String, city:String, uid:String, aid:String)
}
