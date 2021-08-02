package com.atguigu.bigdata.spark.streaming

import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

import com.atguigu.bigdata.spark.util.JDBCUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object Spark12_SparkStreaming_Req1_black_list {
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
                val pstmt1 = conn.prepareStatement(
                  """
                    |INSERT INTO black_list (userid)
                    |VALUES(?)
                    |ON DUPLICATE KEY
                    |UPDATE userid=?
                    |""".stripMargin)
                pstmt1.setString(1, user)
                pstmt1.setString(2, user)
                pstmt1.executeUpdate()
                pstmt1.close()
              }else{
                //没超过阈值
                val pstmt2 = conn.prepareStatement(
                  """
                    |SELECT *
                    |FROM user_ad_count
                    |WHERE dt=? AND userid=? AND adid =?
                    |""".stripMargin)
                pstmt2.setString(1, day)
                pstmt2.setString(2, user)
                pstmt2.setString(3, ad)
                val rs = pstmt2.executeQuery()
                //存在旧值，需要更新
                if(rs.next()){
                  val pstmt4 = conn.prepareStatement(
                    """
                      |UPDATE user_ad_count
                      |SET count=count+?
                      |WHERE dt=? AND userid=? AND adid =?
                      |""".stripMargin)
                  pstmt4.setString(2, day)
                  pstmt4.setString(3, user)
                  pstmt4.setString(4, ad)
                  pstmt4.setInt(1, count)
                  pstmt4.executeUpdate()
                  pstmt4.close()

                  //更新值后，判断是否超过阈值
                  val pstmt5 = conn.prepareStatement(
                    """
                      |SELECT *
                      |FROM user_ad_count
                      |WHERE dt=? AND userid=? AND adid =? AND count>=30
                      |""".stripMargin)
                  pstmt5.setString(1, day)
                  pstmt5.setString(2, user)
                  pstmt5.setString(3, ad)
                  val rs1 = pstmt5.executeQuery()
                  //如果存在超过阈值，加入黑名单
                  if (rs1.next()){
                    val pstmt6 = conn.prepareStatement(
                      """
                        |INSERT INTO black_list
                        |VALUES(?)
                        |ON DUPLICATE KEY
                        |UPDATE userid=?
                        |""".stripMargin)
                    pstmt6.setString(1, user)
                    pstmt6.setString(2, user)
                    pstmt6.executeUpdate()
                    pstmt6.close()
                  }

                }else{
                  //不存在旧值，插入新数据
                  val pstmt3 = conn.prepareStatement(
                    """
                      |INSERT INTO user_ad_count (dt,userid,adid,count)
                      |VALUES (?,?,?,?)
                      |""".stripMargin)
                  pstmt3.setString(1, day)
                  pstmt3.setString(2, user)
                  pstmt3.setString(3, ad)
                  pstmt3.setInt(4, count)
                  pstmt3.executeUpdate()
                  pstmt3.close()
                }

              }
              conn.close()
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
