package com.suntek.algorithm

import java.util.Properties

import com.suntek.algorithm.common.bean.{QueryBean, TableBean}
import com.suntek.algorithm.common.conf.Constant
import com.suntek.algorithm.common.database.DataBaseFactory
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

/**
  * Created with IntelliJ IDEA.
  * Author: yuanweilin
  * Date: 2020-10-15 9:57
  * Description:统计时间分片关联次数误差
  */
object StatTest {

  def main(args: Array[String]): Unit = {
    val ss = SparkSession
      .builder
        .master("local[1]")
      .appName("StatTest")
      .config("spark.serializer", Constant.SPARK_SERIALIZER)
      .config("spark.shuffle.file.buffer","64K")
      .config("spark.reducer.maxSizeInFlight","96M")
      .config("hive.metastore.uris","thrift://172.25.21.2:9083")
      .enableHiveSupport()
      .getOrCreate()


    val df = ss.sql("select object_id_a,object_id_b,sim_score, time_sum, seq_same_length, seq_length_a, seq_length_b from MI_ACCOMPANY_DAY_CAR c where (seq_length_a >= 10 or seq_length_b >=10) and event_date='2020-09-03' and stat_date='2020-10-15' order by sim_score desc  limit 100")

    df.show()

    val subValueList:ListBuffer[Double] = ListBuffer()

    df.rdd.collect().foreach(f => {

      val object_id_a = f.get(0).toString
      val object_id_b = f.get(1).toString
      val seq_same_length = f.get(4).toString.toLong
      val tableDetail = new TableBean("CAR_DETECT_INFO","snowball",
        "jdbc:snowball://172.25.21.17:8123/pd_das?socket_timeout=30000000","default","","","pd_das")
      val properties = new Properties()
      properties.put("numPartitions", "1")
      properties.put("jdbc.url", tableDetail.url)
      properties.put("username", tableDetail.userName)
      properties.put("password", tableDetail.password)

      val database = DataBaseFactory(ss, properties, tableDetail.databaseType)

      val queryParamBean = new QueryBean()
      queryParamBean.isPage = false

      val execSql = s"SELECT t1.TOLLGATE_ID ,abs(t1.timestmp-t2.timestmp) as sub_time from  (SELECT HPHM, TOLLGATE_ID,toUnixTimestamp(parseDateTimeBestEffort(concat('20',toString(JGSK)))) as timestmp  FROM pd_das.CAR_DETECT_INFO where HPHM IN('${object_id_a}') AND JGRQ=200903) t1 join  (SELECT HPHM, TOLLGATE_ID,toUnixTimestamp(parseDateTimeBestEffort(concat('20',toString(JGSK)))) as timestmp  FROM pd_das.CAR_DETECT_INFO where HPHM IN ('${object_id_b}') AND JGRQ=200903) t2 on t1.TOLLGATE_ID= t2.TOLLGATE_ID where abs(t1.timestmp-t2.timestmp) <=60".stripMargin

      val queryRet = database
        .query(execSql, queryParamBean)

      val sub_val = (queryRet.count() * 1.0 - seq_same_length * 1.0)/(queryRet.count() * 1.0)
      subValueList.+=(sub_val)

    })

    subValueList.foreach(f => println(f*100))

    println("平均误差=" +(subValueList.sum/subValueList.size).formatted("%.4f").toDouble*100)

  }

}
