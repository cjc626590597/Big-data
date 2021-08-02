package com.suntek.algorithm

import java.util.{Properties, UUID}

import com.suntek.algorithm.common.bean.{DataBaseBean, QueryBean}
import com.suntek.algorithm.common.database.pgsql.PgSQL
import com.suntek.algorithm.common.database.mysql.MySQL
import com.suntek.algorithm.common.database.snowball.Snowball
import org.apache.spark.sql.{DataFrameReader, SQLContext, SparkSession}
import org.junit.{Assert, Test}

import scala.collection.mutable.ArrayBuffer

class SqlReadTest extends Assert{

  @Test
  def sqlCommon(): Unit = {
    val  session = SparkSession
      .builder
      .master("local[2]")
      .appName(s"suntek-offline${UUID.randomUUID()}")
      .config("spark.sql.shuffle.partitions", 5)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val properties = new Properties()
    properties.put("user", "videoweb")
    properties.put("password", "suntek")
    properties.put("driver", "org.postgresql.Driver")
    properties.put("numPartitions", "2")
    val w = session.getOrCreate()
      .sqlContext
      .read
      .format("jdbc")
      .option("url", "jdbc:postgresql://172.25.21.18:5432/dolphinscheduler")
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", s"(select * from t_ds_schedules) AS T")
      .option("user", "videoweb")
      .option("password", "suntek")
      .option("partitionColumn", "id")
      .option("lowerBound", 1)
      .option("upperBound", 7)
      .option("numPartitions", 2)
      .option("fetchsize", 1)
      .load()
    w.show()
  }

  @Test
  def sqlPGSQL(): Unit = {
    val session = SparkSession
      .builder
      .master("local[2]")
      .appName(s"suntek-offline${UUID.randomUUID()}")
      .config("spark.sql.shuffle.partitions", 5)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val dataBaseBean = new DataBaseBean()
    dataBaseBean.setUsername("videoweb")
    dataBaseBean.setPassword("suntek")
    dataBaseBean.setDriver("org.postgresql.Driver")
    dataBaseBean.setUrl( "jdbc:postgresql://172.25.21.18:5432/dolphinscheduler")
    val pgsql = new PgSQL(session.getOrCreate(), dataBaseBean)
    val sql = "select * from t_ds_schedules"
    val queryBean = new QueryBean()
    queryBean.batchSize=1
    val ret = pgsql.query(sql, queryBean)
    ret.show()
  }

  @Test
  def sqlSnowBallSQL(): Unit = {
    val session = SparkSession
      .builder
      .master("local[2]")
      .appName(s"suntek-offline${UUID.randomUUID()}")
      .config("spark.sql.shuffle.partitions", 5)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val dataBaseBean = new DataBaseBean()
    dataBaseBean.setUsername("default")
    dataBaseBean.setPassword("")
    dataBaseBean.setDriver("com.inforefiner.snowball.SnowballDriver")
    dataBaseBean.setUrl( "jdbc:snowball://172.25.21.22:8123/pd_das")

    val snowball = new Snowball(session.getOrCreate(), dataBaseBean)
    val sql = "select * from MI_SCENE_TAG_STAT_D"
    val queryBean = new QueryBean()
    queryBean.batchSize=1
    queryBean.orderByParam="CERT_ID"
    val ret = snowball.query(sql, queryBean)
    ret.show()
  }

  @Test
  def sqlMySQL(): Unit = {
    val session = SparkSession
      .builder
      .master("local[2]")
      .appName(s"suntek-offline${UUID.randomUUID()}")
      .config("spark.sql.shuffle.partitions", 5)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val dataBaseBean = new DataBaseBean()
    dataBaseBean.setUsername("videoweb")
    dataBaseBean.setPassword("suntek")
    dataBaseBean.setDriver("com.mysql.jdbc.Driver")
    dataBaseBean.setUrl( "jdbc:mysql://172.25.21.5:3306/pd_scemanage?characterEncoding=UTF-8&autoReconnect=true&useSSL=false")
    dataBaseBean.setNumPartitions(2)
    val mysql = new MySQL(session.getOrCreate(), dataBaseBean)
    val sql = "select * from VPLUS_SCENE_INFO"
    val queryBean = new QueryBean()
    queryBean.batchSize=1
    val ret = mysql.query(sql, queryBean)
    ret.show()
  }
}
