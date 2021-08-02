package com.suntek.algorithm

import java.sql.PreparedStatement

import com.alibaba.druid.pool.{DruidDataSource, DruidPooledConnection}
import com.suntek.algorithm.common.bean.SaveParamBean
import com.suntek.algorithm.common.conf.Constant
import com.suntek.algorithm.common.util.DruidUtil
import com.suntek.algorithm.common.util.DruidUtil.{dsMap, logger}
import org.apache.spark.sql.DataFrame
import org.junit.Test

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * @author zhy
  * @date 2020-9-10 10:45
  */
class SaveSnowball {

  def createDs(url: String, userName: String, password: String, driver: String): DruidDataSource = {
    var ds: DruidDataSource = null
    try {
      logger.info("开启一个<{}> Druid Connection",url)
      ds = new DruidDataSource()
      ds.setDriverClassName(driver)
      ds.setUrl(url)
      ds.setUsername(userName)
      ds.setPassword(password)
      ds.setInitialSize(2)
      ds.setMaxActive(10)
      ds.setMinIdle(3)
      ds.setMaxWait(3000)
      ds.setValidationQuery("SELECT 1")
      ds.setTestWhileIdle(true)
      ds.setTestOnBorrow(false)
      ds.setTestOnReturn(false)
    }catch {
      case error: Exception =>
        logger.error("Error Create <"+url+"> Druid Connection",error)
    }
    dsMap += (url -> ds)
    ds

  }
  def saveForJdbcCar(data: ArrayBuffer[ArrayBuffer[String]])
  :Unit = {
    val db = createDs("jdbc:snowball://172.25.21.17:8123/pd_das?socket_timeout=30000000", "default", "", "com.inforefiner.snowball.SnowballDriver")

    var conn: DruidPooledConnection = null
    var statement: PreparedStatement = null
    try{
      conn = db.getConnection
      conn.setAutoCommit(false)
      // 插入数据
      var k = 0
      //val insertSql = s"insert into TB_DEVICE_ROUND(DEVICE_ID1, DEVICE_ID2) values (?, ?)"
      // JGSK, HPHM, TOLLGATE_ID, INFO_ID
      //200817081224	鄂J3T521	440114624191001064
      val insertSql = s"insert into CAR_DETECT_INFO(JGSK, HPHM, TOLLGATE_ID, JGRQ, DEVICE_ID) values (?, ?, ?, ?, ?)"
      //val insertSql = s"insert into TB_STRENGTHEN_WIFI(MAC,CREATE_TIME,EQUID,INFO_ID) values (?, ?, ?, ?)"
      statement = conn.prepareStatement(insertSql)
      data.foreach(v=>{
        statement.setObject(1, v(0))
        statement.setObject(2, v(1))
        statement.setObject(3, v(2))
        statement.setObject(4, v(3))
        statement.setObject(5, v(4))
        //  statement.setObject(4, v(3))
        statement.addBatch()
      })
      statement.executeBatch()
      conn.commit()
    } catch {
      case ex: Exception =>
        throw ex
    }finally {
      if(statement != null) statement.close()
      if(conn != null) conn.close()
    }
  }

  def saveForJdbc(data: ArrayBuffer[ArrayBuffer[String]])
  :Unit = {
    val db = createDs("jdbc:snowball://172.25.21.17:8123/pd_das?socket_timeout=30000000", "default", "", "com.inforefiner.snowball.SnowballDriver")

    var conn: DruidPooledConnection = null
    var statement: PreparedStatement = null
    try{
      conn = db.getConnection
      conn.setAutoCommit(false)
      // 插入数据
      var k = 0
      //val insertSql = s"insert into TB_DEVICE_ROUND(DEVICE_ID1, DEVICE_ID2) values (?, ?)"
      //val insertSql = s"insert into TB_STRENGTHEN_WIFI(MAC,CREATE_TIME,EQUID) values (?, ?, ?)"
      val insertSql = s"insert into TB_STRENGTHEN_WIFI_IMSI(IMSI,CREATE_TIME,EQUID,INFO_ID) values (?, ?, ?, ?)"
      statement = conn.prepareStatement(insertSql)
      data.foreach(v=>{
        statement.setObject(1, v(0))
        statement.setObject(2, v(1))
        statement.setObject(3, v(2))
        statement.setObject(4, v(3))
        statement.addBatch()
      })
      statement.executeBatch()
      conn.commit()
    } catch {
      case ex: Exception =>
        throw ex
    }finally {
      if(statement != null) statement.close()
      if(conn != null) conn.close()
    }
  }

  @Test
  def SaveData(): Unit = {
    val source = Source.fromFile("F:\\code\\git\\dev\\algorithm\\dataalgorithm\\src\\main\\resources\\data\\yan\\1.log", "UTF-8")
    var i = 0
    var ret = new ArrayBuffer[ArrayBuffer[String]]()
    source.getLines().foreach(v=>{
      if(i == 10000){
        i = 0
        saveForJdbc(ret)
        ret = new ArrayBuffer[ArrayBuffer[String]]()
        val buffer = new ArrayBuffer[String]()
        v.split("\t").foreach(v=> buffer.append(v.trim))
        if(buffer.size >= 4 && !"".equals(buffer(0))){
          ret.append(buffer)
        }
      }else{
        i = i + 1
        val buffer = new ArrayBuffer[String]()
        v.split("\t").foreach(v=> buffer.append(v.trim))
        //	2020-08-22 23:42:51	20180305	616251721904170609
        //a4:50:46:78:a2:85	2020-08-22 23:26:45	20180306	616251721928744742
        if(buffer.size >= 4 && !"".equals(buffer(0))){
          ret.append(buffer)
        }

      }
    })
    if(ret.size >0){
      saveForJdbc(ret)
    }

  }

  @Test
  def SaveDataCar(): Unit = {
    val source = Source.fromFile("F:\\code\\git\\dev\\algorithm\\dataalgorithm\\src\\main\\resources\\cdi_0822.bat", "UTF-8")
    var i = 0
    var ret = new ArrayBuffer[ArrayBuffer[String]]()
    source.getLines().foreach(v=>{
      if(i == 10000){
        i = 0
        saveForJdbcCar(ret)
        ret = new ArrayBuffer[ArrayBuffer[String]]()
        val buffer = new ArrayBuffer[String]()
        val list = v.split("\t")
        buffer.append(list(0).trim)
        buffer.append(list(1).trim)
        buffer.append(list(2).trim)
        buffer.append(list(0).trim.substring(0, 6))
        buffer.append(list(2).trim)
        ret.append(buffer)
      }else{
        i = i + 1
        val buffer = new ArrayBuffer[String]()
        val list = v.split("\t")
        buffer.append(list(0).trim)
        buffer.append(list(1).trim)
        buffer.append(list(2).trim)
        buffer.append(list(0).trim.substring(0, 6))
        buffer.append(list(2).trim)
        ret.append(buffer)
      }
    })
    if(ret.size >0){
      saveForJdbcCar(ret)
    }

  }


}
