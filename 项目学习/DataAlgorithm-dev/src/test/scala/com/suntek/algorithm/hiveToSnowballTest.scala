package com.suntek.algorithm

import java.io.File
import java.text.SimpleDateFormat
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSource
import com.suntek.algorithm.common.bean.DataBaseBean
import com.suntek.algorithm.common.conf.Constant
import com.suntek.algorithm.common.util.{DruidUtil, SparkUtil}
import org.junit.{Assert, Test}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * @author zhy
  * @date 2021-1-11 15:55
  */
class hiveToSnowballTest extends Assert{
  @Test
  def wifiInfo1(): Unit ={
    val dataBaseBean: DataBaseBean = new DataBaseBean()
    dataBaseBean.url = "jdbc:snowball://172.25.21.147:8123/pd_dts"
    dataBaseBean.driver = Constant.SNOWBALL_DRIVER
    dataBaseBean.username = "default"
    dataBaseBean.password = ""
    val prop = new Properties()
    prop.setProperty("driver", dataBaseBean.driver)
    prop.setProperty("url",  dataBaseBean.url)
    prop.setProperty("user", dataBaseBean.username)
    prop.setProperty("password", dataBaseBean.password)
    val  db = DruidUtil.getDruidDataSource(dataBaseBean)
    val conn = db.getConnection
    val statement1 = conn.createStatement()
    val sql = "select JGSK, MSISDN, IMSI, DEVICE_ID, LONGITUDE,LATITUDE,MAC,INFO_ID from EFENCE_DETECT_INFO_MIN"
    val rs = statement1.executeQuery(sql)
    while (rs.next()){
      val jgsk = rs.getObject(1).toString
      val msisdn = rs.getObject(2).toString
      val imsi = rs.getObject(3).toString
      val deviceid = rs.getObject(4).toString
      val longitude = rs.getObject(5).toString
      val latitude = rs.getObject(6).toString
      val mac = rs.getObject(7).toString
      val infoid = rs.getObject(8).toString
      val sql_temp1 = s"INSERT INTO EFENCE_DETECT_INFO (JGSK, MSISDN, IMSI, DEVICE_ID, LONGITUDE,LATITUDE,MAC,INFO_ID) " +
        s"values('${jgsk}','${msisdn}','${imsi}','${deviceid}','${longitude}','${latitude}','${mac}','${infoid}')"
      println(sql_temp1)
      statement1.execute(sql_temp1)
    }
    conn.commit()
    statement1.close()
    conn.close()
  }

  @Test
  def carInfo1(): Unit ={
    val dataBaseBean: DataBaseBean = new DataBaseBean()
    dataBaseBean.url = "jdbc:snowball://172.25.21.147:8123/pd_dts"
    dataBaseBean.driver = Constant.SNOWBALL_DRIVER
    dataBaseBean.username = "default"
    dataBaseBean.password = ""
    val prop = new Properties()
    prop.setProperty("driver", dataBaseBean.driver)
    prop.setProperty("url",  dataBaseBean.url)
    prop.setProperty("user", dataBaseBean.username)
    prop.setProperty("password", dataBaseBean.password)
    val  db = DruidUtil.getDruidDataSource(dataBaseBean)
    val conn = db.getConnection
    val statement1 = conn.createStatement()
    val sql = "SELECT concat('200910',SUBSTRING(cast(JGSK as String),7, 12)) as JGSK1, HPHM, HPYS, DEVICE_ID, ORIGINAL_DEVICE_ID, CLLX, INFO_ID, PIC_PLATE, PIC_ABBREVIATE, \nDEVICE_ADDR, 200910 as JGRQ, JGSJ, TOLLGATE_ID " +
      "from pd_dts.CAR_DETECT_INFO_MIN WHERE JGSK>= 200902000000 AND JGSK< 200902235959"
    val rs = statement1.executeQuery(sql)
    var num = 0
    while (rs.next()){
      val jgsk = rs.getObject(1).toString
      val hphm = rs.getObject(2).toString
      val hpys = rs.getObject(3).toString
      val deviceid = rs.getObject(4).toString
      val originalDeviceid = rs.getObject(5).toString
      val cllx = rs.getObject(6).toString
      val infoid = rs.getObject(7).toString
      val picPlate = rs.getObject(8).toString
      val picAbbr = rs.getObject(9).toString
      val deviceAddr = rs.getObject(10).toString
      val jgrq = rs.getObject(11).toString
      val jgsj = rs.getObject(12).toString
      val tollgateId = rs.getObject(13).toString
      val sql_temp1 = s"INSERT INTO CAR_DETECT_INFO ( JGSK, HPHM, HPYS, DEVICE_ID, ORIGINAL_DEVICE_ID, CLLX, INFO_ID, PIC_PLATE, PIC_ABBREVIATE, " +
        "DEVICE_ADDR, JGRQ, JGSJ, TOLLGATE_ID) " +
        s"values('${jgsk}','${hphm}','${hpys}','${deviceid}','${originalDeviceid}','${cllx}','${infoid}','${picPlate}'," +
        s"'${picAbbr}','${deviceAddr}','${jgrq}','${jgsj}','${tollgateId}')"
      statement1.execute(sql_temp1)
      println(sql_temp1)
      num = num + 1
      if(num >= 100){
        print(num)
        conn.commit()
        num = 0
      }
    }
    conn.commit()
    statement1.close()
    conn.close()
  }

  @Test
  def carInfo(): Unit ={
    val dataBaseBean: DataBaseBean = new DataBaseBean()
    dataBaseBean.url = "jdbc:snowball://172.25.21.147:8123/pd_dts"
    dataBaseBean.driver = Constant.SNOWBALL_DRIVER
    dataBaseBean.username = "default"
    dataBaseBean.password = ""
    val prop = new Properties()
    prop.setProperty("driver", dataBaseBean.driver)
    prop.setProperty("url",  dataBaseBean.url)
    prop.setProperty("user", dataBaseBean.username)
    prop.setProperty("password", dataBaseBean.password)
    val  db = DruidUtil.getDruidDataSource(dataBaseBean)
    val conn = db.getConnection
    val statement1 = conn.createStatement()
    val file1 = Source.fromFile("E:\\data\\yanjie.csv")

    var ziduan = file1.getLines.toList.head.split("\t")
    var k1 = 0
    val map1 =  mutable.HashMap[String, String]()
    map1.put("JGSK","JGSK")
    map1.put("HPHM","HPHM")
    map1.put("HPYS","HPYS")
    map1.put("DEVICE_ID","DEVICE_ID")
    map1.put("ORIGINAL_DEVICE_ID", "ORIGINAL_DEVICE_ID")
    map1.put("CLLX", "CLLX")
    map1.put("INFO_ID", "INFO_ID")
    map1.put("PIC_PLATE", "PIC_PLATE")
    map1.put("PIC_ABBREVIATE", "PIC_ABBREVIATE")
    map1.put("DEVICE_ADDR", "DEVICE_ADDR")
    map1.put("JGRQ", "JGRQ")
    map1.put("JGSJ", "JGSJ")
    map1.put("TOLLGATE_ID", "TOLLGATE_ID")
    val map =  mutable.HashMap[Int, String]()
    ziduan.foreach(v=>{
      if(map1.contains(v.toUpperCase)){
        map.put(k1, v.toUpperCase)
      }
      k1 = k1 + 1
    })
    val set1 = map.keySet.toList.sortBy(v=> v)
    var sb1 = new ArrayBuffer[String]()
    set1.foreach(v=>{
      val value = map.get(v).get
      sb1.append(value)
    })
    var sql_temp1 = s"INSERT INTO CAR_DETECT_INFO (${sb1.mkString(",")})"
    var sb = new ArrayBuffer[String]()

    val file = Source.fromFile("E:\\data\\CAR_DETECT_INFO.csv")
    var num = 0
    val statement = conn.createStatement()
    for (line <- file.getLines) {
      val params = line.split("\t")
      var k2 = 0
      var s = new ArrayBuffer[String]()
      params.foreach(v=>{
        if(map.contains(k2)){
          s.append(s"'${v}'")
        }
        k2 = k2 + 1
      })
      val value = s.mkString(",")
      val sql_temp = s"${sql_temp1} values(${value})"
      println(sql_temp)
      statement.execute(sql_temp)
      num = num + 1
      if(num >= 10000){
        print(num)
        conn.commit()
        num = 0
      }
    }
    statement.close()
    conn.commit()
    file.close()
  }

  @Test
  def wifiInfo(): Unit ={
    val dataBaseBean: DataBaseBean = new DataBaseBean()
    dataBaseBean.url = "jdbc:snowball://172.25.21.147:8123/pd_dts"
    dataBaseBean.driver = Constant.SNOWBALL_DRIVER
    dataBaseBean.username = "default"
    dataBaseBean.password = ""
    val prop = new Properties()
    prop.setProperty("driver", dataBaseBean.driver)
    prop.setProperty("url",  dataBaseBean.url)
    prop.setProperty("user", dataBaseBean.username)
    prop.setProperty("password", dataBaseBean.password)
    val  db = DruidUtil.getDruidDataSource(dataBaseBean)
    val conn = db.getConnection
    val statement1 = conn.createStatement()

    var ziduan = "0 DATETIME,1 HOMEAREA,2 MSISDN,3 IMSI,4 IMEI,5 TMSI,6 AREACODE,7 EQUID ,8 LONGITUDE ,9 " +
      " LATITUDE,10 MAC ,11  RSSI,12 LAC,13 INCTIMER,14 AREACODETIME ,15 SPCODE,16 NETWORK,17 ESN,18 PN ,19 " +
      " FREQUENCE ,20 SMSSENDSTATUS,21  SEQNUM ,22 INFO_ID,23 CREATE_TIME"
    var k1 = 0
    val map =  mutable.HashMap[Int, String]()
    map.put(22,"INFO_ID")
    map.put(7,"DEVICE_ID")
    map.put(2, "MSISDN")
    map.put(3, "IMSI")
    map.put(4, "IMEI")
    map.put(10, "MAC")
    map.put(0, "JGSK")
    map.put(8, "LONGITUDE")
    map.put(9, "LATITUDE")

    val set1 = map.keySet.toList.sortBy(v=> v)
    var sb1 = new ArrayBuffer[String]()
    set1.foreach(v=>{
      val value = map.get(v).get
      sb1.append(value)
    })
    var sql_temp1 = s"INSERT INTO EFENCE_DETECT_INFO (${sb1.mkString(",")})"
    var sb = new ArrayBuffer[String]()

    val file = Source.fromFile("E:\\data\\wifi.csv")
    var num = 0
    val dateFormat1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateFormat2 = new SimpleDateFormat("yyyyMMddHHmmss")
    val statement = conn.createStatement()
    var i = 0
    for (line <- file.getLines) {
      if(i > 0){
        val params = line.split("\t")
        val datetime = params(0)
        val longt = dateFormat1.parse(datetime).getTime
        val date1 = dateFormat2.format(longt)
        val date = date1.substring(2, date1.size)
        params(0) = date
        var k2 = 0
        var s = new ArrayBuffer[String]()
        params.foreach(v=>{
          if(map.contains(k2)){
            s.append(s"'${v}'")
          }
          k2 = k2 + 1
        })
        val value = s.mkString(",")
        val sql_temp = s"${sql_temp1} values(${value})"
        println(sql_temp)
        statement.execute(sql_temp)
        num = num + 1
        if(num >= 10000){
          print(num)
          conn.commit()
          num = 0
        }
      }
      i = i + 1
    }
    statement.close()
    conn.commit()
    file.close()
  }
}
