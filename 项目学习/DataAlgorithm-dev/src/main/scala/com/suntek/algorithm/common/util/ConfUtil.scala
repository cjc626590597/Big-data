package com.suntek.algorithm.common.util

import java.io.{BufferedInputStream, FileInputStream, InputStreamReader}
import java.util.Properties

import org.slf4j.LoggerFactory

import scala.collection.mutable

object ConfUtil {

  val logger = LoggerFactory.getLogger(this.getClass)

  def loadPropertyFromFile(): Properties = {
   //val confFile = "F:\\code\\data-fusion\\src\\main\\resources\\common.conf"
    val confFile = s"${System.getProperty("config.location")}/common.conf"
    val configStream = new BufferedInputStream(new FileInputStream(confFile))
    if(configStream == null){
      throw new Exception("cannot find " + confFile)
    }
    val properties = new Properties()
    properties.load(configStream)
    configStream.close()
    return properties
  }


  /**
    * 加载配置文件
    * @param json json
    * @param file 配置文件路径
    * @return
    */
  def loadPropertyFromFile(file: String = ""): mutable.Map[String, String] = {
    val conf = new java.io.File(file)
    val kvMap: mutable.Map[String, String] = mutable.Map[String, String]()
    logger.info(s"******* 加载配置文件：${file}；exists：${conf.exists()}")
    if(conf.exists()){
      readConf(file, kvMap)
    }
    kvMap
  }


  /**
    * 读取配置文件存入map中
    * @param file
    * @param kvMap
    */
  def readConf(file: String,
               kvMap: mutable.Map[String, String]): Unit = {
    val property = new Properties()
    val istr = new InputStreamReader(new FileInputStream(file), "UTF-8")
    property.load(istr)
    istr.close()

    for (elem <- property.stringPropertyNames.toArray(new Array[String](0))) {
      val rsl = property.getProperty(elem).replaceAll("\"", "").trim
      logger.info("property is: (" + elem + "," + rsl + ")")
      if(!kvMap.contains(elem)) kvMap.put(elem, rsl)
    }
  }
}
