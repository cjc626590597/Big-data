package com.suntek.algorithm.common.database

import com.alibaba.druid.pool.DruidDataSource
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

object Druid {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val dsMap: mutable.Map[String, DruidDataSource] = mutable.HashMap[String, DruidDataSource]()
  var ds: DruidDataSource = _

  def setDs(driver: String,
            url: String,
            username: String,
            password: String): DruidDataSource ={
    try{
      ds = new DruidDataSource()
      ds.setDriverClassName(driver)
      ds.setUrl(url)
      ds.setUsername(username)
      ds.setPassword(password)
      ds.setInitialSize(20)
      ds.setMaxActive(20)
      ds.setMinIdle(3)
      ds.setMaxWait(60 * 60 * 1000L)
      ds.setValidationQuery("SELECT 1")
      ds.setTestWhileIdle(true)
      ds.setTestOnBorrow(false)
      ds.setTestOnReturn(false)
      ds
    }catch {
      case error: Exception =>
        logger.error(error.getMessage)
        ds
    }
  }

  def remove(url: String): Unit = {
    if(dsMap.contains(url)){
      dsMap.remove(url)
    }
  }

  def getDruid(driver: String,
               url: String,
               username: String,
               password: String): DruidDataSource = {
    if(dsMap.contains(url)){
      dsMap(url)
    }else{
      val ds = setDs(driver, url, username, password)
      logger.info("创建新链接")
      dsMap.put(url, ds)
      ds
    }
  }

}
