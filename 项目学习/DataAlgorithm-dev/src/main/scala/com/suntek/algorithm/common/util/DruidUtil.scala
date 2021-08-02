package com.suntek.algorithm.common.util

import com.alibaba.druid.pool.DruidDataSource
import com.suntek.algorithm.common.bean.DataBaseBean
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

object DruidUtil{

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val dsMap: mutable.Map[String, DruidDataSource] = mutable.HashMap[String, DruidDataSource]()

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

  def getDruidDataSource(dataBaseBean: DataBaseBean, isJudgeExistDs: Boolean = true): DruidDataSource = {
    if (isJudgeExistDs) {
      if (dsMap.contains(dataBaseBean.url)) {
        dsMap(dataBaseBean.url)
      } else {
        createDs(dataBaseBean.url, dataBaseBean.username, dataBaseBean.password, dataBaseBean.driver)
      }
    } else {
      createDs(dataBaseBean.url, dataBaseBean.username, dataBaseBean.password, dataBaseBean.driver)
    }
  }
}