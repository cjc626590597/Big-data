package com.suntek.algorithm.common.util

import java.util.Properties

import com.suntek.algorithm.common.bean.TableBean
import com.suntek.algorithm.common.conf.Param

/**
  * Created with IntelliJ IDEA.
  * Author: yuanweilin
  * Date: 2020-10-17 15:06
  * Description:
  */
object PropertiesUtil {

  def genJDBCProperties(param: Param): Properties = {
    return genJDBCProperties(param.keyMap.getOrElse("jdbc.url", ""), param.keyMap.getOrElse("jdbc.username", ""),
      param.keyMap.getOrElse("jdbc.password", ""), param.keyMap.getOrElse("NUM_PARTITIONS", "10"))
  }

  def genJDBCProperties(jdbcUrl:Object, userName: Object, password: Object, numPartitions: Object): Properties = {
    val properties = new Properties()
    properties.put("jdbc.url", jdbcUrl)
    properties.put("username", userName)
    properties.put("password", password)
    properties.put("numPartitions", numPartitions)
    return properties
  }

  def genJDBCProperties(tableBean: TableBean, numPartitions:String = "10"): Properties = {
    return genJDBCProperties( tableBean.url ,  tableBean.userName, tableBean.password, numPartitions)
  }

}
