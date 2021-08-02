package com.suntek.algorithm.common.bean

import org.slf4j.{Logger, LoggerFactory}

import scala.beans.BeanProperty

@SerialVersionUID(2L)
class DataBaseBean extends Serializable {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  // 数据库
  @BeanProperty
  var dataBaseName: String =""

  // 分区数
  @BeanProperty
  var numPartitions: Int = 10

  // 数据库url
  @BeanProperty
  var url: String = ""

  // 数据库驱动
  @BeanProperty
  var driver: String = ""

  // 数据库用户名
  @BeanProperty
  var username: String = ""

  // 数据库密码
  @BeanProperty
  var password: String = ""

  def this(url: String,
           username: String,
           password: String,
           numPartitions: Int
          ) {
    this() //调用主构造器
    this.url = url
    this.username = username
    this.password = password
    this.numPartitions = numPartitions
  }

  def isEmpty: Boolean ={
    if(this.username.isEmpty){
      logger.error("用户名(username)不能为空")
      return true
    }
    if(this.password.isEmpty){
      logger.warn("密码(password)不能为空")
    }
    if(this.url.isEmpty){
      logger.error("数据库url(url)不能为空")
      return true
    }
    false
  }
}
