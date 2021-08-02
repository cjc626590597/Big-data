package com.suntek.algorithm.common.bean

import org.slf4j.{Logger, LoggerFactory}

import scala.beans.BeanProperty

/**
  * 保存参数bean
  * Author: zhangyan
  * Date: 2020-01-04 10:32
  * Description:
  */
@SerialVersionUID(2L)
class SaveParamBean extends Serializable{
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  @BeanProperty
  // 任务ID
  var taskId: String = ""

  // 表名
  @BeanProperty
  var tableName: String = ""

  // 标签Id
  @BeanProperty
  var tagId: String = ""

  // 标签名称
  @BeanProperty
  var tagName: String = ""

  // 插入方式，1 全量 2 增量
  @BeanProperty
  var outputType: Int = 0

  // 插入sql
  @BeanProperty
  var insertSql: String = ""

  // 插入前准备sql的参数列表
  @BeanProperty
  var preParams: Array[Any] = Array[Any]()

  // 插入前准备sql
  @BeanProperty
  var preSql: String = ""

  // 数据分隔符
  @BeanProperty
  var linesterminated: String = "\n"

  // 数据库相关
  @BeanProperty
  var dataBaseBean: DataBaseBean = new DataBaseBean()

  // 分区(hive专用)
  @BeanProperty
  var partitions: Array[(String, String)] = Array[(String, String)]()

  // 表字段字符串
  @BeanProperty
  var params: String = ""

  //分区数：针对hive/spark
  @BeanProperty
  var numPartitions: Int = 10

  // 针对mppdb 自动创建分区
  var isCreateMppdbPartition = false
  @BeanProperty
  var mppdbPartition: String = ""
  @BeanProperty
  var mppdbDate: Long = 0L


  def this(tableName: String,
           outputType: Int,
           dataBaseBean: DataBaseBean
          ) {
    this() //调用主构造器
    this.tableName = tableName
    this.outputType = outputType
    this.dataBaseBean = dataBaseBean
  }

  def check(): Boolean ={
    if(this.dataBaseBean.isEmpty) {
      logger.error("数据库数据库相关信息不能为空")
      return false
    }
    if(this.tableName.isEmpty) {
      logger.error("表名(tableName)不能为空")
      return false
    }
    true
  }

  def checkHiveSpark(): Boolean ={

//    if(this.dataBaseBean.driver.isEmpty){
//      logger.error("驱动(driver)不能为空")
//      return false
//    }

//    if(this.partitions.isEmpty){
//      logger.error("数据库分区不能为空")
//      return false
//    }
    if(this.params.isEmpty){
      logger.error("表字段字符串不能为空")
      return false
    }
    true
  }

  def checkMysqlJdbc(): Boolean ={
    if(!check()) return false

    if(this.dataBaseBean.driver.isEmpty){
      logger.error("驱动(driver)不能为空")
      return false
    }

    if(this.linesterminated.isEmpty){
      logger.error("数据分隔符不能为空")
      return false
    }
    if(this.insertSql.isEmpty){
      logger.error("插入语句不能为空")
      return false
    }
    true
  }

  def checkMysqlSpark(): Boolean ={
    if(!check()) return false
    if(this.dataBaseBean.driver.isEmpty){
      logger.error("驱动(driver)不能为空")
      return false
    }
    true
  }

  def checkSnowBall(): Boolean ={
    if(!check()) return {
      false
    }
    if(this.dataBaseBean.driver.isEmpty){
      logger.error("驱动(driver)不能为空")
      return false
    }
     true
  }

  def checkMppJdbc(): Boolean ={
    if(!check()) {
      return false
    }

    if(this.dataBaseBean.driver.isEmpty){
      logger.error("驱动(driver)不能为空")
      return false
    }

    if(this.linesterminated.isEmpty){
      logger.error("数据分隔符不能为空")
      return false
    }
    if(this.params.isEmpty){
      logger.error("表字段字符串不能为空")
      return false
    }
    true
  }

  def checkMppSpark(): Boolean ={
    if(!check()) {
      return false
    }
    if(this.dataBaseBean.driver.isEmpty){
      logger.error("驱动(driver)不能为空")
      return false
    }
    true
  }
}
