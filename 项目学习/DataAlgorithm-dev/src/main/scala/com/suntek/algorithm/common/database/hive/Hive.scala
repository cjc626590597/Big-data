package com.suntek.algorithm.common.database.hive

import com.suntek.algorithm.common.bean.{DataBaseBean, QueryBean, SaveParamBean}
import com.suntek.algorithm.common.conf.Constant
import com.suntek.algorithm.common.database.DataBase
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * @author zhy
  * @date 2020-9-1 9:06
  */
class Hive (ss: SparkSession, dataBaseBean: DataBaseBean) extends DataBase{
  val logger = LoggerFactory.getLogger(this.getClass)

  def getDataBaseBean: DataBaseBean = dataBaseBean

  /**
    * 生成sql
    */
  def makeSql(sql: String, list: List[Any]): String = {
    var i = 0
    val retSql = ArrayBuffer[String]()
    sql.foreach(v=>{
      if(v.equals('?')) {
        retSql.append(s"'${list(i).toString}'")
        i += 1
      }else retSql.append(s"'${v.toString}'")
    })
    retSql.mkString("")
  }

  def query(sql: String,
            queryParam: QueryBean)
  : DataFrame = {
    logger.info("execSql = "+ sql)
    val ret = ss.sql(s"$sql")
    ret
  }

  /**
    * 保存数据
    *
    */
  def save(saveParamBean: SaveParamBean,
           dataForDataFrame: DataFrame): Boolean = {
    //    if(dataForDataFrame.isEmpty) {
    //      logger.info(s"result data is empty!")
    //      return false
    //    }
    if (!saveParamBean.checkHiveSpark()) {
      throw new Exception("配置有误")
    }
    //进行分区重置
    var df = dataForDataFrame
    if (saveParamBean.numPartitions > 0) {
      df = dataForDataFrame.coalesce(saveParamBean.numPartitions)
    }

    val tableName = saveParamBean.tableName
    val params = saveParamBean.params
    val database = saveParamBean.dataBaseBean.dataBaseName
    val outputType = saveParamBean.outputType
    val partitions = saveParamBean.partitions.map(v => s"${v._1}='${v._2}'").mkString(",")
    val tmpTable = s"${tableName}_${System.currentTimeMillis}_${Random.nextInt(10000)}"
    // ss.sql(s"use $database")
    var sql = ""
    outputType match {
      case Constant.APPEND =>
        sql =
          s"""
             |INSERT INTO TABLE $tableName ${if (partitions.isEmpty) "" else s"partition ($partitions)"}
             |SELECT $params FROM $tmpTable
      """.stripMargin

      case Constant.OVERRIDE =>
        sql =
          s"""
             |INSERT OVERWRITE TABLE $tableName ${if (partitions.isEmpty) "" else s"partition ($partitions)"}
             |SELECT $params FROM $tmpTable
      """.stripMargin

      case Constant.OVERRIDE_DYNAMIC => //全量且动态分区方式（避免产生空分区）
        val partitionCols = saveParamBean.partitions.map(_._1).mkString(",")
        val partitionSql = saveParamBean.partitions.map(p => s" '${p._2}' AS ${p._1}").mkString(",")
        //拼接SQL：在原有基础上拼接动态分区的sql
        sql =
          s"""
             |INSERT OVERWRITE TABLE $tableName ${if (partitions.isEmpty) "" else s"partition ($partitionCols)"}
             |SELECT $params ${if (partitions.isEmpty) "" else s",$partitionSql"} FROM $tmpTable
      """.stripMargin
        ss.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        ss.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

      case Constant.OVERRIDE_APPEND => //增量且动态分区方式（避免产生空分区）
        val partitionCols = saveParamBean.partitions.map(_._1).mkString(",")
        val partitionSql = saveParamBean.partitions.map(p => s" '${p._2}' AS ${p._1}").mkString(",")
        //拼接SQL：在原有基础上拼接动态分区的sql
        sql =
          s"""
             |INSERT INTO TABLE $tableName ${if (partitions.isEmpty) "" else s"partition ($partitionCols)"}
             |SELECT $params ${if (partitions.isEmpty) "" else s",$partitionSql"} FROM $tmpTable
      """.stripMargin
        ss.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        ss.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

      case _ => logger.error("入库类型有误")
        throw new Exception("入库类型有误")
    }

    logger.info(s"############# result save sql = ${sql}")

    try {
      if (sql.nonEmpty) {
        df.createTempView(tmpTable)
        ss.sql(sql)
      }
      logger.info(s"############# result save success tableName = $tableName")
      true
    } catch {
      case ex: Exception =>
        logger.error(s"############# result save failed tableName = $tableName tagName = ${saveParamBean.tagName}")
        logger.error(ex.toString)
        throw ex
    }
  }

  /**
    * 删除
    */
  override  def delete(sql: String,
             list: Array[Any],
             saveParamBean: SaveParamBean = null)
  : Boolean = {
    val deleteSql = makeSql(sql, list.toList)
    ss.sql(deleteSql)
    try {
      val execSql = makeSql(sql, list.toList)
      logger.info("delete sql = " + execSql)
      ss.sql(execSql)
      true
    } catch {
      case ex: Exception =>
        logger.error(ex.toString)
        throw ex
    }
  }
}
