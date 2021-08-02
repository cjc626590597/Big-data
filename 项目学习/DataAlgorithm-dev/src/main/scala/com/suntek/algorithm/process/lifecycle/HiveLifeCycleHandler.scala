package com.suntek.algorithm.process.lifecycle

import java.sql.Statement
import java.time.{Instant, LocalDateTime}
import java.time.format.DateTimeFormatter

import com.alibaba.fastjson.{JSON, JSONObject}
import com.suntek.algorithm.common.conf.Param
import com.suntek.algorithm.common.util.HiveJDBCUtil
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
  * Created with IntelliJ IDEA.
  * Author: yuanweilin
  * Date: 2020-12-17 10:13
  * Description:表生命周期管理,目前只支持hive的分区表管理，且通过hivejdbc的方式进行删除分区，spark2.4不支持hive3以及不等式删除分区！
  */
object HiveLifeCycleHandler {
  val logger = LoggerFactory.getLogger(this.getClass)

  val HIVE_DROP_TABLE_PARTITION_SQL_TMPPLATE = "ALTER TABLE @TABLE_NAME@ DROP PARTITION(@PARTITION_LIST@)"
  val HIVE_ALTER_EXTERNAL_TABLE_PROPERTIES_SQL_TMPPLATE = "ALTER TABLE @TABLE_NAME@ SET TBLPROPERTIES ('external.table.purge'='true')"

  val MASTER = "local[1]"

  def execDropPartition(stat: Statement, tableName: String, tableType:String, partitions: String) = {

    if("external".equalsIgnoreCase(tableType)){
      //修改外部表属性
      val alterSql = HIVE_ALTER_EXTERNAL_TABLE_PROPERTIES_SQL_TMPPLATE
        .replaceAll("@TABLE_NAME@", tableName)

      stat.execute(alterSql)
    }

    val exeSql = HIVE_DROP_TABLE_PARTITION_SQL_TMPPLATE
      .replaceAll("@TABLE_NAME@", tableName)
      .replaceAll("@PARTITION_LIST@", partitions)

    logger.info(s"删除分区：${exeSql}")

    stat.execute(exeSql)

  }


  def execDropPartition(sparkSession:SparkSession, tablename: String, partitions: String) = {

    val exeSql = HIVE_DROP_TABLE_PARTITION_SQL_TMPPLATE
      .replaceAll("@TABLE_NAME@", tablename)
      .replaceAll("@PARTITION_LIST@", partitions)

    logger.info(s"删除分区：${exeSql}")

    sparkSession.sql(exeSql)

  }

  def process(param: Param): Unit = {

    if (null == param.keyMap("hiveTablesLifeCycle") || null == param.keyMap("hiveTablesLifeCycle").toString || "".equals(param.keyMap("hiveTablesLifeCycle").toString)) {
      logger.info("生命周期配置为空")
      return
    }

    val hiveJdbcUrl = s"jdbc:hive2://${param.keyMap("hive.nodes").toString}"
    val conn = HiveJDBCUtil.getConnnection(hiveJdbcUrl)
    val stat = conn.createStatement()

    val tables = JSON.parseArray(param.keyMap("hiveTablesLifeCycle").toString).toArray(Array[JSONObject]())
    for (table <- tables) {

      try {
        val partitions = table.getJSONArray("partitions").toArray(Array[JSONObject]())
        val tableName = table.getString("name")
        val tableType = table.getOrDefault("type","managed").toString //默认为托管表
        var partitionsList = List[String]()

        //组装partition
        for (partition <- partitions) {
          val partitionType = partition.getString("type")
          val partitionName = partition.getString("name")
          var partitionPartSql = ""
          try {
            partitionType match {
              case "datetime" =>
                val format = partition.getOrDefault("format", "yyyyMMdd").toString
                val days = partition.getOrDefault("days", "30").toString.toInt
                val maxTimeMills = System.currentTimeMillis - days * 24 * 60 * 60 * 1000L
                val maxDateTime = DateTimeFormatter.ofPattern(format).format(LocalDateTime.ofInstant(Instant.ofEpochMilli(maxTimeMills), param.zoneId))
                partitionPartSql = s"${partitionName}<='${maxDateTime}'"
              case "string" =>
                val value = partition.getString("value")
                if (null == value || "".equals(value)) {
                  throw new Exception(s"表${tableName}的分区${partitionName}配置的value不能为空，请检查！...跳过该表的生命周期管理！")
                } else {
                  partitionPartSql = s"${partitionName}='${value}'"
                }

              case "number" =>
                val value = partition.getInteger("value")
                if (null == value) {
                  throw new Exception(s"表${tableName}的分区${partitionName}配置的value不能为空，请检查！...跳过该表的生命周期管理！")
                } else {
                  partitionPartSql = s"${partitionName}=${value}"
                }

              case _ =>
                val value = partition.getString("value")
                if (null == value || "".equals(value)) {
                  throw new Exception(s"表${tableName}的分区${partitionName}配置的value不能为空，请检查！...跳过该表的生命周期管理！")
                } else {
                  partitionPartSql = s"${partitionName}='${value}'"
                }

            }
            partitionsList = partitionsList :+ partitionPartSql
          } catch {
            case ex: Exception =>
              throw ex
          }
        }

        //执行SQL删除分区
        execDropPartition(stat, tableName, tableType, partitionsList.mkString(","))

      } catch {
        case ex: Exception =>
          logger.error("生命周期管理异常：", ex)
      }

    }

    HiveJDBCUtil.close(conn, stat)
  }

  def readConf(): String = {
    val path: String = System.getenv("ALG_DEPLOY_HOME")
    var commonConfFilePath = s"$path/conf/hive-tables-lifecycle.json"
    var conf = new java.io.File(commonConfFilePath)
    if (!conf.exists()) {
      commonConfFilePath = "/opt/data-algorithm/conf/hive-tables-lifecycle.json"
      conf = new java.io.File(commonConfFilePath)
    }
    logger.info("hive-tables-lifecycle.json path = " + conf.getAbsolutePath)
    val confJson = FileUtils.readFileToString(conf,"UTF-8")
    confJson
  }

  def main(args: Array[String]): Unit = {

    var jsonStr = ""
    if(args == null || args.length == 0 || args(0) == null || "".equals(args(0))){
      jsonStr = "local#--#"+readConf()
    }else{
      jsonStr = args(0)

    }

//    val jsonStr = "local[1]#--#{\"analyzeModel\":{\"params\":[{\"cnName\":\"hive分区表生命周期管理\",\"enName\":\"hiveTablesLifeCycle\",\"desc\":\"hive分区表生命周期管理:只支持分区表生命周期管理,时间分区只保留最近多少天数据\",\"value\":[{\"name\":\"tb_test_drop_partition\",\"partitions\":[{\"name\":\"p1\",\"type\":\"datetime\",\"format\":\"yyyyMMdd\",\"days\":30},{\"name\":\"p2\",\"type\":\"number\",\"value\":\"0\"}]},{\"name\":\"tb_test_drop_partition2\",\"partitions\":[{\"name\":\"p1\",\"type\":\"datetime\",\"format\":\"yyyyMMdd\",\"days\":30},{\"name\":\"p2\",\"type\":\"number\",\"value\":\"1\"},{\"name\":\"p3\",\"type\":\"string\",\"value\":\"test\"}]}]}],\"mainClass\":\"com.suntek.algorithm.process.lifecycle.HiveLifeCycleController\",\"modelId\":0,\"modelName\":\"生命周期管理\",\"stepId\":0,\"taskId\":0,\"batchId\":0,\"descInfo\":\"\"}}"

    val param = new Param(jsonStr)

    process(param)
  }

}
