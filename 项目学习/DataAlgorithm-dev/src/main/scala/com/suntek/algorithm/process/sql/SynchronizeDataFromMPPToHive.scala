package com.suntek.algorithm.process.sql

import com.suntek.algorithm.common.bean.{DataBaseBean, QueryBean, SaveParamBean}
import com.suntek.algorithm.common.conf.{Constant, Param}
import com.suntek.algorithm.common.database.DataBaseFactory
import com.suntek.algorithm.common.database.mpp.Mpp
import com.suntek.algorithm.common.util.SparkUtil
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import java.text.SimpleDateFormat
import java.util.Properties

/**
 * Created with IntelliJ IDEA.
 * Author: liaojp
 * Date: 2021-02-01 11:12
 * Description:同步mpp数据导hive中
 * 支持表：
 * hive_face_detect_rl
 * efence_detect_info
 * car_detect_info
 *
 *
 */
//noinspection ScalaDocMissingParameterDescription
object SynchronizeDataFromMPPToHive {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)


  def synchronizeByJDBC(sparkSession: SparkSession,
                        sParam: SynchronizeDataFromMPPToHiveParam,
                        param: Param): Unit = {

    //通过datafram返回mpp查询结果
    val dataBaseBean = new DataBaseBean()
    dataBaseBean.setUsername(sParam.username) //suntek
    dataBaseBean.setPassword(sParam.password) //suntek@123
    dataBaseBean.setDriver(Constant.MPP_DRIVER)
    dataBaseBean.setUrl(sParam.url) //
    dataBaseBean.setNumPartitions(2)
    val mpp = new Mpp(sparkSession, dataBaseBean)
    val queryBean = new QueryBean()
    queryBean.batchSize = sParam.batchSize
    queryBean.isPage = true

    //获取天、小时的索引下标

    val formatStr = sParam.dataTimeFormat
    logger.info(s"formatStr: $formatStr")
    var dayStartIndex = 1
    if (formatStr.startsWith("yyyy")){
      dayStartIndex = 3
    }
    val dayIndex = formatStr.indexOf("H")
    val hourIndex = dayIndex + 1
    logger.info(s"dayIndex: $dayIndex")
    logger.info(s"hourIndex: $hourIndex")

    // 人脸数据
    if (sParam.tableName.toLowerCase == "hive_face_detect_rl") {

      val sql = s"""
                |select
                |      distinct
                |      substring(${sParam.mppPartitionKey}, $dayStartIndex, 6) as stay_date,
                |      substring(${sParam.mppPartitionKey}, $hourIndex, 2) as hour
                |from
                |      ${sParam.tableName}
                |where
                |      ${sParam.createTimeKey} >=${sParam.createTimeStartTime} and ${sParam.createTimeKey}< ${sParam.createTimeEndTime} and
                |      ${sParam.mppPartitionKey} >= ${sParam.preTime} and ${sParam.mppPartitionKey} < ${sParam.endTime}
                |order by
                |      stay_date,hour
       """.stripMargin

      val partitions = mpp.query(sql, queryBean)
                          .rdd
                          .map { row =>
                            try {
                              (row.get(0).toString, row.get(1).toString)
                            } catch {
                              case _: Exception => ("", "")
                            }
                          }
                          .filter(_._1.nonEmpty)
                          .collect()
                          .map(r => Array[(String, String)]((sParam.PARTITIONEDBYDAY, r._1), (sParam.PARTITIONEDBYHOUR, r._2)))

      if (partitions.nonEmpty) {
        logger.info(s"partitions length: ${partitions.length}")
        partitions.foreach(r => logger.info(s"${r.head._2}-${r.last._2}"))
        val sqlKey: String = s"mpp.${sParam.mppTableName}"
        partitions.foreach { p =>
          var sql: String = param.keyMap(sqlKey).toString.replace(sParam.STARTTIME, sParam.createTimeStartTime)
                                 .replace(sParam.ENDTIME, sParam.createTimeEndTime)
            sql += s" and substring(${sParam.mppPartitionKey}, $dayStartIndex, 6) = ${p.head._2} and substring(${sParam.mppPartitionKey}, $hourIndex, 2) = ${p.last._2}"
          val ret = mpp.query(sql, queryBean)
          val properties = new Properties()
          val database = DataBaseFactory(sparkSession, properties, Constant.HIVE)
          val saveParamBean = new SaveParamBean(sParam.tableName, Constant.APPEND, database.getDataBaseBean)
          saveParamBean.dataBaseBean = database.getDataBaseBean
          saveParamBean.partitions = p
          saveParamBean.params = ret.columns.mkString(",")
          saveParamBean.numPartitions = sParam.numPartitions
          database.save(saveParamBean, ret)
        }
      }
    }else {
        //保存导hive数据库
        val ret = mpp.query(sParam.sql, queryBean)
        val properties = new Properties()
        val database = DataBaseFactory(sparkSession, properties, Constant.HIVE)
        val saveParamBean = new SaveParamBean(sParam.tableName, Constant.APPEND, database.getDataBaseBean)
        saveParamBean.dataBaseBean = database.getDataBaseBean
        saveParamBean.partitions = sParam.partition
        saveParamBean.params = ret.columns.mkString(",")
        saveParamBean.numPartitions = sParam.numPartitions
        database.save(saveParamBean, ret)
      }
  }


    def synchronizeByGDS(sparkSession: SparkSession, sParam: SynchronizeDataFromMPPToHiveParam): Boolean = {
      false
    }


    def countTable(sparkSession: SparkSession, sParam: SynchronizeDataFromMPPToHiveParam): Long = {
      val partitionStr = if (sParam.partition.nonEmpty) {
        " AND " + sParam.partition.map {
          case (partitionKey, partitionValue) =>
            s"$partitionKey=$partitionValue"
        }
          .mkString(" AND ")
      } else ""
      val sql = s"SELECT COUNT(1) FROM ${sParam.tableName} WHERE 1=1  $partitionStr"
      logger.info(s"统计hive分区条数sql:[$sql]")
      sparkSession.sql(sql)
        .collect()
        .head
        .getLong(0)
    }

    /**
     * mpp同步数据导hive
     * 支持方式 jdbc
     *
     * @param param
     */
    def process(param: Param): Unit = {
      val sParam = new SynchronizeDataFromMPPToHiveParam(param)
      val master = sParam.master
      val sparkSession = SparkUtil.getSparkSession(master, s"SYNCHRONIZE DATA FROM MPP TO HIVE  ${sParam.batchId}  Job", param)
      //    val beforeInsertCount = countTable(sparkSession, sParam)

      synchronizeByJDBC(sparkSession, sParam, param)

      //
      //      sParam.method match {
      //        case sParam.JDBC => synchronizeByJDBC(sparkSession, sParam, param)
      ////        case sParam.GDS => synchronizeByGDS(sparkSession, sParam)
      //        case _ => throw new Exception("请输入 method 参数：[jdbc，gds]")
      //      }
      //    val afterInsertCount = countTable(sparkSession, sParam)
      //    logger.info(s"hive表${sParam.tableName}:插入分区：${sParam.partition.map(x=>s"${x._1}=${x._2}").mkString("/")}  " +
      //      s"插入前条数：$beforeInsertCount," +
      //      s"插入条数：${afterInsertCount-beforeInsertCount},现总条数：$afterInsertCount")
      sparkSession.close()
    }
}


  class SynchronizeDataFromMPPToHiveParam(param: Param, debug: Boolean = false) {
    val batchSize: Int = 50000

    val numPartitions: Int = param.keyMap.getOrElse("numPartitions", 20).toString.toInt

    // mpp同步sql，shotTime过滤的时间区间，单位：天
    val shotTimeRange: Int = param.keyMap.getOrElse("shotTimeRange", 15).toString.toInt


    // 加载mpp.properties配置文件
    val loadMppProperties: Unit = {
      var path = System.getenv(param.deployEnvName)
      if (debug) {
        path = this.getClass.getResource("/").getPath
      }
      var mppSqlFilePath = s"$path/conf/mpp.properties"
      var conf = new java.io.File(mppSqlFilePath)
      if (!conf.exists()) {
        mppSqlFilePath = "/opt/data-algorithm/conf/mpp.properties"
        conf = new java.io.File(mppSqlFilePath)
      }
      param.readConf(conf)
    }

    val JDBC = "jdbc"
    val GDS = "gds"

    //=================通过启动程序的json进行配置=================
    //mpp链接和密码
    //  val url: String = param.keyMap("url").toString
    //  val username: String = param.keyMap("username").toString
    //  val password: String = param.keyMap("password").toString
    val landDataBase: String = param.keyMap.getOrElse("landDataBase", "pd_dts").toString
    var queryParam = "characterEncoding=UTF-8&autoReconnect=true&useSSL=false&zeroDateTimeBehavior=convertToNull&serverTimezone=UTC"
    var url = s"jdbc:postgresql://${param.mppdbIp}:${param.mppdbPort}/$landDataBase?$queryParam"
    var username: String = param.mppdbUsername
    var password: String = param.mppdbPassword

    if (debug) {
      url = param.keyMap("url").toString
      username = param.keyMap("username").toString
      password = param.keyMap("password").toString
    }


    val batchId: String = param.keyMap("batchId").toString
    //mpp导数据至hive方式 jdbc,gds
    val method: String = param.keyMap("method").toString.toLowerCase

    //hive表名
    val tableName: String = param.keyMap("tableName").toString.toLowerCase
    //mpp表名
    val mppTableName: String = param.keyMap("mppTableName").toString.toLowerCase

    //=================通过启动程序的json进行配置=================


    //开始结束时间戳
    val batchIdDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")

    var startTimeStamp: Long = batchIdDateFormat.parse(batchId.substring(0, 10) + "0000").getTime - 1 * 60 * 60 * 1000L
    var endTimeStamp: Long = batchIdDateFormat.parse(batchId.substring(0, 10) + "0000").getTime


    //sql where条件的开始结束时间
    val STARTTIME = "@START_TIME@"
    val ENDTIME = "@END_TIME@"

    //hive时间分区
    val PARTITIONEDBYDAY = "stat_day"
    val PARTITIONEDBYHOUR = "hour"


    /**
     * 解析mpp.properties文件，获得sql以及hive相应的partition字段。
     */
    val sqlKey: String = s"mpp.$mppTableName"
    val partitionKey = s"$sqlKey.hive.partition"
    val pValue: String = param.keyMap(partitionKey).toString
    val splits: Array[String] = pValue.split(",")

    val dataTimeStr: Array[String] = splits.head.split(":")
    val mppPartitionKey: String = dataTimeStr.head
    val dataTimeFormat: String = dataTimeStr.last

    val createTimeStr: Array[String] = splits.last.split(":")
    val createTimeKey: String = createTimeStr.head
    val createTimeFormat: String = createTimeStr.last

    val simpleDateFormat = new SimpleDateFormat(dataTimeFormat)
    val startTime: String = simpleDateFormat.format(startTimeStamp)
    val endTime: String = simpleDateFormat.format(endTimeStamp)

    val createTimeSdf = new SimpleDateFormat(createTimeFormat)
    val createTimeStartTime: String = createTimeSdf.format(startTimeStamp)
    val createTimeEndTime: String = createTimeSdf.format(endTimeStamp)

    val sql: String = param.keyMap(sqlKey).toString.replace(STARTTIME, startTime).replace(ENDTIME, endTime)
    val partitionTime: String = batchIdDateFormat.format(startTimeStamp)

    // mpp同步sql，shotTime过滤的时间
    val preTimeLong: Long = batchIdDateFormat.parse(batchId.substring(0, 10) + "0000").getTime - shotTimeRange * 24 * 60 * 60 * 1000L

    val preTime: String = simpleDateFormat.format(preTimeLong)

    val partition: Array[(String, String)] = Array((PARTITIONEDBYDAY, partitionTime.substring(2, 8)), (PARTITIONEDBYHOUR, partitionTime.substring(8, 10)))

    val master: String = param.master
  }
