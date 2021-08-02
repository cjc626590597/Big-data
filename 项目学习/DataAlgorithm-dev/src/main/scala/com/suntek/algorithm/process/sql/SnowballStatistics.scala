package com.suntek.algorithm.process.sql

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}
import com.alibaba.fastjson.JSONObject
import com.suntek.algorithm.common.bean.{DataBaseBean, QueryBean, TableBean}
import com.suntek.algorithm.common.conf.{Constant, Param}
import com.suntek.algorithm.common.database.{DataBase, DataBaseFactory}
import com.suntek.algorithm.common.util.{PropertiesUtil, SparkUtil}
import com.suntek.algorithm.common.database.snowball.Snowball
import com.suntek.algorithm.process.util.RedisUtil
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import java.io.FileNotFoundException
import java.util
import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
 * Created with IntelliJ IDEA.
 * Author: liaojp
 * Date: 2021-02-01 11:12
 * Description:Mpp统计
 * 支持类型:人档，车辆，终端
 *
 *
 */
object SnowballStatistics {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)


  def execSql(snowball: Snowball, queryBean: QueryBean, sqls: List[(String, Array[String])]): Unit = {
    sqls.foreach {
      //执行人，车，终端的统计sql
      case (_, sqlArray) =>
        sqlArray.foreach(f = sql =>
          try {
            snowball.excute(sql, queryBean)
          } catch {
            case e: Exception =>
              e.printStackTrace()
          })
    }
  }


  def writeToRedis(snowball: Snowball, queryBean: QueryBean, batchId: String, sParam: SnowballStatisticsParam): Unit = {

//    val tmpTable = s"tb_track_statistics_tmp"
//    mpp.excute(s"drop table if EXISTS  $tmpTable ", queryBean)
//    val sql = s"create table $tmpTable WITH (ORIENTATION = ROW ) DISTRIBUTE BY HASH (id,data_type) as select t.id,t.first_time ,t.first_device_id ,t.first_device_type ,t.last_time ,t.last_device_id,t.last_device_type ,t.all_cnt ,t.cnt ,t.data_type " +
//      s"from (select id from tb_track_statistics_min where pt=$batchId group by id)s join  " +
//      s"(select t.id,t.first_time ,t.first_device_id ,t.first_device_type ,t.last_time ,t.last_device_id,t.last_device_type ,t.all_cnt ,t.cnt ,t.data_type " +
//      s"from tb_track_statistics t where pt=$batchId) t  on s.id=t.id"
//    mpp.excute(sql, queryBean)
//    val sqlQuery = s"select id,first_time ,first_device_id ,first_device_type ,last_time ,last_device_id " +
//      s",last_device_type ,all_cnt ,cnt ,data_type from $tmpTable"
val sqlQuery =
    s"""
       |SELECT
       |       T.ID,T.FIRST_TIME ,T.FIRST_DEVICE_ID ,T.FIRST_DEVICE_TYPE ,T.FIRST_INFO_ID, T.LAST_TIME ,T.LAST_DEVICE_ID,T.LAST_DEVICE_TYPE, T.LAST_INFO_ID,
       |       T.ALL_CNT ,T.CNT ,T.DATA_TYPE
       |FROM
       |       (SELECT ID FROM TB_TRACK_STATISTICS_MIN WHERE PT=$batchId GROUP BY ID)S
       |JOIN
       |        (SELECT T.ID,T.FIRST_TIME ,T.FIRST_DEVICE_ID ,T.FIRST_DEVICE_TYPE ,T.FIRST_INFO_ID, T.LAST_TIME ,T.LAST_DEVICE_ID,T.LAST_DEVICE_TYPE, T.LAST_INFO_ID,
       |         T.ALL_CNT ,T.CNT ,T.DATA_TYPE FROM TB_TRACK_STATISTICS T WHERE PT=$batchId) T
       |ON
       |        S.ID=T.ID
       """.stripMargin

    //    val sqlQuery = s"select id,first_time ,first_device_id ,first_device_type ,last_time ,last_device_id " +
    //      s",last_device_type ,all_cnt ,cnt ,data_type from tb_track_statistics where pt=20210409200000   "


    snowball.query(sqlQuery, queryBean).rdd
      .foreachPartition { iter =>
        val redis = new RedisUtil(sParam.redisNode, sParam.redisPassword, sParam.redisDatabase)
        val retainTimes = sParam.RETAINDURATION * 24 * 60 * 60
        try {
          iter.foreach(r => {
            val id = r.get(0).toString
            val first_time = r.get(1).toString
            val first_device_id = r.get(2).toString
            val first_device_type = r.get(3).toString
            val first_info_id = r.get(4).toString
            val last_time = r.get(5).toString
            val last_device_id = r.get(6).toString
            val last_device_type = r.get(7).toString
            val last_info_id = r.get(8).toString
            val all_cnt = r.get(9).toString
            val cnt = r.get(10).toString
            val data_type = r.get(11).toString

            val json = new JSONObject()
            json.put("id", id);
            json.put("first_time", first_time);
            json.put("first_device_id", first_device_id);
            json.put("first_device_type", first_device_type);
            json.put("first_info_id", first_info_id);
            json.put("last_time", last_time);
            json.put("last_device_id", last_device_id);
            json.put("last_device_type", last_device_type);
            json.put("last_info_id", last_info_id);
            json.put("all_cnt", all_cnt);
            json.put("cnt", cnt);
            json.put("data_type", data_type);

            val key = sParam.REDIS_PRE + ":" + data_type + ":" + id
            redis.setex(key, retainTimes, json.toJSONString)
          }
          )
          redis.sync();
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          redis.close()
        }

      }
//    mpp.excute(s"drop table if EXISTS  $tmpTable ",queryBean)
  }

  def CountByJDBC(sparkSession: SparkSession, sParam: SnowballStatisticsParam, batchId: String, snowball: Snowball, queryBean: QueryBean): Unit = {

    beforeCleanCache(snowball, queryBean, batchId, sParam)

    execSql(snowball, queryBean, sParam.sqls)

    afterCleanCache(snowball, queryBean, batchId, sParam)

    if (sParam.redis) {
      writeToRedis(snowball, queryBean, batchId, sParam)
    }


  }


  /*def clearTable(mpp: Mpp, queryBean: QueryBean): Unit = {
    mpp.excute(s"drop table  tb_track_statistics ", queryBean)
    mpp.excute(s"drop table  tb_track_statistics_min ", queryBean)
    mpp.excute(s"drop table  tb_track_statistics_hour ", queryBean)
    mpp.excute(s"drop table  tb_track_all", queryBean)
    logger.info(s"清除表！！！！")
  }*/


  /**
   *
   * 执行前删除当前批次的缓存
   *
   * @param mpp
   * @param queryBean
   * @param batchID
   * @param sParam
   */
  def beforeCleanCache(snowball: Snowball, queryBean: QueryBean, batchID: String, sParam: SnowballStatisticsParam): Unit = {

    //删除缓存的临时表分区数据
    Array("TB_TRACK_ALL", "TB_TRACK_STATISTICS_HOUR", "TB_TRACK_STATISTICS_MIN")
      .map(x => s"delete from $x where PT= $batchID")
      .foreach(deleteSql => {
        logger.info(s"清除缓存：$deleteSql")
        snowball.excute(deleteSql.toUpperCase(), queryBean)
      })

  }


  def afterCleanCache(snowball: Snowball, queryBean: QueryBean, batchId: String, sParam: SnowballStatisticsParam): Unit = {

    //清除结果表tb_track_statistics
    val calendar = Calendar.getInstance()
    calendar.setTime(sParam.batchIdDateFormat.parse(batchId))
    calendar.add(Calendar.MINUTE, -1 * sParam.deleyTimeMin * sParam.numPartition)
    var deleteTime = sParam.batchIdDateFormat.format(calendar.getTime)

    deletePartition(snowball, queryBean, deleteTime, sParam, sParam.TB_TRACK_STATISTICS, sParam.delayTime)
    deletePartition(snowball, queryBean, deleteTime, sParam, sParam.TB_TRACK_STATISTICS_MIN, sParam.delayTime)
    logger.info(s"从$deleteTime 开始删除 ${sParam.deleyTimeMin * sParam.numPartition} 分钟前数据（tb_track_statistics，tb_track_statistics_min）")

    //删除2天的轨迹初始化表tb_track_all
    val delay = 2
    calendar.setTime(sParam.batchIdDateFormat.parse(batchId))
    calendar.add(Calendar.DAY_OF_YEAR, -1 * delay)
    deleteTime = sParam.batchIdDateFormat.format(calendar.getTime)
    deletePartition(snowball, queryBean, deleteTime, sParam, sParam.TB_TRACK_ALL, sParam.delayTime)
    logger.info(s"从$deleteTime 开始删除 $delay 天的数据（${sParam.TB_TRACK_ALL}）")

    //删除一年前的轨迹表
    calendar.setTime(sParam.batchIdDateFormat.parse(batchId))
    calendar.add(Calendar.DAY_OF_YEAR, -1 * sParam.RETAINDURATION)
    deleteTime = sParam.batchIdDateFormat.format(calendar.getTime)
    deletePartition(snowball, queryBean, deleteTime, sParam, sParam.TB_TRACK_STATISTICS_HOUR, sParam.HOUR)

    logger.info(s"从$deleteTime 开始删除 ${sParam.RETAINDURATION} 天前的数据（${sParam.TB_TRACK_STATISTICS_HOUR}）")

  }


  /**
   * 从  batchID删除前面的分区
   *
   * @param mpp
   * @param queryBean
   * @param batchId
   * @param sParam
   * @param table
   * @param delay
   */
  def deletePartition(snowball: Snowball, queryBean: QueryBean, batchId: String, sParam: SnowballStatisticsParam, table: String, delay: Int): Unit = {
    val partitionTimeStamp = sParam.batchIdDateFormat.parse(batchId).getTime + (delay * 1000L)
    val partition = sParam.batchIdDateFormat.format(partitionTimeStamp)
    //数据表必须有一个分区 p20200101000500
    val sql =
      s"""
        |delete from $table
        |where PT < $partition
        |""".stripMargin
    try{
      snowball.excute(sql.toUpperCase(), queryBean)
    }catch {
      case e:Exception=>
        logger.error(e.getMessage)
    }

  }

  /**
   *
   * 得到最大的分区。
   *
   * @param mpp
   * @param queryBean
   * @return
   */
  def getMaxPtFromResult(snowball: Snowball, queryBean: QueryBean, sParam: SnowballStatisticsParam): Long = {
    //

    val sql =
      """
        |select max(PT) from TB_TRACK_STATISTICS
        |""".stripMargin

    val dataBaseLastTime = snowball.query(sql, queryBean).rdd.collect().head.get(0)
    if (dataBaseLastTime != null && dataBaseLastTime.toString != "0") {
      val time = sParam.batchIdDateFormat.parse(dataBaseLastTime.toString).getTime
      for (i <- 1 to sParam.numPartition) {
        val maxptTimeStamp = time - sParam.delayTime * 1000 * i
        val maxPt = sParam.batchIdDateFormat.format(new Date(maxptTimeStamp))
        val selectSql = s"select PT from TB_TRACK_STATISTICS where PT=$maxPt limit 2"
        val st = snowball.query(selectSql, queryBean).rdd.collect()
        if (st.size > 0) {
          return maxPt.toLong
        }
      }
    }
    0L

  }


  def getStartEndTime(sparkSession: SparkSession, sParam: SnowballStatisticsParam, snowball: Snowball, queryBean: QueryBean): (String, String) = {


    //初始化tb_track_statistics表
    val sqlArray = sParam.loadSqlFile("init.sql", sParam.YYYYMMDDHHMMSS)
    logger.info(s"init.sql= ${sqlArray.head}")
    sqlArray.indices.foreach(i => snowball.excute(sqlArray(i), queryBean))

    val dataBaseLastTime = getMaxPtFromResult(snowball, queryBean, sParam)
    var startTIme: String = null
    //如果结果表tb_track_statistics没有数据则从原始表中获取最小时间
    // 并且是 mpp数据库
    if (dataBaseLastTime == 0) {
      val array = sParam.loadSqlFile("minTime.sql", sParam.YYYYMMDDHHMMSS)
      logger.info(s"minTime.sql= ${array.head}")
      val minTime = snowball.query(array.head, queryBean).rdd.collect().head.getDecimal(0).toString
      val calendar = Calendar.getInstance()
      calendar.setTime(sParam.batchIdDateFormat.parse(minTime))
      calendar.set(Calendar.MINUTE, calendar.get(Calendar.MINUTE) / sParam.deleyTimeMin * sParam.deleyTimeMin)
      calendar.set(Calendar.SECOND, 0)
      startTIme = sParam.batchIdDateFormat.format(calendar.getTime)

    } else {
      //如果结果表tb_track_statistics有数据
      val date = sParam.batchIdDateFormat.parse(dataBaseLastTime.toString)
      val timeStamp = date.getTime + sParam.delayTime * 1000
      date.setTime(timeStamp)
      startTIme = sParam.batchIdDateFormat.format(date)
    }
    val calendar = Calendar.getInstance()
    calendar.set(Calendar.MINUTE, calendar.get(Calendar.MINUTE) / sParam.deleyTimeMin * sParam.deleyTimeMin)
    calendar.set(Calendar.SECOND, 0)
    val endTime = sParam.batchIdDateFormat.format(calendar.getTime)
    logger.info(s"===========开始时间为：$startTIme，结束时间：${endTime}")
    (startTIme.toString, endTime)
  }


  /**
   * mpp同步数据导hive
   * 支持方式 jdbc
   *
   * @param param
   */
  def process(param: Param): Unit = {

    val sParam = new SnowballStatisticsParam(param, param.batchId)
    val master = sParam.master
    val sparkSession = SparkUtil.getSparkSession(master, s"MPP Statistics Job", param, enableHiveSupport = false)
    logger.info(s"redisNode: ${sParam.redisNode}")
    logger.info(s"redisPassword: ${sParam.redisPassword}")
    logger.info(s"redisDatabase: ${sParam.redisDatabase}")
    val dataBaseBean = new DataBaseBean()
    dataBaseBean.setUrl(sParam.url) //
    dataBaseBean.setNumPartitions(2)
    dataBaseBean.setDriver(Constant.SNOWBALL_DRIVER)
    dataBaseBean.setUsername(sParam.username) //suntek
    dataBaseBean.setPassword(sParam.password) //suntek@123
    val snowball = new Snowball(sparkSession, dataBaseBean)
    val queryBean = new QueryBean()
    logger.info(param.mppdbType)
    logger.info(sParam.url)
    logger.info(param.mppdbUsername)
    logger.info(param.mppdbPassword)

    if (sParam.needUpdate) {
      val maxPt = getMaxPtFromResult(snowball, queryBean, sParam)
      val sp = new SnowballStatisticsParam(param, maxPt.toString)
      execSql(snowball, queryBean, sp.updateSqls)
      return
    }


    val arrayBuffer = ArrayBuffer[String]()
    val (start, end) = getStartEndTime(sparkSession, sParam, snowball, queryBean)
    val startDate = sParam.batchIdDateFormat.parse(start)
    val endDate = sParam.batchIdDateFormat.parse(end)
    val calendar = Calendar.getInstance()
    calendar.setTime(startDate)
    while (calendar.getTime.getTime <= endDate.getTime) {
      arrayBuffer.append(sParam.batchIdDateFormat.format(calendar.getTime))
      calendar.add(Calendar.MINUTE, sParam.deleyTimeMin)
    }

    arrayBuffer.foreach(batchId => {
      val sp = new SnowballStatisticsParam(param, batchId)
      CountByJDBC(sparkSession, sp, batchId, snowball, queryBean)
    })

    sparkSession.close()
  }


}


class SnowballStatisticsParam(param: Param, batchId: String) extends Serializable {

  val debug: Boolean = param.keyMap.getOrElse("debug", "false").toString.toBoolean
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val REDIS_PRE: String = "das:trackstatic"

  //=================通过启动程序的json进行配置=================
  val landDataBase: String = param.keyMap.getOrElse("landDataBase", "pd_dts").toString
  //mpp链接和密码
  val queryParam = "socket_timeout=3000000"
  var url: String = s"jdbc:${Constant.SNOWBALL_SUB_PROTOCOL}://${param.mppdbIp}:${param.mppdbPort}/$landDataBase?$queryParam"
  var username: String = param.mppdbUsername
  var password: String = param.mppdbPassword

  val TB_TRACK_STATISTICS = "tb_track_statistics"
  val TB_TRACK_STATISTICS_MIN = "tb_track_statistics_min"
  val TB_TRACK_ALL: String = "tb_track_all"
  val TB_TRACK_STATISTICS_HOUR = "tb_track_statistics_hour"

  val needUpdate: Boolean = param.keyMap.getOrElse("needUpdate", "false").toString.toBoolean
  val RETAINDURATION: Int = param.keyMap.getOrElse("retainDuration", "90").toString.toInt

  if (debug) {
    url = "jdbc:postgresql://172.25.21.18:25308/beehive"
    username = "suntek"
    password = "suntek@123"
  }
  val redis = param.keyMap.getOrElse("redis", "true").toString.toBoolean
  //  val redisHost = param.keyMap.getOrElse("redisHost", "172.25.21.104").toString
  //  val redisPort = param.keyMap.getOrElse("redisPort", "6379").toString.toInt

  val redisNode = param.redisNode
  val redisPassword = param.redisPassword
  val redisDatabase = param.redisDataBase

  //  val batchId: String = param.keyMap("batchId").toString
  val delayTime: Int = param.keyMap.getOrElse("dalayTime", 300).toString.toInt
  val numPartition: Int = param.keyMap.getOrElse("numPartition", 2).toString.toInt
  //=================通过启动程序的json进行配置=================


  var deleyTimeMin: Int = delayTime / 60

  //开始结束时间戳
  val HOUR: Int = 24 * 3600


  val batchIdDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
  val batchIdTimeStamp: Long = batchIdDateFormat.parse(batchId).getTime
  var startTimeStamp: Long = batchIdTimeStamp - delayTime * 1000L
  var endTimeStamp: Long = batchIdTimeStamp
  var partitionStamp: Long = batchIdTimeStamp + delayTime * 1000L
  val partitionHourStamp: Long = batchIdTimeStamp + HOUR * 1000L
  val startimeHourStamp: Long = batchIdTimeStamp - HOUR * 1000L

  //sql where条件的开始结束时间
  val STARTTIME = "@START_TIME@"
  val ENDTIME = "@END_TIME@"
  val PATITIONTIME = "@PARTITION_TIME@"
  val PATITIONTIMEHOUR = "@PARTITION_TIME_HOUR@"
  val STARTTIMEHOUR = "@START_TIME_HOUR@"
  val BATCHTIME = "@BATCH_TIME@"
  val statisticsType: String = param.keyMap("statisticsType").toString

  // 加载sql文件文件
  def loadSqlFile(fileName: String, formatStr: String): Array[String] = {
    var path = System.getenv(param.deployEnvName)
    if (debug) {
      path = this.getClass.getResource("/").getPath
    }
    val mppSqlFilePath = s"$path/conf/snowballStatistics/$fileName"
    println("=========" + mppSqlFilePath)
    logger.info("=========" + mppSqlFilePath)
    var file = new java.io.File(mppSqlFilePath)
    if (!file.exists()) {
      throw new FileNotFoundException("找不到sql文件路径")
    }
    val simpleDateFormat = new SimpleDateFormat(formatStr)
    val source = Source.fromFile(file)
    val execSqls: Array[String] = source.getLines().mkString(" ")
      .replace(STARTTIME, simpleDateFormat.format(startTimeStamp))
      .replace(ENDTIME, simpleDateFormat.format(endTimeStamp))
      .replace(PATITIONTIME, batchIdDateFormat.format(partitionStamp))
      .replace(PATITIONTIMEHOUR, batchIdDateFormat.format(partitionHourStamp))
      .replace(STARTTIMEHOUR, batchIdDateFormat.format(startimeHourStamp))
      .replace(BATCHTIME, batchId)
      .split(";").map(_.trim)
      .filter(x => x.nonEmpty && !x.startsWith("--"))
    source.close()
    execSqls
  }


  val HOURFILENAME = "statistics_hour.sql"
  val YYYYMMDDHHMMSS = "yyyyMMddHHmmss"
  val INITFILENAME = "init.sql"

  val UPDATEFILENAME = "update.sql"

  var sqls: List[(String, Array[String])] = statisticsType.split(";").map {
    str =>
      val strs = str.split(":")
      strs.head -> loadSqlFile(s"${strs.head}.sql", strs.last)
  }.toList

  // var sqls: List[(String, Array[String])] = List()
  //判断是否整时，执行按天汇总
  if (batchId.endsWith("000000"))
    sqls :+= ("HOUR" -> loadSqlFile(HOURFILENAME, YYYYMMDDHHMMSS))

  sqls +:= ("INIT" -> loadSqlFile(INITFILENAME, YYYYMMDDHHMMSS).takeRight(1))
  val master: String = param.master


  lazy val updateSqls: List[(String, Array[String])] = List(("update" -> loadSqlFile(UPDATEFILENAME, YYYYMMDDHHMMSS)))

}
