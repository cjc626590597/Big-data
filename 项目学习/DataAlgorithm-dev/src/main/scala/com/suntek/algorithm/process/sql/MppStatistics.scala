package com.suntek.algorithm.process.sql

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.alibaba.fastjson.JSONObject
import com.suntek.algorithm.common.bean.{DataBaseBean, QueryBean}
import com.suntek.algorithm.common.conf.{Constant, Param}
import com.suntek.algorithm.common.util.SparkUtil
import com.suntek.algorithm.common.database.mpp.Mpp
import com.suntek.algorithm.process.util.RedisUtil
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import java.io.FileNotFoundException

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
object MppStatistics {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)


  def execSql(mpp: Mpp, queryBean: QueryBean, sqls: List[(String, Array[String])]): Unit = {
    sqls.foreach {
      //执行人，车，终端的统计sql
      case (_, sqlArray) =>
        sqlArray.foreach(f = sql =>
          try {
            mpp.excute(sql, queryBean)
          } catch {
            case e: Exception =>
              e.printStackTrace()
          })
    }
  }


  def writeToRedis(mpp: Mpp, queryBean: QueryBean, batchId: String, sParam: MppStatisticsParam): Unit = {

    val sTypeArr = sParam.statisticsType.split(";")
    (0 until sTypeArr.length - 1).foreach(i => {
      val arr = sTypeArr(i).split(":")
      logger.info(s"统计类型：${arr(0)}")
      arr(0).toLowerCase match {
        case "person" => writeToRedis(mpp, queryBean, batchId, List(4, 5, 6).mkString(","), sParam)
        case "car" => writeToRedis(mpp, queryBean, batchId, "1", sParam)
        case "code" => writeToRedis(mpp, queryBean, batchId, List(2, 3).mkString(","), sParam)
        case _ => logger.info("统计类型参数不在预期的person,car,code中")
      }
    })
  }

  def writeToRedis(mpp: Mpp, queryBean: QueryBean, batchId: String, dataType: String, sParam: MppStatisticsParam): Unit = {

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
           |select
           |       t.id,t.first_time ,t.first_device_id ,t.first_device_type ,t.first_info_id, t.last_time ,t.last_device_id,t.last_device_type,t.last_info_id,
           |       t.all_cnt ,t.cnt ,t.data_type
           |from
           |       (select id from tb_track_statistics_min where pt=$batchId and data_type in ($dataType) group by id)s
           |join
           |        (select t.id,t.first_time ,t.first_device_id ,t.first_device_type ,t.first_info_id, t.last_time ,t.last_device_id,t.last_device_type,t.last_info_id,
           |         t.all_cnt ,t.cnt ,t.data_type from tb_track_statistics t where pt=$batchId and data_type in ($dataType)) t
           |on
           |        s.id=t.id
       """.stripMargin

      //    val sqlQuery = s"select id,first_time ,first_device_id ,first_device_type ,last_time ,last_device_id " +
      //      s",last_device_type ,all_cnt ,cnt ,data_type from tb_track_statistics where pt=20210409200000   "

      mpp.query(sqlQuery, queryBean).rdd
        .foreachPartition { iter =>
          val redis = new RedisUtil(sParam.redisNode, sParam.redisPassword, sParam.redisDatabase)
          val retainTimes = sParam.RETAINDURATION * 24 * 60 * 60
          try {
            iter.foreach(r => {
              val map = r.getValuesMap[String](r.schema.fieldNames)
              val json = new JSONObject(map)
              val key = sParam.REDIS_PRE + ":" + map("data_type") + ":" + map("id")
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

  def CountByJDBC(sparkSession: SparkSession, sParam: MppStatisticsParam, batchId: String, mpp: Mpp, queryBean: QueryBean): Unit = {

    beforeCleanCache(mpp, queryBean, batchId, sParam)

    execSql(mpp, queryBean, sParam.sqls)

    afterCleanCache(mpp, queryBean, batchId, sParam)

    if (sParam.redis) {
      writeToRedis(mpp, queryBean, batchId, sParam)
    }


  }


  def clearTable(mpp: Mpp, queryBean: QueryBean): Unit = {
    mpp.excute(s"drop table  tb_track_statistics ", queryBean)
    mpp.excute(s"drop table  tb_track_statistics_min ", queryBean)
    mpp.excute(s"drop table  tb_track_statistics_hour ", queryBean)
    mpp.excute(s"drop table  tb_track_all", queryBean)
    logger.info(s"清除表！！！！")
  }


  /**
   *
   * 执行前删除当前批次的缓存
   *
   * @param mpp
   * @param queryBean
   * @param batchID
   * @param sParam
   */
  def beforeCleanCache(mpp: Mpp, queryBean: QueryBean, batchID: String, sParam: MppStatisticsParam): Unit = {

    //删除缓存的临时表分区数据
    Array("tb_track_all", "tb_track_statistics_hour", "tb_track_statistics_min")
      .map(x => s"delete  $x where pt= $batchID")
      .foreach(deleteSql => {
        logger.info(s"清除缓存：$deleteSql")
        mpp.excute(deleteSql, queryBean)
      })

  }


  def afterCleanCache(mpp: Mpp, queryBean: QueryBean, batchId: String, sParam: MppStatisticsParam): Unit = {

    //清除结果表tb_track_statistics
    val calendar = Calendar.getInstance()
    calendar.setTime(sParam.batchIdDateFormat.parse(batchId))
    calendar.add(Calendar.MINUTE, -1 * sParam.deleyTimeMin * sParam.numPartition)
    var deleteTime = sParam.batchIdDateFormat.format(calendar.getTime)

    deletePartition(mpp, queryBean, deleteTime, sParam, sParam.TB_TRACK_STATISTICS, sParam.delayTime)
    deletePartition(mpp, queryBean, deleteTime, sParam, sParam.TB_TRACK_STATISTICS_MIN, sParam.delayTime)
    logger.info(s"从$deleteTime 开始删除 ${sParam.deleyTimeMin * sParam.numPartition} 分钟前数据（tb_track_statistics，tb_track_statistics_min）")

    //删除2天的轨迹初始化表tb_track_all
    val delay = 2
    calendar.setTime(sParam.batchIdDateFormat.parse(batchId))
    calendar.add(Calendar.DAY_OF_YEAR, -1 * delay)
    deleteTime = sParam.batchIdDateFormat.format(calendar.getTime)
    deletePartition(mpp, queryBean, deleteTime, sParam, sParam.TB_TRACK_ALL, sParam.delayTime)
    logger.info(s"从$deleteTime 开始删除 $delay 天的数据（${sParam.TB_TRACK_ALL}）")

    //删除一年前的轨迹表
    calendar.setTime(sParam.batchIdDateFormat.parse(batchId))
    calendar.add(Calendar.DAY_OF_YEAR, -1 * sParam.RETAINDURATION)
    deleteTime = sParam.batchIdDateFormat.format(calendar.getTime)
    deletePartition(mpp, queryBean, deleteTime, sParam, sParam.TB_TRACK_STATISTICS_HOUR, sParam.HOUR)

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
  def deletePartition(mpp: Mpp, queryBean: QueryBean, batchId: String, sParam: MppStatisticsParam, table: String, delay: Int): Unit = {
    val partitionTimeStamp = sParam.batchIdDateFormat.parse(batchId).getTime + (delay * 1000L)
    val partition = sParam.batchIdDateFormat.format(partitionTimeStamp)
    //数据表必须有一个分区 p20200101000500
    val deletePartitiionSql =
      s"select partition_name, high_value from dba_tab_partitions where table_name='$table'  " +
        s"and high_value <= $partition and partition_name != 'p20200101000500' "
    val rows = mpp.query(deletePartitiionSql, queryBean).rdd.collect()
    if (rows != null && rows.length > 0) {
      rows.foreach(r => {
        val sql = s"ALTER TABLE $table DROP partition ${r.getString(0)}"
        try{
          mpp.excute(sql, queryBean)
        }catch {
          case e:Exception=>
            logger.error(e.getMessage)
        }
      }
      )
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
  def getMaxPtFromResult(mpp: Mpp, queryBean: QueryBean, sParam: MppStatisticsParam): Long = {
    //

    val sql =
      s"select max(high_value) from dba_tab_partitions where table_name='tb_track_statistics'  " +
        s" and partition_name != 'p20200101000500' "

    val dataBaseLastTime = mpp.query(sql, queryBean).rdd.collect().head.get(0)
    if (dataBaseLastTime != null) {
      val time = sParam.batchIdDateFormat.parse(dataBaseLastTime.toString).getTime
      for (i <- 1 to sParam.numPartition) {
        val maxptTimeStamp = time - sParam.delayTime * 1000 * i
        val maxPt = sParam.batchIdDateFormat.format(new Date(maxptTimeStamp))
        val selectSql = s"select pt from tb_track_statistics where pt=$maxPt limit 2"
        val st = mpp.query(selectSql, queryBean).rdd.collect()
        if (st.size > 0) {
          return maxPt.toLong
        }
      }
    }
    0L

  }


  def getStartEndTime(sparkSession: SparkSession, sParam: MppStatisticsParam, mpp: Mpp, queryBean: QueryBean): (String, String) = {


    //初始化tb_track_statistics表
    val sqlArray = sParam.loadSqlFile("init.sql", sParam.YYYYMMDDHHMMSS)
    logger.info(s"init.sql= ${sqlArray.head}")
    (0 until sqlArray.length - 1).foreach(i => mpp.excute(sqlArray(i), queryBean))

    val dataBaseLastTime = getMaxPtFromResult(mpp, queryBean, sParam)
    var startTIme: String = null
    //如果结果表tb_track_statistics没有数据则从原始表中获取最小时间
    if (dataBaseLastTime == 0) {
      val array = sParam.loadSqlFile("minTime.sql", sParam.YYYYMMDDHHMMSS)
      logger.info(s"minTime.sql= ${array.head}")
      val minTime = mpp.query(array.head, queryBean).rdd.collect().head.getLong(0).toString
      val calendar = Calendar.getInstance()
      calendar.setTime(sParam.batchIdDateFormat.parse(minTime))
      calendar.set(Calendar.MINUTE, calendar.get(Calendar.MINUTE) / sParam.deleyTimeMin * sParam.deleyTimeMin)
      calendar.set(Calendar.SECOND, 0)
      startTIme = sParam.batchIdDateFormat.format(calendar.getTime)
      //第一次补重分区
      val partition = s"ALTER TABLE tb_track_statistics  ADD PARTITION P$startTIme VALUES LESS THAN ($startTIme)"
      mpp.excute(partition,queryBean)
      logger.info(s"从原始表中获取数据最小时间为 :$minTime")

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

    val sParam = new MppStatisticsParam(param, param.batchId)
    val master = sParam.master
    val sparkSession = SparkUtil.getSparkSession(master, s"MPP Statistics Job", param, enableHiveSupport = false)
    logger.info(s"redisNode: ${sParam.redisNode}")
    logger.info(s"redisPassword: ${sParam.redisPassword}")
    logger.info(s"redisDatabase: ${sParam.redisDatabase}")
    val dataBaseBean = new DataBaseBean()
    dataBaseBean.setUsername(sParam.username) //suntek
    dataBaseBean.setPassword(sParam.password) //suntek@123
    dataBaseBean.setDriver(Constant.MPP_DRIVER)
    dataBaseBean.setUrl(sParam.url) //
    dataBaseBean.setNumPartitions(2)
    val mpp = new Mpp(sparkSession, dataBaseBean)
    val queryBean = new QueryBean()


    if (sParam.needUpdate) {
      val maxPt = getMaxPtFromResult(mpp, queryBean, sParam)
      val sp = new MppStatisticsParam(param, maxPt.toString)
      execSql(mpp, queryBean, sp.updateSqls)
      return
    }


    val arrayBuffer = ArrayBuffer[String]()
    val (start, end) = getStartEndTime(sparkSession, sParam, mpp, queryBean)
    val startDate = sParam.batchIdDateFormat.parse(start)
    val endDate = sParam.batchIdDateFormat.parse(end)
    val calendar = Calendar.getInstance()
    calendar.setTime(startDate)
    while (calendar.getTime.getTime <= endDate.getTime) {
      arrayBuffer.append(sParam.batchIdDateFormat.format(calendar.getTime))
      calendar.add(Calendar.MINUTE, sParam.deleyTimeMin)
    }

    arrayBuffer.foreach(batchId => {
      val sp = new MppStatisticsParam(param, batchId)
      CountByJDBC(sparkSession, sp, batchId, mpp, queryBean)
    })

    sparkSession.close()
  }


}


class MppStatisticsParam(param: Param, batchId: String) extends Serializable {

  val debug: Boolean = param.keyMap.getOrElse("debug", "false").toString.toBoolean
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val REDIS_PRE: String = "das:trackstatic"

  //=================通过启动程序的json进行配置=================
  val landDataBase: String = param.keyMap.getOrElse("landDataBase", "pd_dts").toString
  //mpp链接和密码
  val queryParam = "characterEncoding=UTF-8&autoReconnect=true&useSSL=false&zeroDateTimeBehavior=convertToNull&serverTimezone=UTC"
  var url: String = s"jdbc:postgresql://${param.mppdbIp}:${param.mppdbPort}/$landDataBase?$queryParam"
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
    val mppSqlFilePath = s"$path/conf/mppStatistics/$fileName"
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
