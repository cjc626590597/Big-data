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
object MppStatisticsNew {

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


  def writeToRedis(mpp: Mpp, queryBean: QueryBean, batchId: String, dataType: String, sParam: MppStatisticsParamNew): Unit = {

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

    mpp.query(sqlQuery, queryBean)
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
  }

  def writeToRedis(mpp: Mpp, queryBean: QueryBean, batchId: String, sParam: MppStatisticsParamNew): Unit = {

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

  def CountByJDBC(sparkSession: SparkSession, sParam: MppStatisticsParamNew, batchId: String, mpp: Mpp, queryBean: QueryBean): Unit = {

    beforeCleanCache(mpp, queryBean, batchId, sParam)

    execSql(mpp, queryBean, sParam.sqls)

//    if(sParam.isNewDay) {
      afterCleanCache(mpp, queryBean, batchId, sParam)
//    }

    if (sParam.redis) {
      writeToRedis(mpp, queryBean, batchId, sParam)
    }
  }


  def clearTable(mpp: Mpp, queryBean: QueryBean): Unit = {
    mpp.excute(s"drop table  tb_track_statistics ", queryBean)
    mpp.excute(s"drop table  tb_track_statistics_min ", queryBean)
//    mpp.excute(s"drop table  tb_track_statistics_hour ", queryBean)
    logger.info(s"清除表！！！！")
  }


  def clearTrackStatisticsTable(mpp: Mpp, queryBean: QueryBean): Unit = {
    mpp.excute(s"delete tb_track_statistics ", queryBean)
    mpp.excute(s"delete tb_track_statistics_min ", queryBean)
//    mpp.excute(s"delete from tb_track_statistics_hour ", queryBean)
    logger.info(s"清空统计信息！！！！")
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
  def beforeCleanCache(mpp: Mpp, queryBean: QueryBean, batchID: String, sParam: MppStatisticsParamNew): Unit = {

    //删除缓存的临时表分区数据
    sParam.cacheTableArr.foreach(table => {
      val sql = s"delete $table where pt= $batchID"
      logger.info(s"清除缓存：$sql")
      mpp.excute(sql, queryBean)
    })
  }


  def afterCleanCache(mpp: Mpp, queryBean: QueryBean, batchId: String, sParam: MppStatisticsParamNew): Unit = {

    //清除结果表tb_track_statistics
    //删除一天前的 tb_track_statistics 轨迹表 的分区数据
    val calendar = Calendar.getInstance()
    calendar.setTime(sParam.batchIdDateFormat.parse(batchId))
    calendar.add(Calendar.SECOND, -1 * sParam.delayTimeGap)
    var partition = sParam.batchIdDateFormat.format(calendar.getTime)

    logger.info(s"开始删除历史分区数据...")
    logger.info(s"删除${sParam.TB_TRACK_STATISTICS}的${partition}分区数据")
    delPartition(mpp, sParam, queryBean, sParam.TB_TRACK_STATISTICS, partition)

    calendar.setTime(sParam.batchIdDateFormat.parse(batchId))
    calendar.add(Calendar.SECOND, -1 * sParam.HOUR)
    partition = sParam.batchIdDateFormat.format(calendar.getTime)
    logger.info(s"删除${sParam.TB_TRACK_STATISTICS_MIN}的${partition}分区数据")
    delPartition(mpp, sParam, queryBean, sParam.TB_TRACK_STATISTICS_MIN, partition)

    //删除90天的 tb_track_statistics_hour 轨迹表 的分区数据
    calendar.setTime(sParam.batchIdDateFormat.parse(batchId))
    calendar.add(Calendar.DAY_OF_YEAR, -1 * sParam.RETAINDURATION)
    partition = sParam.batchIdDateFormat.format(calendar.getTime)
    logger.info(s"删除${sParam.TB_TRACK_STATISTICS_HOUR}的${partition}分区数据")
    delPartition(mpp, sParam, queryBean, sParam.TB_TRACK_STATISTICS_HOUR, partition)
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
  def deletePartition(mpp: Mpp, queryBean: QueryBean, batchId: String, sParam: MppStatisticsParamNew, table: String, delay: Int): Unit = {
    val partitionTimeStamp = sParam.batchIdDateFormat.parse(batchId).getTime + (delay * 1000L)
    val partition = sParam.batchIdDateFormat.format(partitionTimeStamp)
    //数据表必须有一个分区 p20200101000500
    val deletePartitiionSql =
      s"select partition_name, high_value from dba_tab_partitions where table_name='$table'  " +
        s"and high_value <= $partition and partition_name != 'p20200101000500' "
    val rows = mpp.query(deletePartitiionSql, queryBean).collect()
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

  def getMinTimeFromOds(mpp: Mpp, queryBean: QueryBean, sParam: MppStatisticsParamNew): Long = {
    val array = sParam.loadSqlFile("minTime.sql", sParam.YYYYMMDDHHMMSS)
    logger.info(s"minTime.sql= ${array.head}")
    val minTime = mpp.query(array.head, queryBean).head.toString
    if (minTime != null) {
      return minTime.toLong
    }
    0L
  }

  /**
   * 得到最大的分区。
   * @param mpp
   * @param queryBean
   * @return
   */
  def getMaxPtFromResult(mpp: Mpp, queryBean: QueryBean, sParam: MppStatisticsParamNew): Long = {
    //
    val sql =
      s"""
         |select
         |  max(high_value)
         |from dba_tab_partitions
         |where table_name='tb_track_statistics' and partition_name != 'p20200101000500'
         |""".stripMargin
    val dataBaseLastTime = mpp.query(sql, queryBean).rdd.first().get(0)
    if (dataBaseLastTime != null) {
      return dataBaseLastTime.toString.toLong
    }
    0L
  }


  def getStartEndTime(sparkSession: SparkSession, sParam: MppStatisticsParamNew, mpp: Mpp, queryBean: QueryBean, isReRun: Boolean): (String, String) = {

    var startTIme: String = null
    if(isReRun) {
      val sql = s"select  min(CREATE_TIME) t from HIVE_FACE_DETECT_RL where length (toString(CREATE_TIME))>=14"
      logger.info(s"minTime.sql= $sql")
      val minTime = mpp.query(sql, queryBean).collect().head.toString
      var calendar = Calendar.getInstance()
      // 开始时间
      calendar.setTime(sParam.batchIdDateFormat.parse(minTime))
      calendar.set(Calendar.MINUTE, calendar.get(Calendar.MINUTE) / sParam.deleyTimeMin * sParam.deleyTimeMin)
      calendar.set(Calendar.SECOND, 0)
      startTIme = sParam.batchIdDateFormat.format(calendar.getTime)
      // 结束时间
      calendar.set(Calendar.MINUTE, calendar.get(Calendar.MINUTE) / sParam.deleyTimeMin * sParam.deleyTimeMin)
      calendar.set(Calendar.SECOND, 0)
      val endTime = sParam.batchIdDateFormat.format(calendar.getTime)
      logger.info(s"===========开始时间为：$startTIme，结束时间：${endTime}")
      (startTIme, endTime)
    }
    else {
      val dataBaseLastTime = getMaxPtFromResult(mpp, queryBean, sParam)

      //如果结果表tb_track_statistics没有数据则从原始表中获取最小时间
      if (dataBaseLastTime == 0L) {
        val array = sParam.loadSqlFile("minTime.sql", sParam.YYYYMMDDHHMMSS)
        logger.info(s"minTime.sql= ${array.head}")
        val minTime = mpp.query(array.head, queryBean).collect().head.getLong(0).toString
        val calendar = Calendar.getInstance()
        calendar.setTime(sParam.batchIdDateFormat.parse(minTime))
        calendar.set(Calendar.HOUR_OF_DAY, 0)
        calendar.set(Calendar.MINUTE, 0)
        calendar.set(Calendar.SECOND, 0)
        startTIme = sParam.batchIdDateFormat.format(calendar.getTime)
        calendar.add(Calendar.SECOND, sParam.HOUR)
      } else {
        //如果结果表tb_track_statistics有数据
        /*val date = sParam.batchIdDateFormat.parse(dataBaseLastTime.toString)
        val timeStamp = date.getTime + sParam.delayTime * 1000
        date.setTime(timeStamp)
        startTIme = sParam.batchIdDateFormat.format(date)*/
        startTIme = dataBaseLastTime.toString
      }
      val calendar = Calendar.getInstance()
      calendar.set(Calendar.MINUTE, calendar.get(Calendar.MINUTE) / sParam.deleyTimeMin * sParam.deleyTimeMin)
      calendar.set(Calendar.SECOND, 0)
      val endTime = sParam.batchIdDateFormat.format(calendar.getTime)
      logger.info(s"===========开始时间为：$startTIme，结束时间：${endTime}")
      (startTIme, endTime)
    }
  }

  def addPartition(mpp: Mpp, sParam: MppStatisticsParamNew, queryBean: QueryBean) = {

    sParam.cacheTableArr.foreach(table => {
      var partition = sParam.batchIdDateFormat.format(sParam.partitionStamp)
      var sql = s"select count(1) from dba_tab_partitions where table_name='${table}' and partition_name = 'p$partition'"
      var count = mpp.query(sql, queryBean).head().getLong(0)
      if(count > 0L) {
        val delDataSql = s"delete '${table}' where pt = 'p$partition'"
        mpp.query(delDataSql, queryBean)
      }
      else {
        // 先删除比本批次大的分区
        val delPartitionSql = s"select high_value from dba_tab_partitions where table_name='${table}' and high_value > $partition"
        mpp.query(delPartitionSql, queryBean).collect().foreach(row => {
          val tp = row.getString(0)
          delPartition(mpp, sParam, queryBean, table, tp)
        })

        if (table == sParam.TB_TRACK_STATISTICS_HOUR) {
          partition = sParam.batchIdDateFormat.format(sParam.partitionHourStamp)
        }
        sql = s"ALTER TABLE ${table}  ADD PARTITION P${partition} VALUES LESS THAN (${partition})"
        logger.info(s"新增分区：$sql")
        mpp.excute(sql, queryBean)
      }
    })
  }

  def delPartition(mpp: Mpp, sParam: MppStatisticsParamNew, queryBean: QueryBean, table: String, partition: String) = {
    var sql = s"select count(1) from dba_tab_partitions where table_name='$table' and partition_name = 'p${partition}'"
    val count = mpp.query(sql, queryBean).head().getLong(0)
    if(count > 0L) {
      sql = s"ALTER TABLE $table DROP PARTITION P${partition} "
      logger.info(s"删除历史数据分区：$sql")
      mpp.excute(sql, queryBean)
    }
  }

  def getBatchIdArr(mpp: Mpp, sparkSession: SparkSession, sParam: MppStatisticsParamNew, queryBean: QueryBean, isReRun: Boolean) = {
    val arrayBuffer = ArrayBuffer[(String, Int)]()
    val (start, end) = getStartEndTime(sparkSession, sParam, mpp, queryBean, isReRun)
    val startDate = sParam.batchIdDateFormat.parse(start)
    val endDate = sParam.batchIdDateFormat.parse(end)

    var lastTime = startDate.getTime

    val calendar = Calendar.getInstance()
    calendar.setTime(startDate)

    var isZeroFlag = false
    if(start.endsWith("000000")) {
      isZeroFlag = true
    }

    while (calendar.getTime.getTime <= endDate.getTime) {
      if(isZeroFlag) {
        // 判断是否大于1天
        if (endDate.getTime - lastTime > 1 * sParam.HOUR * 1000L) {
          arrayBuffer.append((sParam.batchIdDateFormat.format(calendar.getTime), 1))
          calendar.add(Calendar.SECOND, sParam.HOUR)
          isZeroFlag = true
        }
        else {
          if(arrayBuffer.nonEmpty) {
            val lastTuple = arrayBuffer(arrayBuffer.size - 1)
            val date = sParam.batchIdDateFormat.parse(lastTuple._1)
            if (calendar.getTime.getTime() - date.getTime() >= (sParam.HOUR * 1000L)) {
              arrayBuffer.append((sParam.batchIdDateFormat.format(calendar.getTime), 2))
            }
            else {
              arrayBuffer.append((sParam.batchIdDateFormat.format(calendar.getTime), 0))
            }
          }
          else {
            arrayBuffer.append((sParam.batchIdDateFormat.format(calendar.getTime), 0))
          }
          calendar.add(Calendar.SECOND, sParam.delayTime)
          isZeroFlag = false
        }
      }
      else {
        if(calendar.get(Calendar.HOUR_OF_DAY) == 0 &&
          calendar.get(Calendar.MINUTE) == 0 &&
          calendar.get(Calendar.MINUTE) == 0) {
          if (endDate.getTime - lastTime > 1 * sParam.HOUR * 1000L) {
            arrayBuffer.append((sParam.batchIdDateFormat.format(calendar.getTime), 1))
            calendar.add(Calendar.SECOND, sParam.HOUR)
          }
          else {
            arrayBuffer.append((sParam.batchIdDateFormat.format(calendar.getTime), 0))
            calendar.add(Calendar.SECOND, sParam.delayTime)
          }
          isZeroFlag = true
        }
        else {
          arrayBuffer.append((sParam.batchIdDateFormat.format(calendar.getTime), 0))
          calendar.add(Calendar.SECOND, sParam.delayTime)
          isZeroFlag = false
        }
      }
      lastTime = calendar.getTime.getTime
    }
    arrayBuffer
  }

  def initTable(mpp: Mpp, sParam: MppStatisticsParamNew, queryBean: QueryBean) = {
    //初始化tb_track_statistics表
    val sqlArray = sParam.loadSqlFile("init.sql", sParam.YYYYMMDDHHMMSS)
    sqlArray.foreach(sql => {
      logger.info(s"init.sql = $sql")
      mpp.excute(sql, queryBean)
    })
  }

  /**
   * mpp同步数据导hive
   * 支持方式 jdbc
   *
   * @param param
   */
  def process(param: Param): Unit = {

    val sparkSession = SparkUtil.getSparkSession(param.master, s"MPP Statistics Job", param, enableHiveSupport = false)
    val sParam = new MppStatisticsParamNew(param, param.batchId, 0)
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

    // 初始化表
    initTable(mpp, sParam, queryBean)

    // 人脸整合表存在更新，只是重跑人员数据，车，码数据保留
    val isReRun = false

    if (isReRun) {
      // TODO 删除人员统计数据
    }
    else {
      // 如果选择了批次时间，将清除历史表，重跑所有数据
//      val maxPt = getMaxPtFromResult(mpp, queryBean, sParam)
//      val curBatchId = param.batchId.toLong
//
//      logger.info(s"maxPt: $maxPt")
//      logger.info(s"curBatchId: $curBatchId")
//
//      if (maxPt == 0L || curBatchId < maxPt) {
        // 清理批次数据
//        clearTable(mpp, queryBean)
//        isQuickWalk = true
//        clearTrackStatisticsTable(mpp, queryBean)
//      }
    }
//    logger.info(s"==isQuickWalk : $isQuickWalk")

    val arrayBuffer = getBatchIdArr(mpp, sparkSession, sParam, queryBean, isReRun)
    println(s"批次列表：$arrayBuffer")

    arrayBuffer.foreach{
      case (batchId, typeFlag) =>
        val sp = new MppStatisticsParamNew(param, batchId, typeFlag)

        if(isReRun && batchId != arrayBuffer.last._1) {
          var newSqls = List[(String, Array[String])]()
          sp.sqls.foreach {
            // 只重跑人
            case (key, sqlArray) =>
              if(key != "car" && key != "code") {
                newSqls :+= (key, sqlArray)
              }
          }
          sp.sqls = newSqls
        }

        // 给表添加分区
        addPartition(mpp, sp, queryBean)
        CountByJDBC(sparkSession, sp, batchId, mpp, queryBean)
    }

    sparkSession.close()
  }

}


/**
 * 0：默认为配置的批次间隔
 * 1：按天为批次间隔
 * 2：按天为批次间隔，并且为最后一天
 */
class MppStatisticsParamNew(param: Param, batchId: String, typeFlag: Int) extends Serializable {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val REDIS_PRE: String = "das:trackstatic"

  //=================通过启动程序的json进行配置=================
  val debug: Boolean = param.keyMap.getOrElse("debug", "false").toString.toBoolean
  val landDataBase: String = param.keyMap.getOrElse("landDataBase", "pd_dts").toString
  val needUpdate: Boolean = param.keyMap.getOrElse("needUpdate", "false").toString.toBoolean
  val RETAINDURATION: Int = param.keyMap.getOrElse("retainDuration", "90").toString.toInt
  val redis: Boolean = param.keyMap.getOrElse("redis", "true").toString.toBoolean

  //mpp链接和密码
  val queryParam = "characterEncoding=UTF-8&autoReconnect=true&useSSL=false&zeroDateTimeBehavior=convertToNull&serverTimezone=UTC"
  var url: String = s"jdbc:postgresql://${param.mppdbIp}:${param.mppdbPort}/$landDataBase?$queryParam"
  var username: String = param.mppdbUsername
  var password: String = param.mppdbPassword

  val TB_TRACK_STATISTICS = "tb_track_statistics"
  val TB_TRACK_STATISTICS_MIN = "tb_track_statistics_min"
  val TB_TRACK_STATISTICS_HOUR = "tb_track_statistics_hour"

  if (debug) {
    url = "jdbc:postgresql://172.25.21.18:25308/beehive"
    username = "suntek"
    password = "suntek@123"
  }

  val redisNode: String = param.redisNode
  val redisPassword: String = param.redisPassword
  val redisDatabase: Int = param.redisDataBase

  //  val batchId: String = param.keyMap("batchId").toString
  val delayTime: Int = param.keyMap.getOrElse("dalayTime", 300).toString.toInt
  val numPartition: Int = param.keyMap.getOrElse("numPartition", 2).toString.toInt
  //=================通过启动程序的json进行配置=================

  //开始结束时间戳
  val HOUR: Int = 24 * 3600

  var deleyTimeMin: Int = delayTime / 60

  val delayTimeGap: Int = getDelayTimeGap()

  val batchIdDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
  val batchIdTimeStamp: Long = batchIdDateFormat.parse(batchId).getTime
  var startTimeStamp: Long = if (typeFlag == 2) {batchIdTimeStamp - HOUR * 1000L} else {batchIdTimeStamp - delayTimeGap * 1000L}
  var endTimeStamp: Long = batchIdTimeStamp
  var partitionStamp: Long = batchIdTimeStamp + delayTimeGap * 1000L
  val partitionHourStamp: Long = batchIdTimeStamp + HOUR * 1000L
  val startimeHourStamp: Long = batchIdTimeStamp - HOUR * 1000L

  logger.info(s"delayTimeGap: $delayTimeGap")
  logger.info(s"startTimeStamp: $startTimeStamp")
  logger.info(s"endTimeStamp: $endTimeStamp")

  //sql where条件的开始结束时间
  val STARTTIME = "@START_TIME@"
  val ENDTIME = "@END_TIME@"
  val PATITIONTIME = "@PARTITION_TIME@"
  val PATITIONTIMEHOUR = "@PARTITION_TIME_HOUR@"
  val STARTTIMEHOUR = "@START_TIME_HOUR@"
  val BATCHTIME = "@BATCH_TIME@"
  val statisticsType: String = param.keyMap("statisticsType").toString
  val cacheTableArr: ArrayBuffer[String] = ArrayBuffer("tb_track_statistics", "tb_track_statistics_min")

  // 加载sql文件文件
  def loadSqlFile(fileName: String, formatStr: String): Array[String] = {
    var path = System.getenv(param.deployEnvName)
    if (debug) {
      path = this.getClass.getResource("/").getPath
    }
    val mppSqlFilePath = s"$path/conf/mppStatisticsNew/$fileName"
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

  def getDelayTimeGap() = {
    if (batchId.endsWith("000000")) {
      if(typeFlag == 1) {
        HOUR
      }
      else {
        delayTime
      }
    }
    else {
      delayTime
    }
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
  if (batchId.endsWith("000000")) {
    sqls :+= ("HOUR" -> loadSqlFile(HOURFILENAME, YYYYMMDDHHMMSS))
    cacheTableArr.append("tb_track_statistics_hour")
  }

//  sqls +:= ("INIT" -> loadSqlFile(INITFILENAME, YYYYMMDDHHMMSS).takeRight(1))
  val master: String = param.master


//  lazy val updateSqls: List[(String, Array[String])] = List(("update" -> loadSqlFile(UPDATEFILENAME, YYYYMMDDHHMMSS)))

}
