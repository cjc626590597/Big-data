package com.suntek.algorithm.fusion

import com.suntek.algorithm.common.bean.{QueryBean, SaveParamBean}
import com.suntek.algorithm.common.conf.{Constant, Param}
import com.suntek.algorithm.common.database.DataBaseFactory
import com.suntek.algorithm.common.util.{OtherDBTaleLoadUtil, SparkUtil}
import com.suntek.algorithm.evaluation.DataManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}
import java.text.SimpleDateFormat
import java.util.{Date, Properties, TimeZone}

import scala.collection.mutable

/**
 * 人员ID与人体ID关联分析
 *
 * @author chenb
 * */
//noinspection SpellCheckingInspection

object PersonIdBodyHandler {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val CATEGORY1 = "category1"
  val CATEGORY2 = "category2"
  val FACE_PERSON_BODY_LOAD_DAY = "face.person.body.load.day"
  val FACE_DETECT_LOAD_DAY = "face.detect.load.day"
  val relationMPPDBTableName = "FUSION_RELATION_INFO"
  val relationMap: mutable.Map[String, Object] = DataManager.loadRelation()

  @throws[Exception]
  def process(param: Param): Unit = {

    var category1 = param.releTypes(0)
    var category2 = param.releTypes(1)
    if (Constant.PRIORITY_LEVEL(category1) > Constant.PRIORITY_LEVEL(category2)) {
      category1 = param.releTypes(1)
      category2 = param.releTypes(0)
    }

    param.keyMap.put(CATEGORY1, category1)
    param.keyMap.put(CATEGORY2, category2)

    val batchId = param.keyMap("batchId").toString.substring(0, 8)
    val spark: SparkSession = SparkUtil.getSparkSession(param.master, s"PersonIdBodyHandler_$batchId", param)

    val numPartitions = param.keyMap.getOrElse(param.NUMPARTITIONS, "10").toString.toInt

    val sdf = new SimpleDateFormat("yyyyMMdd")
    sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    val runTimeStamp = sdf.parse(batchId).getTime - 60 * 60 * 1000L
    val startDate = sdf.format(runTimeStamp)
    val statDate = startDate.substring(2)

    val sql1 = param.keyMap(FACE_PERSON_BODY_LOAD_DAY).toString
      .replaceAll("@startDate@", startDate)
      .replaceAll("@type@", "personbody-face")

    val facePersonBodyData = loadFacePersonBodyData(spark, param, sql1) //personId, (idType, idNumber, personName)

    val sql2 = param.keyMap(FACE_DETECT_LOAD_DAY).toString
      .replaceAll("@startDate@", statDate)

    val facedetectData = loadFaceDetectData(spark, param, sql2) // person_id,(face_id,device_id,shot_time)


    val dataRow = combineData(spark, facePersonBodyData, facedetectData, param, category1, category2, startDate)
    logger.info(s"count: ${dataRow.count()}") //count 目的将数据先进行join,再重分区,最后再通过foreachPartition入mppdb

    insertRelationData(spark, dataRow.coalesce(numPartitions = numPartitions), relationMPPDBTableName, param, startDate)

    spark.close()
  }

  @throws[Exception]
  def loadFacePersonBodyData(spark: SparkSession,
                             param: Param,
                             sql: String)
  : RDD[(String, String)] = {
    try {
      val databaseType = param.keyMap.getOrElse("databaseType","hive").toString
      logger.info("facePersonBodyData: " + sql)
      val dataBase = DataBaseFactory(spark, new Properties(), databaseType)

      val rdd = dataBase.query(sql, new QueryBean())
        .rdd
        .map { r =>
          try {
            val personBodyId = r.get(0).toString
            val faceId = r.get(1).toString
            (faceId, personBodyId)
          } catch {
            case _: Exception =>
              ("", "")
          }
        }
        .filter(_._1.nonEmpty)
      rdd
    } catch {
      case ex: Exception =>
        logger.error(ex.getMessage, ex)
        throw new Exception(ex.getMessage)
    }
  }

  @throws[Exception]
  def loadFaceDetectData(spark: SparkSession,
                         param: Param,
                         sql: String)
  : RDD[(String, (String, String, String))] = {

    try {
      val databaseType = param.keyMap.getOrElse("databaseType","hive").toString
      logger.info("faceDetectData: " + sql)
      val dataBase = DataBaseFactory(spark, new Properties(), databaseType)

      val df = dataBase.query(sql, new QueryBean())

      df.rdd
        .map { r =>
          try {
            val personId = r.get(0).toString
            val faceId = r.get(1).toString
            val deviceId = r.get(2).toString
            val shotTime = r.get(3).toString
            (faceId, (personId, deviceId, shotTime))
          } catch {
            case ex: Exception =>
              ("", ("", "", ""))
          }
        }
        .filter(_._1.nonEmpty)
    } catch {
      case ex: Exception =>
        logger.error(ex.getMessage, ex)
        throw new Exception(ex.getMessage)
    }
  }

  def combineData(spark: SparkSession,
                  facePersonBodyData: RDD[(String, String)],
                  facedetectData: RDD[(String, (String, String, String))],
                  param: Param,
                  category1: String,
                  category2: String,
                  statDate: String
                 ): RDD[Row] = {

    val category1Level = spark.sparkContext.broadcast(Constant.PRIORITY_LEVEL(category1))
    val category2Level = spark.sparkContext.broadcast(Constant.PRIORITY_LEVEL(category2))
    val relIdBc = spark.sparkContext.broadcast(relationMap.getOrElse(param.releTypeStr.toLowerCase(), "").toString)
    val relationTypeBc = spark.sparkContext.broadcast(param.relationType)
    val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
    sdf.setTimeZone(TimeZone.getTimeZone(param.zoneId))

    val joinRdd = facedetectData.join(facePersonBodyData)
      .map {
        case (_, ((personId, deviceId, shotTime), personBodyId)) =>
          ((personId, personBodyId), (shotTime, deviceId))
      }
      .reduceByKey((x, _) => x)
      .map {
        case ((personId, personBodyId), (shotTime, deviceId)) =>
          // ID1,ID1_TYPE,ID1_ATTR,ID2,ID2_TYPE,ID2_ATTR
          Row.fromTuple(personId, category1Level.value, "", personBodyId, category2Level.value, "",
            // REL_ID,REL_TYPE,REL_ATTR,REL_GEN_TYPE
            relIdBc.value, relationTypeBc.value, "", 2,
            // FIRST_TIME,LAST_TIME,OCCUR_NUM,SCORE,FIRST_DEVICE_ID,LAST_DEVICE_ID,STAT_DATE,INSERT_TIME
            shotTime, shotTime, 1, 100, deviceId, deviceId, statDate, sdf.format(new Date()))
      }
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    joinRdd
  }

  def insertRelationData(spark: SparkSession,
                         dataRow: RDD[Row],
                         tableName: String,
                         param: Param,
                         statDate: String,
                         outputType: Int = Constant.OVERRIDE)
  : Unit = {
    val relId = relationMap.getOrElse(param.releTypeStr.toLowerCase(), "").toString
    try {
      OtherDBTaleLoadUtil.insertFusionRelationInfo(spark, param, dataRow, tableName, relId)
    } catch {
      case ex: Exception =>
        logger.error(s"结果数据落 ${tableName},请检查：", ex)
    }
  }
}
