package com.suntek.algorithm.fusion

import com.alibaba.fastjson.JSONObject
import com.suntek.algorithm.common.bean.QueryBean
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
 * 人脸Id与人员证件关联分析
 *
 * @author chenb
 * */
//noinspection SpellCheckingInspection
@SerialVersionUID(2L)
object FaceIdentificationHandler extends Serializable {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val CATEGORY1 = "category1"
  val CATEGORY2 = "category2"
  val FACE_DETECT_LOAD_DAY = "face.detect.load.day"
  val RELATION_MPPDB_TABLE_NAME = "FUSION_RELATION_INFO"

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
    val spark: SparkSession = SparkUtil.getSparkSession(param.master, s"FaceIdentificationHandler_$batchId", param)

    val numPartitions = param.keyMap.getOrElse(param.NUMPARTITIONS, "10").toString.toInt

    val sdf = new SimpleDateFormat("yyyyMMdd")
    sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    val runTimeStamp = sdf.parse(batchId).getTime - 60 * 60 * 1000L
    val startDate = sdf.format(runTimeStamp)
    val statDate = sdf.format(sdf.parse(batchId).getTime).substring(2)

    val personArchiveData = OtherDBTaleLoadUtil.loadPersonArchiveData(spark, param) //personId, (idType, idNumber, personName)

    val sql2 = param.keyMap(FACE_DETECT_LOAD_DAY).toString
      .replaceAll("@startDate@", startDate.substring(2))

    val facedetectData = loadFaceDetectData(spark, param, sql2) // person_id,(face_id,device_id,shot_time)

    val dataRow = combineData(spark, personArchiveData, facedetectData, param, category1, category2, startDate)
    logger.info(s"count: ${dataRow.count()}") //count 目的将数据先进行join,再重分区,最后再通过foreachPartition入mppdb
    insertRelationData(spark, dataRow.coalesce(numPartitions = numPartitions), RELATION_MPPDB_TABLE_NAME, param, startDate)

    spark.close()
  }


  @throws[Exception]
  def loadFaceDetectData(spark: SparkSession,
                         param: Param,
                         sql: String)
  : RDD[(String, (String, String, Long))] = {

    try {
      val databaseType = param.keyMap.getOrElse("databaseType","hive").toString

      logger.info("faceDetectData sql: " + sql)
      val dataBase = DataBaseFactory(spark, new Properties(), databaseType)
      dataBase.query(sql, new QueryBean())
        .rdd
        //person_id,info_id,device_id,shot_time
        .map { r =>
        try {
          val personId = r.get(0).toString
          //在整合表中face_id不等于原始表的face_id，只能用整合表的infoId关联原始表的viid_object_id
          val faceId = r.get(1).toString
          val deviceId = r.get(2).toString
          val shotTime = r.get(3).toString.toLong
          (personId, (faceId, deviceId, shotTime))
        } catch {
          case _: Exception =>
            ("", ("", "", 0L))
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
                  personArchiveData: RDD[(String, (String, String, String))],
                  facedetectData: RDD[(String, (String, String, Long))],
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

    facedetectData.join(personArchiveData)
      .map {
        case (_, ((faceId, deviceId, shotTime), (idType, idNumber, personName))) =>
          val jsonObject = new JSONObject()
          jsonObject.put("ID_TYPE", idType)
          jsonObject.put("ID_NUMBER", idNumber)
          jsonObject.put("PERSON_NAME", personName)

          ((faceId, idNumber), (jsonObject.toJSONString, shotTime, deviceId))
      }
      .reduceByKey((x, _) => x)
      .map {
        case ((faceId, idNumber), (id2Attr, shotTime, deviceId)) =>

          // ID1,ID1_TYPE,ID1_ATTR,ID2,ID2_TYPE,ID2_ATTR
          Row.fromTuple(faceId, category1Level.value, "", idNumber, category2Level.value, id2Attr,
            // REL_ID,REL_TYPE,REL_ATTR,REL_GEN_TYPE
            relIdBc.value, relationTypeBc.value, "", 2,
            // FIRST_TIME,LAST_TIME,OCCUR_NUM,SCORE,FIRST_DEVICE_ID,LAST_DEVICE_ID,STAT_DATE,INSERT_TIME
            shotTime.toString, shotTime.toString, 1, 100, deviceId, deviceId, statDate, sdf.format(new Date()))
      }
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
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
