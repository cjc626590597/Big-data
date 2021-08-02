package com.suntek.algorithm.fusion

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime}

import com.suntek.algorithm.common.bean.{SaveParamBean, TableBean}
import com.suntek.algorithm.common.conf.Param
import com.suntek.algorithm.common.database.DataBaseFactory
import com.suntek.algorithm.common.util.{OtherDBTaleLoadUtil, PropertiesUtil, SparkUtil}
import com.suntek.algorithm.evaluation.DataManager
import com.suntek.algorithm.process.util.ReplaceSqlUtil
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
  * Created with IntelliJ IDEA.
  * Author: yuanweilin
  * Date: 2021-2-23 19:52
  * Description:人体关联人脸抽取统计
  */
object PersonFaceRelateStatHandler {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val relationMap: mutable.Map[String, Object] = DataManager.loadRelation()

  def loadPersonBodyFaceResult(sparkSession: SparkSession, param: Param): DataFrame = {
    val confKey = param.keyMap.getOrElse("personBodyFaceRelateSqlKey","personBody.face.relate.sql").toString
    val execSql = ReplaceSqlUtil.replaceSqlParam(param.keyMap(confKey).toString, param)

    logger.info(s"execSql === ${execSql}")
     sparkSession.sql(execSql)
                 .persist(StorageLevel.MEMORY_AND_DISK)
  }

  def insertHiveResultTable(sparkSession: SparkSession, param: Param,resultDF: DataFrame): Unit = {
    val params = "OBJECT_ID_A,TYPE_A,OBJECT_ID_B,TYPE_B,CNT,REMARK,FIRST_TIME,LAST_TIME"

    val resultTableName = param.keyMap.getOrElse("RESULT_TABLE", "DM_COMMON_STAT_DAY").toString
    val outTable = new TableBean(resultTableName, "hive",
      "", "", "", "", "default")

    val properties = PropertiesUtil.genJDBCProperties(outTable)

    val database = DataBaseFactory(sparkSession, properties, outTable.databaseType)
    val dataBaseBean = database.getDataBaseBean
    dataBaseBean.dataBaseName = outTable.databaseName
    val saveParamBean: SaveParamBean = new SaveParamBean(outTable.tableName, outTable.outputType, dataBaseBean)
    saveParamBean.params = params
    saveParamBean.numPartitions = param.keyMap.getOrElse("numPartitions", "10").toString.toInt
    saveParamBean.partitions = Array(("STAT_DATE",s"${param.endTime.substring(0, 8)}"), ("TYPE",s"${param.releTypeStr}"))

    database.save(saveParamBean, resultDF)

  }

  def insertOthersDBResultTable(sparkSession: SparkSession, param: Param, df: DataFrame): Unit = {
    val numPartitions = param.keyMap.getOrElse(param.NUMPARTITIONS, "10").toString.toInt
    val resultTableName = "FUSION_RELATION_INFO"
    val relId = relationMap.getOrElse(s"${param.releTypeStr}.relate.sql", "00-0000-0000").toString
    val relType = param.relationType

    val resultRdd = df.rdd
      .map{ row =>
        try {
          val id1 = row.get(0).toString
          val id1_type = row.get(1).toString.toInt
          val id1_attr = ""
          val id2 = row.get(2).toString
          val id2_type = row.get(3).toString.toInt
          val id2_attr = ""

          val rel_id = relId
          val rel_type = relType
          val rel_attr = row.get(5).toString
          val rel_gen_type = 2
          val first_time = row.get(6).toString
          val last_time = row.get(7).toString
          val cnt = row.get(4).toString.toInt
          val score = 100
          val first_device = ""
          val last_device =""
          val stat_date = row.get(8).toString
          val insert_time = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.ofInstant(Instant.now(), param.zoneId))
          (id1, id1_type, id1_attr, id2, id2_type, id2_attr, rel_id, rel_type, rel_attr, rel_gen_type, first_time, last_time, cnt, score, first_device, last_device, stat_date, insert_time)
        }catch {
          case ex: Exception =>
            ex.printStackTrace()
            null
        }
      }
      .filter(r => r != null)
      .map{
        case (id1, id1_type, id1_attr, id2, id2_type, id2_attr, rel_id, rel_type, rel_attr, rel_gen_type, first_time,
              last_time, cnt, score, first_device, last_device, stat_date, insert_time) =>
        Row.fromTuple(id1, id1_type, id1_attr, id2, id2_type, id2_attr, rel_id, rel_type, rel_attr, rel_gen_type, first_time, last_time,
          cnt, score, first_device, last_device, stat_date, insert_time)
    }

    try{
      OtherDBTaleLoadUtil.insertFusionRelationInfo(sparkSession, param, resultRdd.coalesce(numPartitions), resultTableName, relId)
    }catch {
      case ex: Exception =>
        logger.error(s"结果数据落 ${resultTableName},请检查：", ex)
    }

  }

  def process(param: Param): Unit = {

    val sparkSession = SparkUtil.getSparkSession(param.master, s"${param.releTypeStr} PersonFaceRelateStatHandler ${param.batchId} Job", param)

    //注册UDF：praseRelateID
    registerUDF(sparkSession, "praseRelateID", "com.suntek.algorithm.udf.PraseRelateIDUDTF" )

    val resultDF = loadPersonBodyFaceResult(sparkSession, param);

    //落hive的DM_COMMON_STAT_DAY表
    insertHiveResultTable(sparkSession, param,resultDF)

    //落Mpp或者Snowball的FUSION_RELATION_INFO
    resultDF.show()
    insertOthersDBResultTable(sparkSession, param,resultDF)
  }

  /**
    * 注册自定义函数
    * @param funcName
    * @param funClassFullName
    * @return
    */
  def registerUDF(sparkSession:SparkSession, funcName: String, funClassFullName: String) = {

    sparkSession.sql(s"CREATE TEMPORARY FUNCTION ${funcName} as '${funClassFullName}'")
  }
}
