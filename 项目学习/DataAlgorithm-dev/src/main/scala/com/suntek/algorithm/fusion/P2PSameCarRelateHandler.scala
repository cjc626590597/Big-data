package com.suntek.algorithm.fusion

import com.suntek.algorithm.common.bean.{SaveParamBean, TableBean}
import com.suntek.algorithm.common.conf.Param
import com.suntek.algorithm.common.database.DataBaseFactory
import com.suntek.algorithm.common.util.{PropertiesUtil, SparkUtil}
import com.suntek.algorithm.evaluation.DataManager
import com.suntek.algorithm.fusion.PersonFaceRelateStatHandler.logger
import com.suntek.algorithm.process.util.ReplaceSqlUtil
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
  * Created with IntelliJ IDEA.
  * Author: yuanweilin
  * Date: 2021-2-23 19:52
  * Description:共用车辆人员，即使用同一辆车人员关系，从DM_COMMON_STAT表中找出person-car 形成 person-person关系
  */
object P2PSameCarRelateHandler {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val relationMap: mutable.Map[String, Object] = DataManager.loadRelation()

  def loadPerson2PersonResult(sparkSession: SparkSession, param: Param): DataFrame = {
    val confKey = param.keyMap.getOrElse("p2pSameCarRelateSqlKey","person-person.relate.sql").toString
    val execSql = ReplaceSqlUtil.replaceSqlParam(param.keyMap(confKey).toString, param)
    logger.info(s"execSql === ${execSql}")
    val df = sparkSession.sql(execSql)
    df
//    df.persist(StorageLevel.MEMORY_AND_DISK)
  }

  def insertHiveResultTable(sparkSession: SparkSession, param: Param,resultDF: DataFrame): Unit = {
    val params = "OBJECT_ID_A,TYPE_A,OBJECT_ID_B,TYPE_B,CNT,REMARK,FIRST_TIME,LAST_TIME"

    val resultTableName = param.keyMap.getOrElse("RESULT_TABLE", "DM_COMMON_STAT").toString
    val outTable = new TableBean(resultTableName, "hive",
      "", "", "", "", "default")

    val properties = PropertiesUtil.genJDBCProperties(outTable)

    val database = DataBaseFactory(sparkSession, properties, outTable.databaseType)
    val dataBaseBean = database.getDataBaseBean
    dataBaseBean.dataBaseName = outTable.databaseName
    val saveParamBean: SaveParamBean = new SaveParamBean(outTable.tableName, outTable.outputType, dataBaseBean)
    saveParamBean.params = params
    saveParamBean.numPartitions = param.keyMap.getOrElse("numPartitions", "10").toString.toInt
    saveParamBean.partitions = Array(
      ("STAT_DATE",s"${param.endTime.substring(0, 8)}"),
      ("REL_TYPE",s"${param.relationType}"),
      ("TYPE",s"${param.releTypeStr}")
    )

    database.save(saveParamBean, resultDF)

  }

  def insertHiveRelationResultTable(sparkSession: SparkSession, param: Param, df: DataFrame): Unit = {

    val params = "OBJECT_ID_A,TYPE_A,OBJECT_ATTR_A,OBJECT_ID_B,TYPE_B,OBJECT_ATTR_B,SCORE,FIRST_TIME,FIRST_DEVICE,LAST_TIME,LAST_DEVICE,TOTAL,REL_ATTR"

    val resultTableName = param.keyMap.getOrElse("RESULT_TABLE", "DM_RELATION").toString
    val outTable = new TableBean(resultTableName, "hive",
      "", "", "", "", "default")

    val properties = PropertiesUtil.genJDBCProperties(outTable)
    val database = DataBaseFactory(sparkSession, properties, outTable.databaseType)
    val dataBaseBean = database.getDataBaseBean
    dataBaseBean.dataBaseName = outTable.databaseName
    val saveParamBean: SaveParamBean = new SaveParamBean(outTable.tableName, outTable.outputType, dataBaseBean)
    saveParamBean.params = params
    saveParamBean.numPartitions = param.keyMap.getOrElse("numPartitions", "10").toString.toInt
    saveParamBean.partitions = Array(
      ("STAT_DATE",s"${param.endTime.substring(0, 8)}"),
      ("REL_TYPE",s"${param.relationType}"),
      ("REL_ID", s"${relationMap.getOrElse(s"${param.releTypeStr}.relate.sql", "")}")
    )
//    df.show()
    database.save(saveParamBean, df)

  }

  def process(param: Param): Unit = {

    val sparkSession = SparkUtil.getSparkSession(param.master, s"${param.releTypeStr} PersonFaceRelateStatHandler ${param.batchId} Job", param)

    //注册UDF：praseRelateID

    val resultDF = loadPerson2PersonResult(sparkSession, param);

    //落hive的DM_COMMON_STAT_DAY表 看情况放开吧，避免两份数据
//    insertHiveResultTable(sparkSession, param,resultDF)

    //落HIVE DM_RELATION表
    insertHiveRelationResultTable(sparkSession, param,resultDF)


  }


}
