package com.suntek.algorithm.fusion

import com.suntek.algorithm.common.bean.{SaveParamBean, TableBean}
import com.suntek.algorithm.common.conf.Param
import com.suntek.algorithm.common.database.DataBaseFactory
import com.suntek.algorithm.common.util.{OtherDBTaleLoadUtil, PropertiesUtil, SparkUtil}
import com.suntek.algorithm.evaluation.DataManager
import com.suntek.algorithm.process.util.ReplaceSqlUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
  * Created with IntelliJ IDEA.
  * Author: yuanweilin
  * Date: 2021-2-23 19:52
  * Description:同驾乘人员车辆，即同一些人使用不同辆车的关系，从DM_COMMON_STAT表中找出person-car 形成 car-car
  * 最终结果需要将车辆转换为车档ID
  */
object Car2CarSamePersonRelateHandler {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val relationMap: mutable.Map[String, Object] = DataManager.loadRelation()

  def loadPerson2PersonResult(sparkSession: SparkSession, param: Param): DataFrame = {
    val confKey = param.keyMap.getOrElse("c2CSamePersonRelateSqlKey","car-car.relate.sql").toString
    val execSql = ReplaceSqlUtil.replaceSqlParam(param.keyMap(confKey).toString, param)

    val df = sparkSession.sql(execSql)
    df.persist(StorageLevel.MEMORY_AND_DISK)
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

  def insertHiveRelationResultTable(sparkSession: SparkSession, param: Param, df: DataFrame, carArchiveRdd: RDD[(String,String)]): Unit = {

    val resultRdd = df.rdd
                      .map { row =>
                        val object_id_a_key = row.get(0).toString
                        val type_a = row.get(1).toString
                        val object_attr_a = row.get(0).toString
                        val object_id_b_key = row.get(2).toString
                        val type_b = row.get(3).toString
                        val object_attr_b = row.get(2).toString
                        val score = 100
                        val first_time = row.get(6).toString
                        val first_device = ""

                        val last_time = row.get(7).toString
                        val last_device = ""
                        val cnt = row.get(4).toString.toInt
                        val rel_attr = "同驾乘人员车辆"

                        (object_id_a_key, (type_a, object_attr_a, object_id_b_key, type_b, object_attr_b, score,
                          first_time,first_device, last_time, last_device, cnt, rel_attr))
                      }
                      .leftOuterJoin(carArchiveRdd) // 获取A车的档案
                      .map{
                        case (object_id_a_key,((type_a, object_attr_a, object_id_b_key, type_b, object_attr_b, score, first_time,
                        first_device, last_time, last_device, cnt, rel_attr), object_id_a)) =>

//                         val objectIdA =  if (object_id_a.nonEmpty) object_id_a.get else object_id_a_key
                          // 档案查不出车档,返回 -1
                          val objectIdA = if (object_id_a.nonEmpty) object_id_a.get else "-1"

                          (object_id_b_key, (objectIdA, type_a, object_attr_a, type_b, object_attr_b, score,
                            first_time, first_device, last_time, last_device, cnt, rel_attr))
                      }
                      .leftOuterJoin(carArchiveRdd)// 获取B车的档案
                      .map{
                        case (object_id_b_key, ((object_id_a, type_a, object_attr_a, type_b, object_attr_b, score, first_time,
                        first_device, last_time, last_device, cnt, rel_attr), object_id_b)) =>
                          // 档案查不出车档,返回 -1
//                          val objectIdB =  if (object_id_b.nonEmpty) object_id_b.get else object_id_b_key
                          val objectIdB = if (object_id_b.nonEmpty) object_id_b.get else "-1"

                          Row.fromTuple(object_id_a, type_a, object_attr_a, objectIdB, type_b, object_attr_b, score,
                            first_time, first_device, last_time, last_device, cnt, rel_attr)
                      }

    val schema = StructType(List[StructField](
      StructField("OBJECT_ID_A", StringType, nullable = true),
      StructField("TYPE_A", StringType, nullable = true),
      StructField("OBJECT_ATTR_A", StringType, nullable = true),
      StructField("OBJECT_ID_B", StringType, nullable = true),
      StructField("TYPE_B", StringType, nullable = true),
      StructField("OBJECT_ATTR_B", StringType, nullable = true),
      StructField("SCORE", IntegerType, nullable = true),
      StructField("FIRST_TIME", StringType, nullable = true),
      StructField("FIRST_DEVICE", StringType, nullable = true),
      StructField("LAST_TIME", StringType, nullable = true),
      StructField("LAST_DEVICE", StringType, nullable = true),
      StructField("TOTAL", IntegerType, nullable = true),
      StructField("REL_ATTR", StringType, nullable = true)
    ))

    val resultDF = sparkSession.createDataFrame(resultRdd, schema)

    val resultTableName = param.keyMap.getOrElse("RESULT_TABLE", "DM_RELATION").toString
    val params = "OBJECT_ID_A,TYPE_A,OBJECT_ATTR_A,OBJECT_ID_B,TYPE_B,OBJECT_ATTR_B,SCORE,FIRST_TIME,FIRST_DEVICE,LAST_TIME,LAST_DEVICE,TOTAL,REL_ATTR"

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
    resultDF.show()
    database.save(saveParamBean, resultDF)

  }

  def process(param: Param): Unit = {

    val sparkSession = SparkUtil.getSparkSession(param.master, s"${param.releTypeStr} PersonFaceRelateStatHandler ${param.batchId} Job", param)

    //注册UDF：praseRelateID

    val resultDF = loadPerson2PersonResult(sparkSession, param)

    //落hive的DM_COMMON_STAT_DAY表 未转成车档ID
    insertHiveResultTable(sparkSession, param, resultDF)

    val count = resultDF.count()

    if (count > 0){
      //加载车档数据
      val carArchiveRDD = OtherDBTaleLoadUtil.loadCarArchiveData(sparkSession, param)
        .persist(StorageLevel.MEMORY_AND_DISK)

      //落HIVE DM_RELATION表
      insertHiveRelationResultTable(sparkSession, param,resultDF, carArchiveRDD)
    }
    sparkSession.close()

  }
}
