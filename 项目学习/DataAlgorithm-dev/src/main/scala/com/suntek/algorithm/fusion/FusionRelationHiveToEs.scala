package com.suntek.algorithm.fusion

import com.suntek.algorithm.common.bean.QueryBean
import com.suntek.algorithm.common.conf.{Constant, Param}
import com.suntek.algorithm.common.database.DataBaseFactory
import com.suntek.algorithm.common.util.SparkUtil
import com.suntek.algorithm.process.util.EsUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import java.text.SimpleDateFormat
import java.util.{Date, Properties, TimeZone}
import com.alibaba.fastjson.{JSON, JSONObject}
import com.suntek.algorithm.fusion.FusionHandler.allPartition
import org.apache.spark.sql.functions.max
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

/**
 * @author chenb
 * */
//noinspection SpellCheckingInspection
object FusionRelationHiveToEs {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val INDEX_NAME = "indexName"
  val BATCH_SIZE = "batchSize"
  val FUSION_RELATION_DATA_SQL = "fusion.relation.data.sql"
  val FUSION_RELATION_DATA_STAT_DATE_SQL = "fusion.relation.stat.date.sql"
  val CAR_ARCHIVE_INFO_SQL = "mysql.car.archive.info"
  val CAR = "CAR"
  def process(param: Param): Unit = {

    try {
      val indexName = param.keyMap.getOrElse(INDEX_NAME, "relation_index").toString
      val batchSize = param.keyMap.getOrElse(BATCH_SIZE, "5000").toString.toInt

      val jestClient = EsUtil.getJestClient(param.esNodesHttp)

      val typeName = EsUtil.getTypeName(jestClient, indexName)

      EsUtil.close(jestClient)

      val spark: SparkSession = SparkUtil.getSparkSession(param.master, s"FusionRelationHiveToEs ${param.batchId}", param)

      logger.info(s"esNodesHttp: ${param.esNodesHttp}, indexName: $indexName, typeName: $typeName, batchSize: $batchSize")

      val esNodesHttpBc = spark.sparkContext.broadcast(param.esNodesHttp)
      val indexNameBc = spark.sparkContext.broadcast(indexName)
      val typeNameBc = spark.sparkContext.broadcast(typeName)
      val batchSizeBc = spark.sparkContext.broadcast(batchSize)
      // 获取当前批次的数据
      val statDataSql = param.keyMap(FUSION_RELATION_DATA_STAT_DATE_SQL).toString

   //   val data = loadNowBatchData(spark, param, statDataSql, numPartitionsBc).persist(StorageLevel.MEMORY_AND_DISK_SER)
      val data = loadNowBatchData(spark, param, statDataSql)

      // 从车档案表中获取车档标识ID
      val ret = getArchiveData(spark, param, data)

      insertEs(esNodesHttpBc, indexNameBc, typeNameBc, batchSizeBc, ret)

      logger.info(s"insert batch success")

    } catch {
      case ex: Exception =>
        logger.error(ex.getMessage)
        throw ex
    }
  }

  def getArchiveData(spark: SparkSession,
                     param: Param,
                     data: RDD[(String, String)])
  :  RDD[(String, String)] = {
    val archiveSql = param.keyMap(CAR_ARCHIVE_INFO_SQL).toString
    val archiveData = loadMysqlArchive(spark, param, archiveSql)
    if(!archiveData.isEmpty()){
      val ret = data.map(v=>{
        val list = v._1.split("_")
        val id1 = list(0)
        val id1Type = list(1)
        val relId= list(2)
        val id2 = list(3)
        val id2Type = list(4)
        ((id1, id1Type), (id2, id2Type, relId, v._2))
      })
        .leftOuterJoin(archiveData)
        .map(v=> {
          var id1 = v._1._1
          val id1Type = v._1._2
          val id2 = v._2._1._1
          val id2Type = v._2._1._2
          val relId = v._2._1._3
          var value = v._2._1._4
          if (v._2._2.nonEmpty) {
            id1 = v._2._2.get
            val map = JSON.parseObject(value)
            map.put("ID1", id1)
            value = map.toJSONString
          } else if (id1Type == "2") {
            // 档案查不出车档,返回 -1
            id1 = "-1"
            val map = JSON.parseObject(value)
            map.put("ID1", id1)
            value = map.toJSONString
          }
          ((id2, id2Type), (id1, id1Type, relId, value))
        })
        .leftOuterJoin(archiveData)
        .map(v=>{
          var id2 = v._1._1
          val id2Type = v._1._2
          val id1 = v._2._1._1
          val id1Type = v._2._1._2
          val relId = v._2._1._3
          var value = v._2._1._4
          if(v._2._2.nonEmpty){
            id2 = v._2._2.get
            val map = JSON.parseObject(value)
            map.put("ID2", id2)
            value = map.toJSONString
          }else if (id2Type == "2") {
            // 档案查不出车档,返回 -1
            id2 = "-1"
            val map = JSON.parseObject(value)
            map.put("ID2", id2)
            value = map.toJSONString
          }
          val docId = s"${id1}_${id1Type}_${relId}_${id2}_$id2Type"
          (docId, value)
        })
        ret
    }else{
        data.map(v=>{
          val list = v._1.split("_")
          var id1 = list(0)
          val id1Type = list(1)
          val relId= list(2)
          var id2 = list(3)
          val id2Type = list(4)
          if (id1Type == "2"){// 档案查不出车档,返回 -1
            id1 = "-1"
          }
          if (id2Type == "2"){// 档案查不出车档,返回 -1
            id2 = "-1"
          }
          val docId = s"${id1}_${id1Type}_${relId}_${id2}_$id2Type"
          (docId, v._2)
        })
    }
  }

  def groupByTypeMaxState(spark: SparkSession, param: Param, statDataSql: String): RDD[(String, String)] = {
    try {

      val partitions = allPartition(spark, tableName = "dm_relation")
        if(!partitions.isEmpty){
          partitions
            .groupBy("rel_id")
            .agg(max("stat_date") as "maxValue")
            .rdd
            .map {
              case Row(rel_id: String, maxValue: String) => (rel_id, maxValue)
            }
        }else{
          spark.sparkContext.emptyRDD[(String, String)]
        }

    } catch {
      case e: Exception =>
        e.printStackTrace()
        val databaseType = param.keyMap("databaseType").toString
        DataBaseFactory(spark, new Properties(), databaseType)
          .query(statDataSql, new QueryBean())
          .rdd
          .map { r =>
            try {
              //对象A,对象A属性,对象A类型
              val relId = r.get(0).toString
              val statDate = r.get(1).toString
              (relId, statDate)
            } catch {
              case _: Exception => ("", "")
            }
          }
    }
  }

  def loadMysqlArchive(spark: SparkSession,
                       param: Param,
                       sql: String)
  : RDD[((String, String), String)] = {
    val carTypeBC = spark.sparkContext.broadcast(Constant.PRIORITY_LEVEL(CAR.toLowerCase()))
    val databaseType = "mysql"
    logger.info(sql)
    try{
      val properties = new Properties()
      val sourceDataBase = param.keyMap.getOrElse("sourceDataBase", "archive").toString
      val mysql_url = s"jdbc:mysql://${param.mysqlIp}:${param.mysqlPort}/$sourceDataBase?characterEncoding=UTF-8&autoReconnect=true&useSSL=false&zeroDateTimeBehavior=convertToNull&serverTimezone=UTC"
      properties.setProperty("jdbc.url", mysql_url)
      properties.setProperty("username", param.mysqlUserName)
      properties.setProperty("password", param.mysqlPassWord)
      logger.info(mysql_url)
      val queryParam = new QueryBean()
      queryParam.isPage = true
      DataBaseFactory(spark, properties, databaseType)
        .query(sql, queryParam)
        .rdd
        .map(r=> {
          try {
            val vehicle_id = r.get(0).toString
            val plate_no = r.get(1).toString
            ((plate_no, carTypeBC.value.toString), vehicle_id)
          } catch {
            case _: Exception => (("", ""), "")
          }
        })
        .filter(_._1._1.nonEmpty)
        .persist(StorageLevel.MEMORY_AND_DISK)
    }catch {
      case ex: Exception =>
        logger.error(ex.getMessage, ex)
        spark.sparkContext.emptyRDD[((String, String), String)]
    }
  }

  def loadNowBatchData(spark: SparkSession,
                       param: Param,
                       statDataSql: String
                      )
  : RDD[(String, String)] = {
    val databaseType = param.keyMap("databaseType").toString
    val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
    sdf.setTimeZone(TimeZone.getTimeZone(param.zoneId))
    var where = groupByTypeMaxState(spark, param, statDataSql)
      .filter(_._1.nonEmpty)
      .map(r => s" (rel_id='${r._1}' and stat_date='${r._2}') ")
      .collect()
      .mkString(" or ")

    //因为dm_relation已经是空的了，所以直接1=0返回
    if(where.isEmpty){
      logger.warn("######### dm_relation is empty!!!")
      where = " 1=0"
    }
    logger.info(s"where: $where")
    val dataSql = param.keyMap(FUSION_RELATION_DATA_SQL).toString
      .replace("@where@", where)
    DataBaseFactory(spark, new Properties(), databaseType)
      .query(dataSql, new QueryBean())
      .rdd
      .map { r =>
        try {
          //对象A,对象A属性,对象A类型
          val id1 = r.get(0).toString
          val id1Attr = EsUtil.regJson(r.get(1).toString)
          val id1Type = r.get(2).toString
          //对象B,对象B属性,对象B类型,
          val id2 = r.get(3).toString
          val id2Attr = EsUtil.regJson(r.get(4).toString)
          val id2Type = r.get(5).toString
          //关系标识,关系类型,关系属性
          val relId = r.get(6).toString
          val relType = r.get(7).toString.toInt
          val relAttr = EsUtil.regJson(r.get(8).toString)
          //最早关联时间,最近关联时间,关联次数,分数,最早关联设备,最近关联设备
          val firstTime = r.get(9).toString
          val lastTime = r.get(10).toString
          val occurNum = r.get(11).toString.toInt
          val score = r.get(12).toString.toInt
          val firstDeviceId = r.get(13).toString
          val lastDeviceId = r.get(14).toString

          val jsonObject = new JSONObject()
          jsonObject.put("ID1", id1)
          jsonObject.put("ID1_ATTR", id1Attr)
          jsonObject.put("ID1_TYPE", id1Type)
          jsonObject.put("ID2", id2)
          jsonObject.put("ID2_ATTR", id2Attr)
          jsonObject.put("ID2_TYPE", id2Type)

          jsonObject.put("REL_ID", relId)
          jsonObject.put("REL_TYPE", relType.asInstanceOf[Object])
          jsonObject.put("REL_ATTR", relAttr)
          jsonObject.put("REL_GEN_TYPE", 2.asInstanceOf[Object])
          jsonObject.put("FIRST_TIME", firstTime)
          jsonObject.put("LAST_TIME", lastTime)
          jsonObject.put("OCCUR_NUM", occurNum.asInstanceOf[Object])
          jsonObject.put("SCORE", score.asInstanceOf[Object])
          jsonObject.put("FIRST_DEVICE_ID", firstDeviceId)
          jsonObject.put("LAST_DEVICE_ID", lastDeviceId)

          jsonObject.put("INSERT_TIME", sdf.format(new Date()))

          val docId = s"${id1}_${id1Type}_${relId}_${id2}_$id2Type"

          (docId, jsonObject.toJSONString)
        } catch {
          case _: Exception => ("", "")
        }
      }
      .filter(_._1.nonEmpty)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
  }

  def insertEs(esNodesHttpBc: Broadcast[String],
               indexNameBc: Broadcast[String],
               typeNameBc: Broadcast[String],
               batchSizeBc: Broadcast[Int],
               data: RDD[(String, String)]
              ): Unit = {
    val startTime = System.currentTimeMillis()

    data.foreachPartition { r =>
       EsUtil.insertBatch(esNodesHttpBc.value, indexNameBc.value, typeNameBc.value, r.toList, batchSizeBc.value)
    }
    val allCnt = data.count()
    logger.info(s"all Data : $allCnt cost time:${System.currentTimeMillis() - startTime} ms")
    data.unpersist()
  }
}