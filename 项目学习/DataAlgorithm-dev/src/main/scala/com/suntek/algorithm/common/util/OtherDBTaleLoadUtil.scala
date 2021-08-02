package com.suntek.algorithm.common.util

import java.text.SimpleDateFormat
import java.util.{Properties, TimeZone}

import com.suntek.algorithm.common.bean.{QueryBean, SaveParamBean}
import com.suntek.algorithm.common.conf.{Constant, Param}
import com.suntek.algorithm.common.database.DataBaseFactory
import com.suntek.algorithm.process.util.ReplaceSqlUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created with IntelliJ IDEA.
  * Author: yuanweilin
  * Date: 2021-3-26 11:40
  * Description: 用于加载非Hive存储表的工具类，如MPP或者Snowball中的FUSION_RELATION_INFO
  */
object OtherDBTaleLoadUtil {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val PERSON_ARCHIVE_INFO = "mysql.person.archive.info"
  val CAR_ARCHIVE_INFO = "mysql.car.archive.info"

  def insertFusionRelationInfo(sparkSession: SparkSession,
                               param: Param,
                               resultRdd: RDD[Row],
                               resultTableName: String,
                               relId: String): Boolean = {
    try {

      val schema = StructType(List[StructField](
        StructField("ID1", StringType, nullable = true),
        StructField("ID1_TYPE", IntegerType, nullable = true),
        StructField("ID1_ATTR", StringType, nullable = true),

        StructField("ID2", StringType, nullable = true),
        StructField("ID2_TYPE", IntegerType, nullable = true),
        StructField("ID2_ATTR", StringType, nullable = true),

        StructField("REL_ID", StringType, nullable = true),
        StructField("REL_TYPE", IntegerType, nullable = true),
        StructField("REL_ATTR", StringType, nullable = true),
        StructField("REL_GEN_TYPE", IntegerType, nullable = true),

        StructField("FIRST_TIME", StringType, nullable = true),
        StructField("LAST_TIME", StringType, nullable = true),

        StructField("OCCUR_NUM", IntegerType, nullable = true),
        StructField("SCORE", IntegerType, nullable = true),

        StructField("FIRST_DEVICE_ID", StringType, nullable = true),
        StructField("LAST_DEVICE_ID", StringType, nullable = true),

        StructField("STAT_DATE", StringType, nullable = true),
        StructField("INSERT_TIME", StringType, nullable = true)
      ))

      val dfm = sparkSession.createDataFrame(resultRdd, schema)

      var resultDF = sparkSession.emptyDataFrame
      val landDataBase = param.keyMap.getOrElse("landDataBase", "pd_dts").toString
      val preSqlTemplate = s"delete from $resultTableName where stat_date='20@endDate@' and rel_id='$relId'"
      val preSql = ReplaceSqlUtil.replaceSqlParam(preSqlTemplate, param)

      val (databaseType, subProtocol, queryParam) =
        if (param.mppdbType == "snowballdb") { //数据落地于睿帆snowballdb
          resultDF = dfm.drop("INSERT_TIME") // 删除INSERT_TIME字段
          (Constant.SNOWBALLDB, Constant.SNOWBALL_SUB_PROTOCOL, "socket_timeout=3000000")
        } else { //数据落地于华为mppdbc
          resultDF = dfm
          (Constant.GAUSSDB, Constant.GAUSSDB_SUB_PROTOCOL, "characterEncoding=UTF-8&autoReconnect=true&useSSL=false&zeroDateTimeBehavior=convertToNull&serverTimezone=UTC")
        }

      resultDF.show()

      val params =
        """ID1,ID1_TYPE,ID1_ATTR,
                    ID2,ID2_TYPE,ID2_ATTR,
                    REL_ID,REL_TYPE,REL_ATTR,REL_GEN_TYPE,
                    FIRST_TIME,LAST_TIME,
                    OCCUR_NUM,SCORE,
                    FIRST_DEVICE_ID,LAST_DEVICE_ID,
                    STAT_DATE,INSERT_TIME
                 """
      val properties = new Properties()
      val url = s"jdbc:$subProtocol://${param.mppdbIp}:${param.mppdbPort}/$landDataBase?$queryParam"
      logger.info("=============="+param.mppdbType)
      logger.info("jdbc url ====== "+url)
      logger.info(param.mppdbUsername)
      logger.info(param.mppdbPassword)
      properties.setProperty("jdbc.url", url)
      properties.setProperty("username", param.mppdbUsername)
      properties.setProperty("password", param.mppdbPassword)
      val sdf2 = new SimpleDateFormat("yyyyMMdd")
      sdf2.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
      val runTimeStamp = sdf2.parse(param.batchId.substring(0, 8)).getTime - 60 * 60 * 1000L
      val statDate = sdf2.format(runTimeStamp)
      val database = DataBaseFactory(sparkSession, properties, databaseType)
      val saveParamBean = new SaveParamBean(resultTableName, 1, database.getDataBaseBean)
      saveParamBean.dataBaseBean = database.getDataBaseBean
      saveParamBean.params = params
      saveParamBean.preSql = preSql
      saveParamBean.tableName = resultTableName
      saveParamBean.isCreateMppdbPartition = true
      saveParamBean.mppdbPartition = s"p$statDate"
      saveParamBean.mppdbDate = param.batchId.substring(0, 8).toLong
      database.save(saveParamBean, resultDF)

    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        throw new Exception(ex)
    }
  }


  /**
    * 加载车档数据
    * @param spark
    * @param param
    * @return
    */
  def loadCarArchiveData(spark: SparkSession, param: Param): RDD[(String, String)] = {

    val sql = param.keyMap(CAR_ARCHIVE_INFO).toString
    val databaseType = "mysql"
    logger.info("loadCarArchiveData sql: " + sql)
    try {
      val properties = new Properties()
      val sourceDataBase = param.keyMap.getOrElse("sourceDataBase", "archive").toString
      val url = s"jdbc:mysql://${param.mysqlIp}:${param.mysqlPort}/$sourceDataBase?characterEncoding=UTF-8&autoReconnect=true&useSSL=false&zeroDateTimeBehavior=convertToNull&serverTimezone=UTC"
      logger.info(url)
      logger.info(param.mysqlUserName)
      logger.info(param.mysqlPassWord)
      properties.setProperty("jdbc.url", url)
      properties.setProperty("username", param.mysqlUserName)
      properties.setProperty("password", param.mysqlPassWord)
      logger.info(url)
      val queryParam = new QueryBean()
      queryParam.isPage = true
      val df = DataBaseFactory(spark, properties, databaseType)
        .query(sql, queryParam)

      df.rdd
        .map { r =>
          val vehicleId = try {
            r.get(0).toString
          } catch {
            case _: Exception => ""
          }
          val plateNo = try {
            r.get(1).toString
          } catch {
            case _: Exception => ""
          }
          (plateNo, vehicleId)
        }
        .filter(r => r._1.nonEmpty && r._2.nonEmpty)
    } catch {
      case ex: Exception =>
        logger.error(ex.getMessage, ex)
        throw new Exception(ex.getMessage)
    }


  }

  /**
    * 加载人档数据
    * @param spark
    * @param param
    * @param sql
    * @throws
    * @return
    */
  @throws[Exception]
  def loadPersonArchiveData(spark: SparkSession,
                            param: Param )
  : RDD[(String, (String, String, String))] = {

    val sql = param.keyMap(PERSON_ARCHIVE_INFO).toString
    val databaseType = "mysql"
    logger.info("loadPersonArchiveData sql: " + sql)
    try {
      val properties = new Properties()
      val sourceDataBase = param.keyMap.getOrElse("sourceDataBase", "archive").toString
      val url = s"jdbc:mysql://${param.mysqlIp}:${param.mysqlPort}/$sourceDataBase?characterEncoding=UTF-8&autoReconnect=true&useSSL=false&zeroDateTimeBehavior=convertToNull&serverTimezone=UTC"
      logger.info(url)
      logger.info(param.mysqlUserName)
      logger.info(param.mysqlPassWord)
      properties.setProperty("jdbc.url", url)
      properties.setProperty("username", param.mysqlUserName)
      properties.setProperty("password", param.mysqlPassWord)
      logger.info(url)
      val queryParam = new QueryBean()
      queryParam.isPage = true
      DataBaseFactory(spark, properties, databaseType)
        .query(sql, queryParam)
        .rdd
        .map { r =>
          val personId = try {
            r.get(0).toString
          } catch {
            case _: Exception => ""
          }
          val idType = try {
            r.get(1).toString
          } catch {
            case _: Exception => ""
          }
          val idNumber = try {
            r.get(2).toString
          } catch {
            case _: Exception => ""
          }
          val personName = try {
            r.get(3).toString
          } catch {
            case _: Exception => ""
          }
          (personId, (idType, idNumber, personName))
        }
        .filter(r => r._1.nonEmpty && r._2._2.nonEmpty)
        .reduceByKey((x , _) => x)
    } catch {
      case ex: Exception =>
        logger.error(ex.getMessage, ex)
        throw new Exception(ex.getMessage)
    }
  }

}
