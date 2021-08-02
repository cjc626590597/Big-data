package com.suntek.algorithm.process.entropy

import com.alibaba.fastjson.JSONObject
import com.suntek.algorithm.algorithm.entropymethod.EntropyAlgorithm
import com.suntek.algorithm.common.bean.{QueryBean, SaveParamBean, TableBean}
import com.suntek.algorithm.common.conf.Param
import com.suntek.algorithm.common.database.DataBaseFactory
import com.suntek.algorithm.common.util.{PropertiesUtil, SparkUtil}
import com.suntek.algorithm.process.entropy.EntropyWeightHandler.{praseIndexConf, praseUnitIndex}
import com.suntek.algorithm.process.util.ReplaceSqlUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created with IntelliJ IDEA.
  * Author: yuanweilin
  * Date: 2020-10-12 10:18
  * Description:熵值法计算结果
  */
object EntropyResultHandler {

  var entropyWeightLoadSqlKey:String = null

  var entropyResultLoadDetailSqlKey:String = null

  //结果表名
  var resultTableName = ""


  /**
    * 加载指标数据
    * @param sparkSession
    * @param param
    * @return
    */
  def loadDetailData(sparkSession: SparkSession, param: Param, loadDetailSql:String) = {
    val tableDetail = new TableBean("","hive",
      "","default","","","default")

    val properties = PropertiesUtil.genJDBCProperties(param)
    val database = DataBaseFactory(sparkSession, properties, tableDetail.databaseType)

    val queryParamBean = new QueryBean()
    queryParamBean.isPage = false

    val execSql  = ReplaceSqlUtil.replaceSqlParam(loadDetailSql, param)

    val queryRet = database.query(execSql, queryParamBean)

    queryRet
  }


  def loadWeightData(sparkSession: SparkSession, param: Param, parentIndexName:String) = {
    val tableDetail = new TableBean("","hive",
      "","default","","","default")

    val properties = PropertiesUtil.genJDBCProperties(param)
    val database = DataBaseFactory(sparkSession, properties, tableDetail.databaseType)

    val queryParamBean = new QueryBean()
    queryParamBean.isPage = false
    var sql = param.keyMap(entropyWeightLoadSqlKey).toString

    sql = sql.replaceAll("@parentIndexName@", parentIndexName)

    val execSql  = ReplaceSqlUtil.replaceSqlParam(sql, param)

    val wegiht = database.query(execSql, queryParamBean)
      .rdd
      .map(r => (r.getString(0), r.getDouble(1)))
      .collectAsMap()

    wegiht
  }


  /**
    * 结果落库
    * @param ss
    * @param param
    * @param retDetail
    */
  def insertData(ss: SparkSession , param: Param, retDetail: RDD[Row], resultType:String): Unit = {
    val params = " OBJECT_ID_A,TYPE_A,OBJECT_ID_B,TYPE_B, SCORE, TIMES, FIRST_TIME, FIRST_DEVICE, LAST_TIME, LAST_DEVICE"
    val schema = StructType(List(
      StructField("OBJECT_ID_A", StringType, nullable = true),
      StructField("TYPE_A", IntegerType, nullable = true),
      StructField("OBJECT_ID_B", StringType, nullable = true),
      StructField("TYPE_B", IntegerType, nullable = true),
      StructField("SCORE", DoubleType, nullable = true),
      StructField("TIMES", LongType, nullable = true),
      StructField("FIRST_TIME", StringType, nullable = true),
      StructField("FIRST_DEVICE", StringType, nullable = true),
      StructField("LAST_TIME", StringType, nullable = true),
      StructField("LAST_DEVICE", StringType, nullable = true)
    ))

    val df = ss.createDataFrame(retDetail, schema)

    val outTable = new TableBean(resultTableName,"hive",
      "","default","","","default")

    val properties = PropertiesUtil.genJDBCProperties(outTable)

    val database = DataBaseFactory(ss, properties, outTable.databaseType)
    val dataBaseBean = database.getDataBaseBean
    dataBaseBean.dataBaseName = outTable.databaseName
    val saveParamBean = new SaveParamBean(outTable.tableName, outTable.outputType, dataBaseBean)
    saveParamBean.dataBaseBean = dataBaseBean
    saveParamBean.params = params
    saveParamBean.numPartitions = param.keyMap.getOrElse("numPartitions", "10").toString.toInt
    saveParamBean.partitions = Array(("STAT_DATE",s"${param.startTime.substring(0,8)}"),("REL_TYPE", s"${param.relationType}"), ("TYPE",s"${param.releTypeStr}") ,("RESULT_TYPE", s"${resultType}"))
    database.save(saveParamBean, df)
  }


  def process(param: Param): Unit = {

    val master = param.master

    entropyWeightLoadSqlKey = param.keyMap.getOrElse("ENTROPY_WEIGHT_LOAD_SQL_KEY", "entropy.weight.load").toString

    resultTableName = param.keyMap.getOrElse("RESULT_TABLE", "DM_ENTROPY_RESULT").toString

    val batchId = param.keyMap.get("batchId").get.toString

    val sparkSession = SparkUtil.getSparkSession(master, s"ENTROPY COMPUTE RESULT ${param.releTypeStr} ${batchId}  Job", param)
    //解析json中配置的指标配置信息
    val indexsArrary = praseIndexConf(param.keyMap("indexs").toString)

    for(i <- 0 to indexsArrary.length -1){

      val index = indexsArrary(i)

      val indexName = index.getString("name")

      val loadDetailSqlKey = index.getString("loadDetailSqlKey")

      val loadDetailSql = param.keyMap(loadDetailSqlKey).toString

      val resultType = indexName

      //按加载明细数据
      val dataRdd = loadDetailData(sparkSession, param, loadDetailSql)

      //加载权重结果
      val indexConfigArray = index.getJSONArray("subIndex").toArray(Array[JSONObject]())

      val weightMap = loadWeightData(sparkSession, param, indexName)

      if(weightMap != null && weightMap.nonEmpty && weightMap.size > 0){
        val weightMapBroadcast = sparkSession.sparkContext.broadcast(weightMap)

        val indexLabelMap: Map[String, (Int, Boolean)] = praseUnitIndex(indexConfigArray).map(m => (m._1 , (m._2, m._3)) ).toMap

        val resultRdd = EntropyAlgorithm.computeResult(sparkSession, dataRdd, weightMapBroadcast, indexLabelMap)

        //结果数据落库
        insertData(sparkSession, param, resultRdd, resultType)
      }else{
        insertData(sparkSession, param, dataRdd.rdd, resultType)

      }

    }
    sparkSession.close()
  }
}
