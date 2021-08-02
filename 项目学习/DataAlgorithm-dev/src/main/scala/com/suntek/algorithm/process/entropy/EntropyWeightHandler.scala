package com.suntek.algorithm.process.entropy

import com.alibaba.fastjson.{JSON, JSONObject}
import com.suntek.algorithm.algorithm.entropymethod.EntropyAlgorithm
import com.suntek.algorithm.common.bean.{QueryBean, SaveParamBean, TableBean}
import com.suntek.algorithm.common.conf.Param
import com.suntek.algorithm.common.database.DataBaseFactory
import com.suntek.algorithm.common.util.{PropertiesUtil, SparkUtil}
import com.suntek.algorithm.process.util.ReplaceSqlUtil
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._


/**
  * Created with IntelliJ IDEA.
  * Author: yuanweilin
  * Date: 2020-10-12 10:18
  * Description:熵值法指标权重计算
  */
object EntropyWeightHandler {

  var entropyWeightLoadDetailSqlKey = ""

  var resultTableName =""

  /***
    *从分布统计表加载指标数据，用熵值法加载多久数据？一天的吗？
    * @param sparkSession
    * @param param
    * @return
    */
  def loadData(sparkSession: SparkSession, param : Param , sql:String): DataFrame = {
    val tableDetail = new TableBean("","hive",
      "","default","","","default")

    val properties = PropertiesUtil.genJDBCProperties(param)
    val database = DataBaseFactory(sparkSession, properties, tableDetail.databaseType)

    val queryParamBean = new QueryBean()
    queryParamBean.isPage = false

    val execSql  = ReplaceSqlUtil.replaceSqlParam(sql, param)

    val queryRet = database.query(execSql, queryParamBean)

    queryRet
  }


  /**
    * 解析指标配置，并根据指标级别排序，级别越高指标越低级，计算优先级越高
    * @param indexConfStr
    * @return
    */
  def praseIndexConf(indexConfStr: String): Array[JSONObject] = {
    if(StringUtils.isBlank(indexConfStr)){

      return null

    }else{

      val indexsArrary = JSON
        .parseArray(indexConfStr)
        .toArray(Array[JSONObject]())
        .sortWith((a, b) => a.getIntValue("level") > b.getIntValue("level"))

      indexsArrary
    }
  }


  /**
    * 解析最小单元的指标配置，指标即加载的字段名，指标1:0/1（0定性指标，1定量指标）;指标2:0/1（0定性指标，1定量指标）
    * @param indexConfStr
    * @return
    */
  def praseUnitIndex(indexConfArray: Array[JSONObject]): Array[(String, Int, Boolean)] = {
    if(indexConfArray == null || indexConfArray.length == 0 ){
      return null
    }else{
      indexConfArray.map(m => (m.getString("name"), m.getIntValue("type"), m.getBooleanValue("isNormalize")))
    }

  }



  /**
    * 权重结果落库
    * @param ss
    * @param retDetail
    * @param param
    */
  def insertData(ss: SparkSession , param: Param, retDetail: List[(String, Double)], parentIndexName: String): Unit = {
    val params = " INDEX_NAME, WEIGHT"
    val schema = StructType(List(
      StructField("INDEX_NAME", StringType, nullable = true),
      StructField("WEIGHT", DoubleType, nullable = true)
    ))

    val resultRow = retDetail.map(m => Row(m._1, m._2))

    val df = ss.createDataFrame(resultRow.asJava, schema)
//
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
    saveParamBean.partitions = Array(("STAT_DATE",s"${param.startTime.substring(0,8)}"), ("TYPE",s"${param.releTypeStr}"), ("PARENT_INDEX_NAME",s"${parentIndexName}"))
    database.save(saveParamBean, df)
  }

  def process(param: Param): Unit = {

    val master = param.master

    resultTableName = param.keyMap.getOrElse("RESULT_TABLE", "DM_ENTROPY_WEIGHT").toString

    val batchId = param.keyMap.get("batchId").get.toString

    val sparkSession = SparkUtil.getSparkSession(master, s"ENTROPY COMPUTE  WEIGHT  ${param.releTypeStr} ${batchId}  Job", param)

    //解析json中配置的指标配置信息
     val indexsArrary = praseIndexConf(param.keyMap("indexs").toString)


    for(i <- 0 to indexsArrary.length -1){

      val index = indexsArrary(i)

      val indexName = index.getString("name")

      param.keyMap.put("resultType",indexName)

      val featureSqlKey = index.getString("featureSqlKey")

      val loadSql = param.keyMap(featureSqlKey).toString
      //按天加载数据
      val dataRdd = loadData(sparkSession, param, loadSql)

      val indexConfigArray = index.getJSONArray("subIndex").toArray(Array[JSONObject]())

      val indexLabelArray: Array[(String, Int, Boolean)] = praseUnitIndex(indexConfigArray)

      val weightList = EntropyAlgorithm.computeWeight(param, dataRdd, indexLabelArray )

      insertData(sparkSession, param , weightList, indexName)

    }

    sparkSession.close()
  }
}
