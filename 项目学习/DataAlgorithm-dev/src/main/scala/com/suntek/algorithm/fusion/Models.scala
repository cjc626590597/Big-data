package com.suntek.algorithm.fusion

import java.text.SimpleDateFormat
import java.util.{Properties, TimeZone}

import com.suntek.algorithm.common.bean.{QueryBean, SaveParamBean}
import com.suntek.algorithm.common.conf.{Constant, Param}
import com.suntek.algorithm.common.database.DataBaseFactory
import com.suntek.algorithm.common.util.SparkUtil
import com.suntek.algorithm.evaluation.DataManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}


/**
 *
 * 模型融合主类
 *
 * @author liaojp
 *         2020-11-17
 */
object Models {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def genProperties(param: Param): Properties = {
    val properties = new Properties()
    properties.put("numPartitions", param.keyMap.getOrElse("NUM_PARTITIONS", "10"))
    properties
  }

  /**
   * 插入数据库
   * @param ss
   * @param retDetail
   * @param properties
   * @param tableName
   * @param param
   */
  def insertData(ss: SparkSession,
                 retDetail: RDD[(String, String, String, String, String,
                   String, String, String, String, String,
                   String, String, String)],
                 properties: Properties,
                 tableName: String,
                 param: Param)
  : Unit = {
    if (retDetail == null) {
      return
    }

    val databaseType = param.keyMap("databaseType").toString
    val params = "OBJECT_ID_A,TYPE_A,OBJECT_ATTR_A,OBJECT_ID_B,TYPE_B,OBJECT_ATTR_B" +
      ",SCORE,FIRST_TIME,FIRST_DEVICE,LAST_TIME,LAST_DEVICE,TOTAL,REL_ATTR"
    val schema = StructType(List(
      StructField("OBJECT_ID_A", StringType, nullable = true),
      StructField("TYPE_A", StringType, nullable = true),
      StructField("OBJECT_ATTR_A", StringType, nullable = true),
      StructField("OBJECT_ID_B", StringType, nullable = true),
      StructField("TYPE_B", StringType, nullable = true),
      StructField("OBJECT_ATTR_B", StringType, nullable = true),
      StructField("SCORE", IntegerType, nullable = true),
      StructField("FIRST_TIME", LongType, nullable = true),
      StructField("FIRST_DEVICE", StringType, nullable = true),
      StructField("LAST_TIME", LongType, nullable = true),
      StructField("LAST_DEVICE", StringType, nullable = true),
      StructField("TOTAL", IntegerType, nullable = true),
      StructField("REL_ATTR", StringType, nullable = true)
    ))
    //转换成Row
    val rowRdd = retDetail.asInstanceOf[RDD[
      (String, String, String, String, String,
        String, String, String, String, String,
        String, String, String)]].map(r => Row.fromTuple((r._1,r._2,r._3,r._4,r._5,r._6,r._7.toDouble.toInt,r._8.toLong,r._9,r._10.toLong,r._11,r._12.toInt,r._13)))
    val df = ss.createDataFrame(rowRdd, schema)

    val batchIdDateFormat = new SimpleDateFormat("yyyyMMddHH")
    batchIdDateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))

    val database = DataBaseFactory(ss, properties, databaseType)
    val saveParamBean = new SaveParamBean(tableName, Constant.OVERRIDE_DYNAMIC, database.getDataBaseBean)
    saveParamBean.dataBaseBean = database.getDataBaseBean
    saveParamBean.params = params
    val map = DataManager.loadRelation()
    saveParamBean.partitions = Array[(String, String)](
      ("STAT_DATE", s"${param.keyMap("statDate").toString.substring(0, 8)}"),
      ("REL_TYPE", s"${param.relationType}"),
      ("REL_ID", s"${map(param.keyMap("relType").toString)}")
    )
    database.save(saveParamBean, df)
  }

  /**
   * 格式化字段
   * @param x 原始数据
   * @return  格式化后数据
   */
  def format(x: ((String, String), (Double, (String, String, String, String, String, String, String, String, String, String)))): (String, String, String, String, String, String, String, String, String, String, String, String, String) = {
    val ((object_id_a, object_id_b), (score, (type_a, object_attr_a, type_b
    , object_attr_b, first_time, first_device, last_time, last_device, total, rel_attr))) = x
    (object_id_a, type_a, object_attr_a, object_id_b, type_b, object_attr_b
      , score.toString, first_time, first_device, last_time, last_device, total, rel_attr)
  }


  def subDeal(ss: SparkSession, param: Param): Unit ={
    val properties = genProperties(param: Param)
    param.keyMap.put("properties", properties)
    val stat_date = param.keyMap("statDate").toString
    if(stat_date.length<8)  {
      logger.error(s"stat_date 不是正常的时间！")
      return
    }
    val queryParam = new QueryBean()
    queryParam.parameters = List[Object]()
    queryParam.isPage = false
    val topN = param.keyMap.getOrElse("topN","0").toString.toInt
    val data = DataManager.multiLoadData(ss, param, queryParam, similarSqls, similarSchema)
    val fusionData = ModelFusion(data, param.keyMap("method").toString,topN)
    val otherData = DataManager.multiLoadData(ss, param, queryParam, otherSqls, otherSchema)
      .reduce(_.union(_)).groupByKey().mapValues(iter => iter.head)
    val r = fusionData.join(otherData).map(format)
    insertData(ss, r, properties, tableName = "DM_RELATION", param)
  }

  def deal(param: Param): Unit = {
    val ss: SparkSession = SparkUtil.getSparkSession(param.master, "ModelFusionForAccompany", param)
     subDeal(ss,param)
    ss.close()
  }





  def transferweights(param:Param): Map[String, String] = {
    val weights = param.keyMap.getOrElse("weights", s"lcss:0.33,entropy:0.33,dtw:0.33").toString
    val wmap = weights.split(",").map(x => {
      val strs = x.split(":")
      (strs.head.toLowerCase, strs(1))
    }).toMap
    wmap
  }

  val LCSS = "lcss"
  val FPTREE = "fptree"
  val DTW = "dtw"
  val ENTROPY = "entropy"


  /**
   * 获取相似度的sql
   * @param param
   * @return
   */
  def similarSqls(param: Param): Array[String] = {
    val statDate = param.keyMap("statDate").toString.substring(0, 8)
    val relt = param.keyMap("relType").toString
    val minImpactCount = param.keyMap.getOrElse("minImpactCount", "2").toString
    logger.info(s"minImpactCount: ${minImpactCount}")

    val relType = param.relationType
    //todo
    val weightsMap = transferweights(param)

    def coefficient(alg: String) = {
      val maxWeight = "1"
      if (param.keyMap("method").toString.equals(ModelFusionType.AVG)) weightsMap(alg) else maxWeight
    }

    weightsMap.keys.map {
      case LCSS =>
        s"""
           |SELECT  OBJECT_ID_A,OBJECT_ID_B, CAST(SIM_SCORE AS FLOAT) * ${coefficient(LCSS)} FROM  DM_LCSS
           |   WHERE STAT_DATE='$statDate' AND TYPE='$relt' and  REL_TYPE='$relType'  AND  SEQ_SAME_LENGTH >${minImpactCount}
       """.stripMargin
      case FPTREE =>
        s"""
           |SELECT  OBJECT_ID_A,OBJECT_ID_B,CAST(SCORE AS FLOAT) * ${coefficient(FPTREE)} FROM  DM_FPTREE
           |   WHERE STAT_DATE='$statDate' AND TYPE='$relt' AND  REL_TYPE='$relType' AND TIMES>${minImpactCount}
             """.stripMargin
      case DTW =>
        s"""
           |SELECT  OBJECT_ID_A,OBJECT_ID_B, CAST(SCORE AS FLOAT) * ${coefficient(DTW)} FROM  DM_DTW
           |   WHERE STAT_DATE='$statDate' AND TYPE='$relt' and  REL_TYPE='$relType'  AND TIMES>${minImpactCount}
       """.stripMargin

      case ENTROPY =>
        s"""
           |SELECT  OBJECT_ID_A,OBJECT_ID_B, CAST(SCORE AS FLOAT) * ${coefficient(ENTROPY)} FROM  DM_ENTROPY_RESULT
           |   WHERE STAT_DATE='$statDate' AND TYPE='$relt' and  REL_TYPE='$relType' AND TIMES >${minImpactCount}  AND RESULT_TYPE='$relt-per-long-day'
       """.stripMargin

    }.toArray
  }

  /**
   * 获取相似度的schema
   * @param r
   * @return
   */
  def similarSchema(r: Row): ((String, String), Double) = {
    ((r(0).toString, r(1).toString), r(2).toString.toDouble)
  }

  /**
   * 加载其他数据字段
   * @param param
   * @return
   */
  def otherSqls(param: Param): Array[String] = {
    val statDate = param.keyMap("statDate").toString.substring(0, 8)
    val relt = param.keyMap("relType").toString
    val minImpactCount = param.keyMap.getOrElse("minImpactCount", "2").toString
    logger.info(s"minImpactCount: ${minImpactCount}")

    val relType = param.relationType
    val weightsMap = transferweights(param)
    weightsMap.keys.map {
      case LCSS =>
        s"""
           |SELECT OBJECT_ID_A, OBJECT_ID_B ,TYPE_A, '' AS OBJECT_ATTR_A, TYPE_B, '' AS OBJECT_ATTR_B
           | , CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(FIRST_TIME,'yyyy-MM-dd HH:mm:ss'),'yyyyMMddHHmmss')  AS STRING) AS  FIRST_TIME,
           |  FIRST_DEVICE,
           |  CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(LAST_TIME, 'yyyy-MM-dd HH:mm:ss'),'yyyyMMddHHmmss') AS STRING) AS LAST_TIME,
           |   LAST_DEVICE,
           |  CAST(SEQ_SAME_LENGTH AS  STRING) AS TOTAL,
           |    '' AS  REL_ATTR
           |  FROM  DM_LCSS  WHERE STAT_DATE='$statDate' AND TYPE='$relt' AND  REL_TYPE='$relType'  AND    SEQ_SAME_LENGTH >${minImpactCount}
       """.stripMargin
      case FPTREE =>
        s"""
           |SELECT OBJECT_ID_A, OBJECT_ID_B ,TYPE_A, '' AS OBJECT_ATTR_A, TYPE_B, '' AS OBJECT_ATTR_B
           |, FROM_UNIXTIME( FIRST_TIME,'yyyyMMddHHmmss') AS FIRST_TIME , FIRST_DEVICE, FROM_UNIXTIME(LAST_TIME,'yyyyMMddHHmmss') AS LAST_TIME, LAST_DEVICE,   CAST(TIMES AS STRING) AS  TOTAL,'' AS  REL_ATTR
           | FROM  DM_FPTREE  WHERE STAT_DATE='$statDate' AND TYPE='$relt' AND  REL_TYPE='$relType' AND TIMES>${minImpactCount}
       """.stripMargin
      case DTW =>
        s"""
           |SELECT OBJECT_ID_A, OBJECT_ID_B ,TYPE_A, '' AS OBJECT_ATTR_A, TYPE_B, '' AS OBJECT_ATTR_B
           | , CAST(FROM_UNIXTIME( FIRST_TIME,'yyyyMMddHHmmss') AS STRING) AS FIRST_TIME,
           | FIRST_DEVICE,
           | CAST(FROM_UNIXTIME(LAST_TIME,'yyyyMMddHHmmss') AS STRING) AS  LAST_TIME,
           |  LAST_DEVICE,
           |   CAST(TIMES AS STRING) AS TOTAL,'' AS  REL_ATTR
           |  FROM  DM_DTW  WHERE STAT_DATE='$statDate' AND TYPE='$relt' AND  REL_TYPE='$relType' AND TIMES>${minImpactCount}
       """.stripMargin

      case ENTROPY =>
        s"""
           |SELECT OBJECT_ID_A, OBJECT_ID_B ,TYPE_A, '' AS OBJECT_ATTR_A, TYPE_B, '' AS OBJECT_ATTR_B
           | ,  CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(FIRST_TIME,'yyyy-MM-dd HH:mm:ss'),'yyyyMMddHHmmss')  AS STRING) AS  FIRST_TIME, FIRST_DEVICE
           | ,CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(LAST_TIME, 'yyyy-MM-dd HH:mm:ss'),'yyyyMMddHHmmss') AS STRING) AS  LAST_TIME, LAST_DEVICE,
           |   CAST(TIMES AS STRING) AS TOTAL,'' AS  REL_ATTR
           |  FROM  DM_ENTROPY_RESULT  WHERE STAT_DATE='$statDate' AND TYPE='$relt' AND  REL_TYPE='$relType' AND TIMES>${minImpactCount}
           |  AND RESULT_TYPE='$relt-per-long-day'
       """.stripMargin


    }.toArray

  }

  /**
   * 加载其他字段的shema
   * @param r
   * @return
   */
  def otherSchema(r: Row): ((String, String), (String, String, String, String, String
    , String, String, String, String, String)) = {
    ((r(0).toString, r(1).toString), (r(2).toString, r(3).toString, r(4).toString, r(5).toString, r(6).toString
      , r(7).toString, r(8).toString, r(9).toString, r(10).toString, r(11).toString))
  }


}



