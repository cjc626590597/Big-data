package com.suntek.algorithm.evaluation

import java.text.SimpleDateFormat
import java.util.{Date, Properties, TimeZone}

import com.suntek.algorithm.common.bean.{QueryBean, SaveParamBean}
import com.suntek.algorithm.common.conf.{Constant, Param}
import com.suntek.algorithm.common.database.DataBaseFactory
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.evaluation.RankingMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.reflect.ClassTag


/**
 * 排序评估，包括 topK准确率，平均准确率，ndcg
 * 支持lcss，fptree
 * 支持 car-car ，imsi_imsi ，car_imsi，imsi_car 四种同行关系的评估
 *
 * @author liaojp
 *         2020-11-17
 */
object RankingEvaluation {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val relTypes= "imei-imei,imsi-imsi,phone-phone,person-phone,person-imei,mac-mac,person-mac,person-imsi,person-person,car-phone,car-imei,car-mac,car-imsi,car-car"
  val algs  = "fptree,dtw,entropy,fusion"



  def genProperties(param: Param): Properties = {
    val properties = new Properties()
    properties.put("numPartitions", param.keyMap.getOrElse("NUM_PARTITIONS", "10"))
    properties
  }


  def loadData(ss: SparkSession,
               param: Param,
               sql: String,
               queryParam: QueryBean): RDD[Row] = {
    val databaseType = param.keyMap("databaseType").toString
    val properties = param.keyMap("properties").asInstanceOf[Properties]
    val dataBase = DataBaseFactory(ss, properties, databaseType)
    val data = dataBase.query(sql, queryParam)
      .rdd
    data
  }


  def insertData(ss: SparkSession,
                 retDetail: RDD[(String, String, String, String, String)],
                 properties: Properties,
                 tableName: String,
                 param: Param)
  : Unit = {
    if (retDetail == null) {
      return
    }

    val databaseType = param.keyMap("databaseType").toString
    val params = "POSID,NEGID,THR_or_FPR,VAL_or_TPR,EVALUATION"

    //  (具体算法，正样本集ID，负样本集ID，阈值，值，"评估指标")
    val schema = StructType(List(
      StructField("POSID", StringType, nullable = true),
      StructField("NEGID", StringType, nullable = true),
      StructField("THR_or_FPR", DoubleType, nullable = true),
      StructField("VAL_or_TPR", DoubleType, nullable = true),
      StructField("EVALUATION", StringType, nullable = true)
    ))
    //转换成Row
    val rowRdd = retDetail.asInstanceOf[RDD[(String, String, String, String, String)]]
      .map(r => Row.fromTuple((r._1,r._2,if(r._3.isEmpty) 0 else r._3.toDouble,r._4.toDouble,r._5)))
    val df = ss.createDataFrame(rowRdd, schema)

    val batchIdDateFormat = new SimpleDateFormat("yyyyMMddHH")
    batchIdDateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))

    val database = DataBaseFactory(ss, properties, databaseType)
    val saveParamBean = new SaveParamBean(tableName, Constant.OVERRIDE_DYNAMIC, database.getDataBaseBean)
    saveParamBean.dataBaseBean = database.getDataBaseBean
    saveParamBean.params = params
    saveParamBean.numPartitions = param.keyMap.getOrElse("numPartitions", "10").toString.toInt
    saveParamBean.partitions = Array[(String, String)](("alg", s"${param.keyMap("algorithm")}-${param.keyMap("aType")}")
      , ("bd", s"${param.keyMap("batchId").toString.substring(0, 8)}")
      , ("ad", s"${batchIdDateFormat.format(new Date())}")
    )
    database.save(saveParamBean, df)
  }

  /**
   *
   *
   * 计算 topK准确率，平均准确率，ndcg
   *
   * @param ss        SparkSession
   * @param pred_data (OBJA,OBJB,SCORE)
   * @param posSetBc  Broadcast[Set(String,String)]
   * @param param     Param
   * @return
   */
  def evaluation(ss: SparkSession,
                 pred_data: RDD[(String, String, Double)],
                 //                 truth_data: RDD[(String, String, String)],
                 posSetBc: Broadcast[Set[(String, String)]],
                 param: Param)
  : RDD[(String, String, String, String, String)] = {

    var initRDD: RDD[(String, String, Double)] = null


   //车车，imsi_imsi需要预测数据正反，所以反过来重复算，后面按分数取最大去重
    val aType = param.keyMap("aType").toString
    val strs = aType.split("-")
    if(strs(0).equals(strs(1))) {
      initRDD = pred_data.map { case (obja, objb, score) => (objb, obja, score) }
      logger.info(s"测试：类型一样")
    }




    val targetSet: Set[String] = posSetBc.value.map(_._1) // truth_data.map(_._2).collect().toSet
    val targetSetBc = ss.sparkContext.broadcast(targetSet)
    val pred_data_all = if (initRDD != null) pred_data.union(initRDD) else pred_data
    val pred_arry =
    //只处理需要评估指标的车牌或imsi,
      pred_data_all.filter(x => targetSetBc.value.contains(x._1))
        .groupBy(_._1)
        .map(groupAndDistinct)
    //    val truth_arry = truth_data.map { case (_, obja, other) => (obja, other.split(":")) }
    //转换成 RDD(车牌，Array(车牌1,车牌2，车牌3）
    val truth_arry_map = posSetBc.value.groupBy(_._1)
      .map { case (str, tuples) => (str, tuples.map(_._2).toArray) }.toSeq
    val truth_arry = ss.sparkContext.parallelize(truth_arry_map)
    val preLabel = pred_arry.join(truth_arry).values
    val metrics = new RankingMetrics(preLabel)
    val pid = param.keyMap("posId").toString
    val nid = "neg" + pid
    val ldata = (1 to 20).flatMap { k =>
      Seq((pid, nid, k.toString, metrics.precisionAt(k).toString, "precisionAtk"),
        (pid, nid, k.toString, metrics.ndcgAt(k).toString, "ndcgAtk"),
        (pid, nid, k.toString, recallAtk(preLabel, k, isSorted = false).toString, "RecallAtk")
      )
    }.+:((pid, nid, "", metrics.meanAveragePrecision.toString, "MAP"))

    ss.sparkContext.parallelize(ldata)
  }

  /**
   * 1.每个分组下可能重复的情况，以分值最高的方式做去重，
   * 2.并对每个分组按倒序排序
   *
   * @param elem
   * @return
   */
  def groupAndDistinct(elem: (String, Iterable[(String, String, Double)])): (String, Array[String]) = {
    val (obj, iter) = (elem._1, elem._2)
    //iter => id, obja, objb, score
    //group by objb,可能车牌重复，取关联车牌最大
    val others = iter.groupBy(_._2).map { case (_, iter) =>
      iter.maxBy { case (_, _, score) => score }
      //为了排名评估按倒序先排
    }.toList.sortWith(_._3 > _._3)
      .map { case (_, objb, _) =>
        objb
      }.toArray
    (obj, others)
  }

  /**
   * 加载预测结果的数据
   *
   * @param ss         SparkSession
   * @param param      Param
   * @param queryParam QueryBean
   * @return
   */
  def loadPreData(ss: SparkSession, param: Param, queryParam: QueryBean): RDD[(String, String, String, Double)] = {

    val alg = param.keyMap("algorithm")
    val date = param.keyMap("batchId").toString.substring(0, 8)
    val pred_sql =
      s"""
         |SELECT ID,OBJA,OBJB,SCORE
         |  FROM  TB_ACC_PREDICT_RESULT
         |   WHERE  alg = '$alg' and  bd='$date'
       """.stripMargin
    logger.info(pred_sql)
    val pred_data = loadData(ss, param, pred_sql, queryParam)
      .map { case Row(id: String, obja: String, objb: String, score: Double) =>
        (id, obja, objb, score)
      }
    pred_data
  }

  /**
   * 加载真实的数据
   *
   * @param ss         SparkSession
   * @param param      Param
   * @param queryParam QueryBean
   * @return
   */
  def loadTruth_data(ss: SparkSession, param: Param, queryParam: QueryBean): RDD[(String, String, String)] = {
    val date = param.keyMap("batchId").toString.substring(0, 8)
    val a_type = param.keyMap("aType").toString
    val sql_truth =
      s"""
         |SELECT  ID,OBJA,OTHER FROM  TB_ACC_TRUTH_SET
         |   WHERE type='$a_type' AND bd='$date'
       """.stripMargin
    logger.info(sql_truth)
    val truth_data = loadData(ss, param, sql_truth, queryParam)
      .map { case Row(id: String, obja: String, other: String) => (id, obja, other) }
    truth_data
  }

  def verify(ss: SparkSession,
             param: Param): Unit = {
    val properties = genProperties(param: Param)




    relTypes.split(",").foreach(atype=>{
      param.keyMap.put("aType",atype)
      val statDay = maxPartition(ss,param,"dm_lcss")
      val batchId = statDay.toString+"000000"
      if(statDay != 0){
        algs.split(",").foreach(alg=>{
          param.keyMap.put("algorithm",alg)
          param.keyMap.put("batchId",batchId)
          param.keyMap.put("statDate",batchId)
          param.keyMap.put("posId",atype+"-"+batchId)
          param.keyMap.put("properties", properties)
          param.keyMap.put("databaseType",Constant.HIVE)
          val queryParam = new QueryBean()
          queryParam.parameters = List[Object]()
          queryParam.isPage = false
          val tableName = param.keyMap("tableName").toString

          val (pred_data, truth_data) = DataManager.loadData(ss, param, queryParam, dataTransform)
          val r = evaluation(ss, pred_data, truth_data, param)
          insertData(ss, r, properties, tableName, param)
        })

      }else {
        logger.error(s"======没有 lcss 类型为：$atype  的数据")
      }

    })


  }

  /**
   * 透传
   *
   * @param simData  相似度数据 （目标1，目标2，相似度）
   * @param posSetBc 正样本（目标1，目标2）
   * @param param    参数
   * @param ss       SparkSession
   * @return
   */
  def dataTransform(simData: RDD[(String, String, Double)], posSetBc: Broadcast[Set[(String, String)]], param: Param, ss: SparkSession)
  : (RDD[(String, String, Double)], Broadcast[Set[(String, String)]]) = {
    (simData, posSetBc)
  }


  def recallAtk[T: ClassTag](predictionAndLabels: RDD[(Array[T], Array[T])], k: Int, isSorted: Boolean): Double = {
    predictionAndLabels.map { case (pred, lab) =>
      val labSet = lab.toSet

      if (labSet.nonEmpty) {
        var i = 0
        var cnt = 0
        var precSum = 0.0
        val n = math.min(pred.length, k)

        while (i < n) {
          if (labSet.contains(pred(i))) {
            cnt += 1
            precSum = if (isSorted)  precSum + cnt.toDouble /(i + 1)else cnt

          }
          i += 1
        }
        precSum / labSet.size
      } else {
        logger.warn("Empty ground truth set, check input data")
        0.0
      }
    }.mean()
  }


  def maxPartition(spark: SparkSession, param: Param, tableName: String): Long ={

    val relType = param.keyMap("aType").toString
    val maxStateTime =
      try{
        val r =  allPartition(spark,tableName)
        if(!r.isEmpty){
          val filterR = r.where( s" type='$relType'" )
            .groupBy("rel_type")
            .agg(max("stat_date") as "maxValue")
            .select("maxValue").collect()
          if(filterR.isEmpty)
            0L
          else
            filterR.head.getString(0).toLong
        }else{
          0L
        }

      }catch {
        case e: Throwable =>e.printStackTrace()
          getMaxPartition(spark,param,tableName)

      }

    logger.info(s" tableName:$tableName relType:$relType,maxStateTime:$maxStateTime")
    maxStateTime
  }

  val SPLITPATITIONS="/"
  val INNERSPLIT="="

  /**
   * 获得所有的分区。
   *
   * @param spark SparkSession
   * @param tableName tableName
   * @return
   */
  def allPartition(spark: SparkSession, tableName: String): DataFrame ={
    val dm = spark.sql(s"show partitions $tableName ").collect()
    //Row("stat_date=2020052609/ rel_type=2/ rel_id=06-0004-0002-02")
    if (dm.nonEmpty){
      val schemaArray = dm.head.getString(0).split(SPLITPATITIONS).map(x=> {
        val strs = x.split(INNERSPLIT)
        StructField(strs.head, StringType, nullable = false)
      })

      val data  = dm.map{ row =>
        val partions =  row.getString(0).split(SPLITPATITIONS)
        val r = partions.map(x=>{
          val strs = x.split(INNERSPLIT)
          strs.last
        })
        Row.fromSeq(r)
      }.toList

      val schema = StructType(schemaArray)
      val df =  spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df
    }else {
      spark.emptyDataFrame
    }
  }
  def getMaxPartition(spark: SparkSession, param: Param, tableName: String): Long ={

    val databaseType = param.keyMap("databaseType").toString
    val relType = param.keyMap("aType").toString
    val releTypeStr = relType.toLowerCase
    //关系类型
    var relationType = 1 //默认为要素关联
    val releTypes: Array[String] = releTypeStr.split("-")
    //如果关联类型一样，则关系类型为2，即实体关系
    if(releTypes(0).equals(releTypes(1))){
      relationType = 2
    }
    val sql =
      s"""
         | SELECT
         |       MAX(STAT_DATE)
         | FROM
         |       $tableName
         | WHERE TYPE='$relType' and  REL_TYPE='$relationType'
       """.stripMargin

    val array = DataBaseFactory(spark, new Properties(), databaseType)
      .query(sql, new QueryBean())
      .rdd
      .map { r =>
        try {
          r.get(0).toString.toLong
        }catch {
          case _ : Exception => 0L
        }
      }
      .filter( _ > 0L)
      .collect()

    if (array.nonEmpty) array.max else 0L
  }


}



