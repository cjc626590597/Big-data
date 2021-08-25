package com.suntek.algorithm.fusion

import com.suntek.algorithm.common.bean.QueryBean
import com.suntek.algorithm.common.conf.Param
import com.suntek.algorithm.common.database.DataBaseFactory

import org.apache.spark.sql.{DataFrame, Row}

import com.suntek.algorithm.common.util.{ConstantUtil, SparkUtil}
import org.apache.spark.sql.SparkSession

import org.slf4j.{Logger, LoggerFactory}
import java.util.Properties

import org.apache.spark.sql.functions.max
import org.apache.spark.sql.types.{StringType, StructField, StructType}



/**
  * @author chenb
  * */
//noinspection SpellCheckingInspection
object FusionHandler {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val LCSS = "lcss"
  val FPTREE = "fptree"
  val DTW = "dtw"
  val ENTROPY = "entropy"

  val LCSS_TABLE = "dm_lcss"
  val FPTREE_TABLE = "dm_fptree"
  val DTW_TABLE = "dm_dtw"
  val EMTROPY_TABLE = "dm_entropy_result"

  val tableMap: Map[String, String] = Map[String, String](LCSS -> LCSS_TABLE, FPTREE -> FPTREE_TABLE, DTW -> DTW_TABLE, ENTROPY -> EMTROPY_TABLE)


  def process(param: Param): Unit = {
    val master = param.master
    val taskId = param.keyMap("taskId").toString.toInt
    val stepId = param.keyMap("stepId").toString.toInt
    val waitTimeOut = param.keyMap.getOrElse("waitTimeOut", "10800").toString.toLong
    val startTime = System.currentTimeMillis()
    val parentStepId = param.keyMap.getOrElse("parentStepId", "").toString.trim
    // 检查该任务依赖的任务是否都完成
    var flag = ConstantUtil.checkTask(param.batchId, parentStepId)
    while (flag){
      logger.info(s"该任务依赖的任务还未完成继续等待")
      Thread.sleep(900 * 1000L)
      val endTime = System.currentTimeMillis()
      if(endTime - startTime > waitTimeOut){
        logger.info(s"任务等待${endTime - startTime},超过设定时长$waitTimeOut,强制开跑")
        flag = false
      }else{
        flag = ConstantUtil.checkTask(param.batchId, parentStepId)
      }
    }
    val dataBaseName = param.keyMap.getOrElse("dataBaseName", "default").toString
    val databaseType = param.keyMap.getOrElse("databaseType", "hive").toString
    val weights = param.keyMap.getOrElse("weights", s"$LCSS:0.25,$ENTROPY:0.25,$DTW:0.25,$FPTREE:0.25").toString
    val indexName = param.keyMap.getOrElse("indexName", "relation_index").toString
    val batchSize = param.keyMap.getOrElse("batchSize", "5000").toString.toInt
    val numPartitions = param.keyMap.getOrElse("numPartitions", "10").toString.toInt

    val method = param.keyMap.getOrElse("method", "rank").toString
    val topN = param.keyMap.getOrElse("topN", "30").toString
    val batchId = param.batchId

    val spark_executor_memory_fusion = param.keyMap.getOrElse("spark.executor.memory.fusion", "10g").toString
    val spark_port_maxRetries_fusion = param.keyMap.getOrElse("spark.port.maxRetries.fusion", "500").toString
    val spark_default_parallelism_fusion = param.keyMap.getOrElse("spark.default.parallelism.fusion", "96").toString
    val spark_executor_cores_fusion = param.keyMap.getOrElse("spark.executor.cores.fusion", "10").toString


    val spark_executor_memory_es = param.keyMap.getOrElse("spark.executor.memory.es", "15g").toString
    val spark_port_maxRetries_es = param.keyMap.getOrElse("spark.port.maxRetries.es", "500").toString
    val spark_default_parallelism_es = param.keyMap.getOrElse("spark.default.parallelism.es", "24").toString
    val spark_executor_cores_es = param.keyMap.getOrElse("spark.executor.cores.es", "2").toString

    val minImpactCountResult = param.keyMap.getOrElse("minImpactCountResult", "2").toString

    val relTypes = param.keyMap.getOrElse("relTypes", "imei-imei,imsi-imsi,phone-phone,person-phone,person-imei,mac-mac,person-mac,person-imsi,person-person,car-phone,car-imei,car-mac,car-imsi,car-car").toString

    // 针对每一个relType,获取 statDate weights
    param.keyMap += "spark.executor.memory" -> spark_executor_memory_fusion
    param.keyMap += "spark.port.maxRetries" -> spark_port_maxRetries_fusion
    param.keyMap += "spark.default.parallelism" -> spark_default_parallelism_fusion
    param.keyMap += "spark.executor.cores" -> spark_executor_cores_fusion

    val spark = SparkUtil.getSparkSession(param.master, s"FusionHandler $batchId", param)
    relTypes.split(",").foreach { r =>
      param.keyMap += "relType" -> r
      param.keyMap += "weights" -> weights
      //test test!!!
      param.keyMap("statDate") = "2020052609"
      val p = getParam(spark, param)
      val jsonFusion =
        s"""$master#--#{"analyzeModel":{"inputs":[
           |{"cnName":"最小碰撞次数","enName":"minImpactCount","desc":"最小碰撞次数","inputVal":"${minImpactCountResult}"},
           |{"cnName": "权重", "enName": "weights", "desc": "权重", "inputVal": "${p._2}"},
           |{"cnName": "融合方法", "enName": "method", "desc": "融合方法", "inputVal": "$method"},
           |{"cnName": "关系类型", "enName": "relType", "desc": "关系类型", "inputVal": "$r"},
           |{"cnName": "获取数据时间", "enName": "statDate", "desc": "获取数据时间", "inputVal": "${p._1}"},
           |{"cnName": "topN", "enName": "topN", "desc": "topN", "inputVal": "$topN"},
           |{"cnName": "数据库名称", "enName": "dataBaseName", "desc": "数据库名称", "inputVal": "$dataBaseName"},
           |{"cnName": "数据库类型", "enName": "databaseType", "desc": "数据库类型", "inputVal": "$databaseType"}],
           |"isTag": 1, "tagInfo": [], "mainClass": "com.suntek.algorithm.fusion.Models",
           |"modelId": 99302, "modelName": "模型融合", "stepId": $stepId, "taskId": $taskId,"descInfo": ""},
           |"batchId": $batchId}
       """.stripMargin.trim
      logger.info(s"jsonFusion: $jsonFusion")

      val fusionParam = new Param(jsonFusion)
      Models.subDeal(spark, fusionParam)
    }

    spark.close()

    val jsonEsResult =
      s"""$master#--#{"analyzeModel":{"inputs":[
         |{"cnName":"","enName":"spark.executor.memory","desc":"","inputVal":"$spark_executor_memory_es"},
         |{"cnName":"","enName":"spark.port.maxRetries","desc":"","inputVal":"$spark_port_maxRetries_es"},
         |{"cnName":"","enName":"spark.default.parallelism","desc":"","inputVal":"$spark_default_parallelism_es"},
         |{"cnName":"","enName":"spark.executor.cores","desc":"","inputVal":"$spark_executor_cores_es"},
         |{"cnName":"索引名称","enName":"indexName","desc":"索引名称","inputVal":"$indexName"},
         |{"cnName":"批次导入数据量","enName":"batchSize","desc":"批次导入数据量","inputVal":"$batchSize"},
         |{"cnName":"并发分区数","enName":"numPartitions","desc":"并发分区数","inputVal":"$numPartitions"},
         |{"cnName":"数据库名称","enName":"dataBaseName","desc":"数据库名称","inputVal":"$dataBaseName"},
         |{"cnName":"数据库类型","enName":"databaseType","desc":"数据库类型","inputVal":"$databaseType"}],
         |"isTag":1,"tagInfo":[],"mainClass":"com.suntek.algorithm.fusion.FusionRelationHiveToEs",
         |"modelId": 99302, "modelName": "融合分析结果导入es", "stepId": $stepId, "taskId": $taskId,"descInfo": ""},
         |"batchId": $batchId}
       """.stripMargin.trim
    val esResultParam = new Param(jsonEsResult)
    FusionRelationHiveToEs.process(esResultParam)
  }

  //寻找最大的统计时间，且这个统计时间存在所有的表 例如dm_lcss, dm_dtw, 不存在的表则不考虑，并重新分配权重
  def getParam(spark: SparkSession, param: Param): (String, String) = {

    val weightsMap = transferWeights(param)

    val partitions = weightsMap.keys.map(r =>(r, maxPartition(spark, param, tableMap(r))))

    if (partitions.map(_._2).max == 0L){ // 无数据, statDate = ""
      ("", "")
    }else {
      logger.info(s"partitions: ${partitions.mkString("|++|")}")
      val statDate = partitions.map(_._2).max

      logger.info(s"statDate: $statDate")

      val existDataPartition = partitions.filter(_._2 == statDate)
        .map(r => (r._1, weightsMap(r._1)))
        .toList
      logger.info(s"existDataPartition: ${existDataPartition.mkString("|++|")}")

      val weightSum = existDataPartition.map(_._2.toDouble.formatted("%.4f").toDouble).sum
      logger.info(s"weightSum: $weightSum")
      val weight = existDataPartition.map(r => s"${r._1}:${r._2.toDouble / weightSum}").mkString(",")
      logger.info(s"weight: $weight")
      (statDate.toString, weight)
    }
  }

  def transferWeights(param:Param): Map[String, String] = {
    val weights = param.keyMap.getOrElse("weights", s"lcss:0.33,entropy:0.33,dtw:0.33").toString
    val wmap = weights.split(",").map(x => {
      val strs = x.split(":")
      (strs.head.toLowerCase, strs.last)
    }).toMap
    wmap
  }

  def getMaxPartition(spark: SparkSession, param: Param, tableName: String): Long ={

    val databaseType = param.keyMap("databaseType").toString
    val relType = param.keyMap("relType").toString
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


  def maxPartition(spark: SparkSession, param: Param, tableName: String): Long ={

    val relType = param.keyMap("relType").toString
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
    //dm_lcss: [stat_date=20210518/rel_type=2/type=car-car]
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
}