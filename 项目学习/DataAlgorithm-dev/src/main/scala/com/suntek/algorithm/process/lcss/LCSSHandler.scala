package com.suntek.algorithm.process.lcss

import com.suntek.algorithm.common.conf.Param
import com.suntek.algorithm.common.util.{EnvUtil, SparkUtil}
import com.suntek.algorithm.process.dtw.{DtwDayHandler, DtwResultHandler}
import com.suntek.algorithm.process.util.Utils
import org.slf4j.{Logger, LoggerFactory}

import java.util.StringJoiner
import java.util.concurrent.{CountDownLatch, ExecutorService, Executors, TimeUnit}

/**
  * @author zhy
  * @date 2020-12-14 16:54
  */
//noinspection SpellCheckingInspection,ScalaDocUnknownTag
object LCSSHandler {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)


  def shell(batchId: String, secondsSeriesThresholdHour: String,
            timeSeriesThreshold: String, relType: String): String ={
    var path = EnvUtil.getEnv("ALG_DEPLOY_HOME")
    if (path == null ){
      path = "/opt/mda/data-algorithm"
    }

    val shellCommand = path + "/bin/batch/run_hour.sh"
    val sj = new StringJoiner(" ")
    sj.add("sh")
    sj.add(shellCommand)
    sj.add(batchId)
    sj.add(batchId)
    sj.add(secondsSeriesThresholdHour)
    sj.add(timeSeriesThreshold)
    sj.add(relType)

    val command = sj.toString
    logger.info("exec shell command: " + command)
    command
  }

  def process(param: Param): Unit = {
    val master = param.master
    val relType = param.keyMap("relType").toString
    val dataBaseName = param.keyMap.getOrElse("dataBaseName", "default").toString
    val databaseType = param.keyMap.getOrElse("databaseType", "hive").toString
    val batchId = param.batchId

    val spark_executor_memory_day = param.keyMap.getOrElse("spark.executor.memory.day", "4g").toString
    val spark_port_maxRetries_day = param.keyMap.getOrElse("spark.port.maxRetries.day", "500").toString
    val spark_default_parallelism_day = param.keyMap.getOrElse("spark.default.parallelism.day", "96").toString
    val spark_executor_cores_day = param.keyMap.getOrElse("spark.executor.cores.day", "4").toString

    val spark_executor_memory_result = param.keyMap.getOrElse("spark.executor.memory.result", "10g").toString
    val spark_port_maxRetries_result = param.keyMap.getOrElse("spark.port.maxRetries.result", "500").toString
    val spark_default_parallelism_result = param.keyMap.getOrElse("spark.default.parallelism.result", "800").toString
    val spark_executor_cores_result = param.keyMap.getOrElse("spark.executor.cores.result", "5").toString
    val prev_hour_nums_result = param.keyMap.getOrElse("prev.hour.nums.result", "1440").toString

    val secondsSeriesThresholdHour = param.keyMap.getOrElse("secondsSeriesThreshold.hour", "60").toString
    val timeSeriesThreshold = param.keyMap.getOrElse("timeSeriesThreshold", "180").toString

    val minImpactCountDay = param.keyMap.getOrElse("minImpactCountDay", "1").toString
    val minImpactCountResult = param.keyMap.getOrElse("minImpactCountResult", "3").toString
    val mf = param.keyMap.getOrElse("mf", "1.0").toString
    val secondsSeriesThreshold = param.keyMap.getOrElse("secondsSeriesThreshold", "60").toString
    // 人脸整合表(hive_face_detect_rl)数据是否实时落地,1：实时(默认) 2 离线
    val faceDetectRlRealTime = param.keyMap.getOrElse("faceDetectRlRealTime", "1").toString.toInt
    if (relType.contains("person") && faceDetectRlRealTime == 2){
      val days = prev_hour_nums_result.toInt / 24
      val (dateList, hourList) = Utils.getTimeList(batchId, days.toString)
      logger.info("#################分布统计############################")
      //人脸整合表数据存在延迟,分布统计, 计算最近15天的按小时分布统计
      logger.info(s"hourList: ${hourList.mkString(" ")}")

      val shellList = hourList.sortWith(_ > _).map(r => shell(r, secondsSeriesThresholdHour, timeSeriesThreshold, relType))
      val threadPool: ExecutorService = Executors.newFixedThreadPool(5)

      val latch = new CountDownLatch(shellList.size)

      try{
        shellList.foreach{shell =>
          threadPool.submit(new ThreadDistribution(shell, latch))
        }
        // 等待
        latch.await(4, TimeUnit.HOURS)
      }finally {
        threadPool.shutdown()
      }

      logger.info("#################lcss、dtw按天统计############################")
      val jsonSparkInit =
        s"""|$master#--#{"analyzeModel":{"inputs":[
            |
            |      {"cnName":"最小碰撞次数","enName":"minImpactCount","desc":"最小碰撞次数","inputVal":"$minImpactCountDay"},
            |      {"cnName":"","enName":"spark.executor.memory","desc":"","inputVal":"$spark_executor_memory_day"},
            |      {"cnName":"","enName":"spark.port.maxRetries","desc":"","inputVal":"$spark_port_maxRetries_day"},
            |      {"cnName":"","enName":"spark.default.parallelism","desc":"","inputVal":"$spark_default_parallelism_day"},
            |      {"cnName":"","enName":"spark.executor.cores","desc":"","inputVal":"$spark_executor_cores_day"},
            |      {"cnName":"入库分区数","enName":"numPartitions","desc":"入库分区数","inputVal":"100"},
            |      {"cnName":"关系类别","enName":"relType","desc":"关系类别,如：imei-imsi,imsi-imsi","inputVal":"$relType"},
            |      {"cnName":"算法类型","enName":"algorithmType","desc":"算法类型(可选一个或多个，多个用逗号分开),如：lcss,fptree,dtw,entropy","inputVal":"lcss"},
            |      {"cnName":"碰撞时间分片","enName":"secondsSeriesThreshold","desc":"碰撞时间分片，单位秒","inputVal":"$secondsSeriesThreshold"},
            |      {"cnName":"mf","enName":"mf","desc":"mf","inputVal":"$mf"},
            |      {"cnName":"数据库名称","enName":"dataBaseName","desc":"数据库名称","inputVal":"$dataBaseName"},
            |      {"cnName":"数据库类型","enName":"databaseType","desc":"数据库类型","inputVal":"$databaseType"},
            |      {"cnName":"结果落地MPP库","enName":"landDataBase","desc":"结果落地MPP库","inputVal":"${param.keyMap.getOrElse("landDataBase", "pd_dts").toString}"},
            |      {"cnName":"数据时间范围","enName":"prevHourNums","desc":"获取多长时间的数据，单位是小时","inputVal":"24"}],
            |      "isTag":1,"tagInfo":[],"mainClass":"com.suntek.algorithm.process.lcss.LCSSDayHandler",
            |      "modelId":8,"modelName":"LCSS","stepId":15,"taskId":12,"descInfo":""},
            |      "batchId":$batchId
            |  }
       """.
          stripMargin
      val sparkParam = new Param(jsonSparkInit)
      //人脸整合表数据存在延迟,按天统计, lccs、dtw - 计算最近15天的按天统计
      val spark = SparkUtil.getSparkSession(param.master, s"LCSSDayHandler-$relType-$batchId", sparkParam)
      logger.info(s"dateList: ${dateList.mkString(" ")}")
      dateList.foreach{batchId =>
        //人脸整合表数据存在延迟,按天统计, lccs - 计算最近15天的按天统计
        val jsonLcssDay =
          s"""|$master#--#{"analyzeModel":{"inputs":[
              |
              |      {"cnName":"最小碰撞次数","enName":"minImpactCount","desc":"最小碰撞次数","inputVal":"$minImpactCountDay"},
              |      {"cnName":"","enName":"spark.executor.memory","desc":"","inputVal":"$spark_executor_memory_day"},
              |      {"cnName":"","enName":"spark.port.maxRetries","desc":"","inputVal":"$spark_port_maxRetries_day"},
              |      {"cnName":"","enName":"spark.default.parallelism","desc":"","inputVal":"$spark_default_parallelism_day"},
              |      {"cnName":"","enName":"spark.executor.cores","desc":"","inputVal":"$spark_executor_cores_day"},
              |      {"cnName":"入库分区数","enName":"numPartitions","desc":"入库分区数","inputVal":"100"},
              |      {"cnName":"关系类别","enName":"relType","desc":"关系类别,如：imei-imsi,imsi-imsi","inputVal":"$relType"},
              |      {"cnName":"算法类型","enName":"algorithmType","desc":"算法类型(可选一个或多个，多个用逗号分开),如：lcss,fptree,dtw,entropy","inputVal":"lcss"},
              |      {"cnName":"碰撞时间分片","enName":"secondsSeriesThreshold","desc":"碰撞时间分片，单位秒","inputVal":"$secondsSeriesThreshold"},
              |      {"cnName":"mf","enName":"mf","desc":"mf","inputVal":"$mf"},
              |      {"cnName":"数据库名称","enName":"dataBaseName","desc":"数据库名称","inputVal":"$dataBaseName"},
              |      {"cnName":"数据库类型","enName":"databaseType","desc":"数据库类型","inputVal":"$databaseType"},
              |      {"cnName":"结果落地MPP库","enName":"landDataBase","desc":"结果落地MPP库","inputVal":"${param.keyMap.getOrElse("landDataBase", "pd_dts").toString}"},
              |      {"cnName":"数据时间范围","enName":"prevHourNums","desc":"获取多长时间的数据，单位是小时","inputVal":"24"}],
              |      "isTag":1,"tagInfo":[],"mainClass":"com.suntek.algorithm.process.lcss.LCSSDayHandler",
              |      "modelId":8,"modelName":"LCSS","stepId":15,"taskId":12,"descInfo":""},
              |      "batchId":$batchId
              |  }
       """.
            stripMargin
        logger.info(jsonLcssDay)
        val lcssDayParam = new Param(jsonLcssDay)
        LCSSDayHandler.processSub(lcssDayParam, spark)

        //人脸整合表数据存在延迟,按天统计, dtw - 计算最近15天的按天统计
        val jsonDtwDay =
          s"""$master#--#{"analyzeModel":{"inputs":[
             |      {"cnName":"","enName":"spark.executor.memory","desc":"","inputVal":"$spark_executor_memory_day"},
             |      {"cnName":"","enName":"spark.port.maxRetries","desc":"","inputVal":"$spark_port_maxRetries_day"},
             |      {"cnName":"","enName":"spark.default.parallelism","desc":"","inputVal":"$spark_default_parallelism_day"},
             |      {"cnName":"","enName":"spark.executor.cores","desc":"","inputVal":"$spark_executor_cores_day"},
             |      {"cnName":"最小碰撞次数","enName":"minImpactCount","desc":"最小碰撞次数","inputVal":"$minImpactCountDay"},
             |      {"cnName":"入库分区数","enName":"numPartitions","desc":"入库分区数","inputVal":"50"},
             |      {"cnName":"关系类别","enName":"relType","desc":"关系类别,如：imei-imsi,imsi-imsi","inputVal":"$relType"},
             |      {"cnName":"数据库名称","enName":"dataBaseName","desc":"数据库名称","inputVal":"$dataBaseName"},
             |      {"cnName":"数据库类型","enName":"databaseType","desc":"数据库类型","inputVal":"$databaseType"}],
             |      "isTag":1,"tagInfo":[],"mainClass":"com.suntek.algorithm.process.dtw.DtwDayHandler",
             |      "modelId":8,"modelName":"DTW","stepId":15,"taskId":12, "descInfo":""},
             |      "batchId":$batchId}
       """.stripMargin
        logger.info(jsonDtwDay)
        val dtwDayParam = new Param(jsonDtwDay)
        DtwDayHandler.processSub(dtwDayParam, spark)
      }
      spark.close()

      logger.info("#################dtw汇总计算############################")
      //人脸整合表数据存在延迟,dtw汇总计算 - 计算最近15天的按天统计
      val jsonDtwResult =
        s"""$master#--#{"analyzeModel":{"inputs":[
           |      {"cnName":"","enName":"spark.executor.memory","desc":"","inputVal":"$spark_executor_memory_result"},
           |      {"cnName":"","enName":"spark.port.maxRetries","desc":"","inputVal":"$spark_port_maxRetries_result"},
           |      {"cnName":"","enName":"spark.default.parallelism","desc":"","inputVal":"$spark_default_parallelism_result"},
           |      {"cnName":"","enName":"spark.executor.cores","desc":"","inputVal":"$spark_executor_cores_result"},
           |      {"cnName":"最小碰撞次数","enName":"minImpactCount","desc":"最小碰撞次数","inputVal":"$minImpactCountResult"},
           |      {"cnName":"累计计算天数","enName":"days","desc":"累计计算天数","inputVal":"$days"},
           |      {"cnName":"入库分区数","enName":"numPartitions","desc":"入库分区数","inputVal":"500"},
           |      {"cnName":"关系类别","enName":"relType","desc":"关系类别,如：imei-imsi,imsi-imsi","inputVal":"$relType"},
           |      {"cnName":"数据库名称","enName":"dataBaseName","desc":"数据库名称","inputVal":"$dataBaseName"},
           |      {"cnName":"数据库类型","enName":"databaseType","desc":"数据库类型","inputVal":"$databaseType"}],
           |      "isTag":1,"tagInfo":[],"mainClass":"com.suntek.algorithm.process.dtw.DtwResultHandler",
           |      "modelId":8,"modelName":"DTW汇总","stepId":15,"taskId":12,"descInfo":""},
           |      "batchId":$batchId}
       """.stripMargin
      logger.info(jsonDtwResult)
      val dtwResultParam = new Param(jsonDtwResult)
      DtwResultHandler.process(dtwResultParam)
    }
    else {
      val jsonLcssDay =
        s"""|$master#--#{"analyzeModel":{"inputs":[
            |      {"cnName":"最小碰撞次数","enName":"minImpactCount","desc":"最小碰撞次数","inputVal":"$minImpactCountDay"},
            |      {"cnName":"","enName":"spark.executor.memory","desc":"","inputVal":"$spark_executor_memory_day"},
            |      {"cnName":"","enName":"spark.port.maxRetries","desc":"","inputVal":"$spark_port_maxRetries_day"},
            |      {"cnName":"","enName":"spark.default.parallelism","desc":"","inputVal":"$spark_default_parallelism_day"},
            |      {"cnName":"","enName":"spark.executor.cores","desc":"","inputVal":"$spark_executor_cores_day"},
            |      {"cnName":"入库分区数","enName":"numPartitions","desc":"入库分区数","inputVal":"100"},
            |      {"cnName":"关系类别","enName":"relType","desc":"关系类别,如：imei-imsi,imsi-imsi","inputVal":"$relType"},
            |      {"cnName":"算法类型","enName":"algorithmType","desc":"算法类型(可选一个或多个，多个用逗号分开),如：lcss,fptree,dtw,entropy","inputVal":"lcss"},
            |      {"cnName":"碰撞时间分片","enName":"secondsSeriesThreshold","desc":"碰撞时间分片，单位秒","inputVal":"$secondsSeriesThreshold"},
            |      {"cnName":"mf","enName":"mf","desc":"mf","inputVal":"$mf"},
            |      {"cnName":"数据库名称","enName":"dataBaseName","desc":"数据库名称","inputVal":"$dataBaseName"},
            |      {"cnName":"数据库类型","enName":"databaseType","desc":"数据库类型","inputVal":"$databaseType"},
            |      {"cnName":"结果落地MPP库","enName":"landDataBase","desc":"结果落地MPP库","inputVal":"${param.keyMap.getOrElse("landDataBase", "pd_dts").toString}"},
            |      {"cnName":"数据时间范围","enName":"prevHourNums","desc":"获取多长时间的数据，单位是小时","inputVal":"24"}],
            |      "isTag":1,"tagInfo":[],"mainClass":"com.suntek.algorithm.process.lcss.LCSSDayHandler",
            |      "modelId":8,"modelName":"LCSS","stepId":15,"taskId":12,"descInfo":""},
            |      "batchId":$batchId

            |  }
       """.
          stripMargin

      val lcssDayParam = new Param(jsonLcssDay)
      LCSSDayHandler.process(lcssDayParam)
    }

    val jsonLcssResult =
      s"""|$master#--#{"analyzeModel":{"inputs":[
          |      {"cnName":"最小碰撞次数","enName":"minImpactCount","desc":"最小碰撞次数","inputVal":"$minImpactCountResult"},
          |      {"cnName":"","enName":"spark.executor.memory","desc":"","inputVal":"$spark_executor_memory_result"},
          |      {"cnName":"","enName":"spark.port.maxRetries","desc":"","inputVal":"$spark_port_maxRetries_result"},
          |      {"cnName":"","enName":"spark.default.parallelism","desc":"","inputVal":"$spark_default_parallelism_result"},
          |      {"cnName":"","enName":"spark.executor.cores","desc":"","inputVal":"$spark_executor_cores_result"},
          |      {"cnName":"入库分区数","enName":"numPartitions","desc":"入库分区数","inputVal":"800"},
          |      {"cnName":"关系类别","enName":"relType","desc":"关系类别,如：imei-imsi,imsi-imsi","inputVal":"$relType"},
          |      {"cnName":"算法类型","enName":"algorithmType","desc":"算法类型(可选一个或多个，多个用逗号分开),如：lcss,fptree,dtw,entropy","inputVal":"lcss"},
          |      {"cnName":"相似度计算结果的放大因子","enName":"mf","desc":"相似度计算结果的放大因子","inputVal":"$mf"},
          |      {"cnName":"碰撞时间分片","enName":"secondsSeriesThreshold","desc":"碰撞时间分片，单位秒","inputVal":"$secondsSeriesThreshold"},
          |      {"cnName":"数据库名称","enName":"dataBaseName","desc":"数据库名称","inputVal":"$dataBaseName"},
          |      {"cnName":" 数据库类型","enName":"databaseType","desc":"数据库类型","inputVal":"$databaseType"},
          |      {"cnName":"结果落地MPP库","enName":"landDataBase","desc":"结果落地MPP库","inputVal":"${param.keyMap.getOrElse("landDataBase", "pd_dts").toString}"},
          |      {"cnName":"数据时间范围","enName":"prevHourNums","desc":"获取多长时间的数据，单位是小时","inputVal":"$prev_hour_nums_result"}],
          |      "isTag":1,"tagInfo":[],"mainClass":"com.suntek.algorithm.process.lcss.LCSSResultHandler",
          |      "modelId":8,"modelName":"LCSS","stepId":15,"taskId":12,"descInfo":""},
          |      "batchId":$batchId
          |   }
       """.stripMargin
    val lcssResultParam = new Param(jsonLcssResult)
    LCSSResultHandler.process(lcssResultParam)
  }
}
