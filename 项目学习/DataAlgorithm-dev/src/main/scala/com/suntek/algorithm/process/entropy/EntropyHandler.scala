package com.suntek.algorithm.process.entropy

import com.suntek.algorithm.common.conf.Param
import com.suntek.algorithm.common.util.ConstantUtil
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created with IntelliJ IDEA.
  * Author: yuanweilin
  * Date: 2020-12-15 10:20
  * Description:依赖FeatureDataHandler
  * 3.计算每日权重
  * 4.计算一段时间的权重
  * 5.计算每日结果
  * 6.计算一段时间的结果
  * 7.计算最终结果
  */
object EntropyHandler {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)


  def process(param: Param): Unit = {

    // 检查该任务依赖的任务是否都完成
    val taskId = param.keyMap("taskId").toString.toInt
    val stepId = param.keyMap("stepId").toString.toInt
    val parentStepId = param.keyMap.getOrElse("parentStepId", "").toString.trim
    var flag = ConstantUtil.checkTask(param.batchId, parentStepId)
    val waitTimeOutSeconds = param.keyMap.getOrElse("waitTimeOut","21600").toString.toLong   //默认执行超时时间6个小时

    val startTimeMills = System.currentTimeMillis()
    while (flag){
      logger.info(s"该任务依赖的任务:${parentStepId} 还未完成继续等待")
      Thread.sleep(900 * 1000L)
      flag = ConstantUtil.checkTask(param.batchId, parentStepId)

      val loopTimeMills = System.currentTimeMillis()
      if(loopTimeMills - startTimeMills > waitTimeOutSeconds * 1000L){
        logger.warn(s"该任务依赖的任务:${parentStepId} 执行超过 ${waitTimeOutSeconds} s，此任务${taskId} ${stepId} 退出" )
        flag = false
        throw new Exception(s"该任务依赖的任务:${parentStepId} 执行超过 ${waitTimeOutSeconds} s，此任务${taskId} ${stepId} 退出" )
      }
    }

    val master = param.master
    val relType = param.keyMap("relType").toString

    val dataBaseName = param.keyMap.getOrElse("dataBaseName", "default").toString
    val databaseType = param.keyMap.getOrElse("databaseType", "hive").toString
    val batchId = param.batchId

    var featureSqlKey = ""
    if(relType.contains("car")){
      featureSqlKey = "objecta-objectb.perday.feature.data.sql"
    }else{
      featureSqlKey = "efence_a-efence_b.perday.feature.data.sql"
    }

    val prevHoursNum  = param.keyMap.getOrElse("prevHoursNum","24").toString

    val eventTimeFormat = param.keyMap.getOrElse("eventTimeFormat","yyMMddHHmmss").toString
    val eventDateFormat = param.keyMap.getOrElse("eventDateFormat","yyyyMMdd").toString

    val spark_executor_memory_weight = param.keyMap.getOrElse("spark.executor.memory.weight", "1g").toString
    val spark_port_maxRetries_weight = param.keyMap.getOrElse("spark.port.maxRetries.weight", "500").toString
    val spark_default_parallelism_weight = param.keyMap.getOrElse("spark.default.parallelism.weight", "24").toString
    val spark_executor_cores_weight = param.keyMap.getOrElse("spark.executor.cores.weight", "4").toString

    val spark_executor_memory_result = param.keyMap.getOrElse("spark.executor.memory.result", "4g").toString
    val spark_port_maxRetries_result = param.keyMap.getOrElse("spark.port.maxRetries.result", "500").toString
    val spark_default_parallelism_result = param.keyMap.getOrElse("spark.default.parallelism.result", "24").toString
    val spark_executor_cores_result = param.keyMap.getOrElse("spark.executor.cores.result", "4").toString

    //计算每天的权重
    val jsonComputeDayWeight =
      s"""$master#--#{"analyzeModel":{"inputs":[
         |{"cnName":"","enName":"spark.executor.memory","desc":"","inputVal":"$spark_executor_memory_weight"},
         |{"cnName":"","enName":"spark.port.maxRetries","desc":"","inputVal":"$spark_port_maxRetries_weight"},
         |{"cnName":"","enName":"spark.default.parallelism","desc":"","inputVal":"$spark_default_parallelism_weight"},
         |{"cnName":"","enName":"spark.executor.cores","desc":"","inputVal":"$spark_executor_cores_weight"},
         |{"cnName":"入库分区数","enName":"numPartitions","desc":"入库分区数","inputVal":"100"},
         |{"cnName":"关系类别","enName":"relType","desc":"关系类别,如：imei-imsi,imsi-imsi","inputVal":"$relType"},
         |{"cnName":"指标配置","enName":"indexs","desc":"指标相关配置，支持层级配置","inputVal":[{"name":"${relType}-perday","level":3,"featureSqlKey":"${featureSqlKey}",
         |"subIndex":[
         |{"name":"total","type":1,"isNormalize":true},
         |{"name":"avg_flow_t","type":1,"isNormalize":true}]}
         |]},
         |{"cnName":"数据时间范围","enName":"PREV_HOUR_NUMS","desc":"获取多长时间的数据，单位是小时","inputVal":"${prevHoursNum}"},
         |{"cnName":"发生时间格式","enName":"EVENT_TIME_FORMAT","desc":"发生时间格式，默认是yyyy-MM-dd HH:mm:ss","inputVal":"yyMMddHHmmss"},
         |{"cnName":"发生日期格式","enName":"EVENT_DATE_FORMAT","desc":"发生日期格式，默认是yyyy-MM-dd","inputVal":"yyyyMMdd"},
         |{"cnName":"数据库名称","enName":"dataBaseName","desc":"数据库名称","inputVal":"$dataBaseName"},
         |{"cnName":" 数据库类型","enName":"databaseType","desc":"数据库类型","inputVal":"$databaseType"}],
         |"isTag":1,
         |"tagInfo":[],
         |"mainClass":"com.suntek.algorithm.process.entropy.EntropyWeightHandler",
         |"modelId":8,"modelName":"熵值法特征数据预处理",
         |"stepId":${stepId},"taskId":${taskId},
         |"descInfo":""},
         |"batchId":${batchId}
          }
       """.stripMargin

    val computeDayWeightParam = new Param(jsonComputeDayWeight)
    EntropyWeightHandler.process(computeDayWeightParam)


    //计算一段时间的权重
    if(relType.contains("car")){
      featureSqlKey = "objecta-objectb.longdays.feature.data.sql"
    }else{
      featureSqlKey = "efence_a-efence_b.longdays.feature.data.sql"
    }

    val jsonComputeWeight =
      s"""$master#--#{"analyzeModel":{"inputs":[
         |{"cnName":"","enName":"spark.executor.memory","desc":"","inputVal":"$spark_executor_memory_weight"},
         |{"cnName":"","enName":"spark.port.maxRetries","desc":"","inputVal":"$spark_port_maxRetries_weight"},
         |{"cnName":"","enName":"spark.default.parallelism","desc":"","inputVal":"$spark_default_parallelism_weight"},
         |{"cnName":"","enName":"spark.executor.cores","desc":"","inputVal":"$spark_executor_cores_weight"},
         |{"cnName":"入库分区数","enName":"numPartitions","desc":"入库分区数","inputVal":"500"},
         |{"cnName":"关系类别","enName":"relType","desc":"关系类别,如：imei-imsi,imsi-imsi","inputVal":"$relType"},
         |{"cnName":"指标配置","enName":"indexs","desc":"指标相关配置，支持层级配置","inputVal":[{"name":"${relType}-longdays","level":3,"featureSqlKey":"${featureSqlKey}",
         |"subIndex":[
         |{"name":"days","type":1,"isNormalize":true},
         |{"name":"avg_flow_t","type":1,"isNormalize":true}]}
         |]},
         |{"cnName":"数据时间范围","enName":"PREV_HOUR_NUMS","desc":"获取多长时间的数据，单位是小时","inputVal":"${prevHoursNum}"},
         |{"cnName":"发生时间格式","enName":"EVENT_TIME_FORMAT","desc":"发生时间格式，默认是yyyy-MM-dd HH:mm:ss","inputVal":"${eventTimeFormat}"},
         |{"cnName":"发生日期格式","enName":"EVENT_DATE_FORMAT","desc":"发生日期格式，默认是yyyy-MM-dd","inputVal":"${eventDateFormat}"},
         |{"cnName":"数据库名称","enName":"dataBaseName","desc":"数据库名称","inputVal":"$dataBaseName"},
         |{"cnName":" 数据库类型","enName":"databaseType","desc":"数据库类型","inputVal":"$databaseType"}],
         |"isTag":1,
         |"tagInfo":[],
         |"mainClass":"com.suntek.algorithm.process.entropy.EntropyWeightHandler",
         |"modelId":8,"modelName":"熵值法特征数据预处理",
         |"stepId":${stepId},"taskId":${taskId},
         |"descInfo":""},
         |"batchId":${batchId}
          }
       """.stripMargin

    val computeWeightParam = new Param(jsonComputeWeight)
    EntropyWeightHandler.process(computeWeightParam)


    //计算每天的结果
    var loadDetailSqlKey = ""

    if(relType.contains("car")){
      loadDetailSqlKey = "objecta-objectb.perday.result.data.sql"
    }else{
      loadDetailSqlKey = "efence_a-efence_b.longdays.result.data.sql"
    }

    val jsonComputeDayResult =
      s"""$master#--#{"analyzeModel":{"inputs":[
         |{"cnName":"","enName":"spark.executor.memory","desc":"","inputVal":"$spark_executor_memory_weight"},
         |{"cnName":"","enName":"spark.port.maxRetries","desc":"","inputVal":"$spark_port_maxRetries_weight"},
         |{"cnName":"","enName":"spark.default.parallelism","desc":"","inputVal":"$spark_default_parallelism_weight"},
         |{"cnName":"","enName":"spark.executor.cores","desc":"","inputVal":"$spark_executor_cores_weight"},
         |{"cnName":"入库分区数","enName":"numPartitions","desc":"入库分区数","inputVal":"100"},
         |{"cnName":"关系类别","enName":"relType","desc":"关系类别,如：imei-imsi,imsi-imsi","inputVal":"$relType"},
         |{"cnName":"指标配置","enName":"indexs","desc":"指标相关配置，支持层级配置",
         |"inputVal":[
         |{"name":"${relType}-perday","level":3,"loadDetailSqlKey":"$loadDetailSqlKey",
         |"subIndex":[{"name":"total","type":1,"isNormalize":true},{"name":"avg_flow_t","type":1,"isNormalize":true}]}
         |]},
         |{"cnName":"数据时间范围","enName":"PREV_HOUR_NUMS","desc":"获取多长时间的数据，单位是小时","inputVal":"${prevHoursNum}"},
         |{"cnName":"发生时间格式","enName":"EVENT_TIME_FORMAT","desc":"发生时间格式，默认是yyyy-MM-dd HH:mm:ss","inputVal":"${eventTimeFormat}"},
         |{"cnName":"发生日期格式","enName":"EVENT_DATE_FORMAT","desc":"发生日期格式，默认是yyyy-MM-dd","inputVal":"${eventDateFormat}"},
         |{"cnName":"数据库名称","enName":"dataBaseName","desc":"数据库名称","inputVal":"$dataBaseName"},
         |{"cnName":" 数据库类型","enName":"databaseType","desc":"数据库类型","inputVal":"$databaseType"}],
         |"isTag":1,"tagInfo":[],
         |"mainClass":"com.suntek.algorithm.process.entropy.EntropyResultHandler",
         |"modelId":8,"modelName":"熵值法结果计算",
         |"stepId":${stepId},"taskId":${taskId},
         |"descInfo":""},
         |"batchId":${batchId}
          }
         |""".stripMargin

    val computeDayResultParam = new Param(jsonComputeDayResult)
    EntropyResultHandler.process(computeDayResultParam)

    //计算一段时间的结果
    if(relType.contains("car")){
      loadDetailSqlKey = "objecta-objectb.longdays.result.data.sql"
    }else{
      loadDetailSqlKey = "efence_a-efence_b.longdays.result.data.sql"
    }

    val jsonComputeAllResult =
      s"""$master#--#{"analyzeModel":{"inputs":[
         |{"cnName":"","enName":"spark.executor.memory","desc":"","inputVal":"$spark_executor_memory_result"},
         |{"cnName":"","enName":"spark.port.maxRetries","desc":"","inputVal":"$spark_port_maxRetries_result"},
         |{"cnName":"","enName":"spark.default.parallelism","desc":"","inputVal":"$spark_default_parallelism_result"},
         |{"cnName":"","enName":"spark.executor.cores","desc":"","inputVal":"$spark_executor_cores_result"},
         |{"cnName":"入库分区数","enName":"numPartitions","desc":"入库分区数","inputVal":"500"},
         |{"cnName":"关系类别","enName":"relType","desc":"关系类别,如：imei-imsi,imsi-imsi","inputVal":"$relType"},
         |{"cnName":"指标配置","enName":"indexs","desc":"指标相关配置，支持层级配置",
         |"inputVal":[
         |{"name":"${relType}-longdays","level":3,"loadDetailSqlKey":"$loadDetailSqlKey",
         |"subIndex":[{"name":"days","type":1,"isNormalize":true},{"name":"avg_flow_t","type":1,"isNormalize":true}]}
         |]},
         |{"cnName":"数据时间范围","enName":"PREV_HOUR_NUMS","desc":"获取多长时间的数据，单位是小时","inputVal":"${prevHoursNum}"},
         |{"cnName":"发生时间格式","enName":"EVENT_TIME_FORMAT","desc":"发生时间格式，默认是yyyy-MM-dd HH:mm:ss","inputVal":"${eventTimeFormat}"},
         |{"cnName":"发生日期格式","enName":"EVENT_DATE_FORMAT","desc":"发生日期格式，默认是yyyy-MM-dd","inputVal":"${eventDateFormat}"},
         |{"cnName":"数据库名称","enName":"dataBaseName","desc":"数据库名称","inputVal":"$dataBaseName"},
         |{"cnName":" 数据库类型","enName":"databaseType","desc":"数据库类型","inputVal":"$databaseType"}],
         |"isTag":1,"tagInfo":[],
         |"mainClass":"com.suntek.algorithm.process.entropy.EntropyResultHandler",
         |"modelId":8,"modelName":"熵值法结果计算",
         |"stepId":${stepId},"taskId":${taskId},
         |"descInfo":""},
         |"batchId":${batchId}
         |}
         |""".stripMargin

    val computeAllResultParam = new Param(jsonComputeAllResult)
    EntropyResultHandler.process(computeAllResultParam)


    //最终结果计算
    val jsonComputeFinalResult =
      s"""$master#--#{"analyzeModel":{"inputs":[
         |{"cnName":"","enName":"spark.executor.memory","desc":"","inputVal":"$spark_executor_memory_result"},
         |{"cnName":"","enName":"spark.port.maxRetries","desc":"","inputVal":"$spark_port_maxRetries_result"},
         |{"cnName":"","enName":"spark.default.parallelism","desc":"","inputVal":"$spark_default_parallelism_result"},
         |{"cnName":"","enName":"spark.executor.cores","desc":"","inputVal":"$spark_executor_cores_result"},
         |{"cnName":"入库分区数","enName":"numPartitions","desc":"入库分区数","inputVal":"100"},
         |{"cnName":"关系类别","enName":"relType","desc":"关系类别,如：imei-imsi,imsi-imsi","inputVal":"$relType"},
         |{"cnName":"指标配置","enName":"indexs","desc":"指标相关配置，支持层级配置",
         |"inputVal":[{"name":"${relType}-per-long-day","level":1,"loadDetailSqlKey":"${relType}.perlongday.result.data.sql","subIndex":[]
         |}]},
         |{"cnName":"数据时间范围","enName":"PREV_HOUR_NUMS","desc":"获取多长时间的数据，单位是小时","inputVal":"24"},
         |{"cnName":"发生时间格式","enName":"EVENT_TIME_FORMAT","desc":"发生时间格式，默认是yyyy-MM-dd HH:mm:ss","inputVal":"${eventTimeFormat}"},
         |{"cnName":"发生日期格式","enName":"EVENT_DATE_FORMAT","desc":"发生日期格式，默认是yyyy-MM-dd","inputVal":"${eventDateFormat}"},
         |{"cnName":"数据库名称","enName":"dataBaseName","desc":"数据库名称","inputVal":"$dataBaseName"},
         |{"cnName":" 数据库类型","enName":"databaseType","desc":"数据库类型","inputVal":"$databaseType"}],
         |"isTag":1,"tagInfo":[],
         |"mainClass":"com.suntek.algorithm.process.entropy.EntropyResultHandler",
         |"modelId":8,"modelName":"熵值法结果计算",
         |"stepId":${stepId},"taskId":${taskId},
         |"descInfo":""},
         |"batchId":${batchId}
         |}
         |
         |""".stripMargin

    val computeFinalResultParam = new Param(jsonComputeFinalResult)
    EntropyResultHandler.process(computeFinalResultParam)
  }


}
