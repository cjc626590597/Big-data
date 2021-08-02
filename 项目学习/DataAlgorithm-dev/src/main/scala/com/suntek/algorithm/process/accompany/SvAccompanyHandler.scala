package com.suntek.algorithm.process.accompany

import com.suntek.algorithm.common.conf.Param

/**
  * @author zhy
  * @date 2020-12-14 17:44
  */
//noinspection SpellCheckingInspection
object SvAccompanyHandler {
  def process(param: Param): Unit = {

    val master = param.master
    val relType = param.keyMap("relType").toString
    val taskId = param.keyMap("taskId").toString.toInt
    val stepId = param.keyMap("stepId").toString.toInt
    val dataBaseName = param.keyMap.getOrElse("dataBaseName", "default").toString
    val databaseType = param.keyMap.getOrElse("databaseType", "hive").toString
    val svType = param.keyMap.getOrElse("svType", "TSW").toString
    val seriesCount = param.keyMap.getOrElse("seriesCount", "60").toString
    val dataSource = param.keyMap.getOrElse("dataSource", "5").toString
    val secondsSeriesThreshold = param.keyMap.getOrElse("secondsSeriesThreshold", "60").toString
    val timeSeriesThreshold = param.keyMap.getOrElse("timeSeriesThreshold", "30").toString
    val prevHourNums = param.keyMap.getOrElse("prevHourNums", "24").toString
    val numPartitions = param.keyMap.getOrElse("numPartitions", "30").toString
    val maxAssembleTotal = param.keyMap.getOrElse("maxAssembleTotal", "30").toString
    val aggDays = param.keyMap.getOrElse("aggDays", "7").toString
    val minSupport = param.keyMap.getOrElse("minSupport", "0.1").toString
    val minCount = param.keyMap.getOrElse("minCount", "1").toString

    val spark_executor_memory_day = param.keyMap.getOrElse("spark.executor.memory.day", "3g").toString
    val spark_port_maxRetries_day = param.keyMap.getOrElse("spark.port.maxRetries.day", "500").toString
    val spark_default_parallelism_day = param.keyMap.getOrElse("spark.default.parallelism.day", "500").toString
    val spark_executor_cores_day = param.keyMap.getOrElse("spark.executor.cores.day", "3").toString

    val spark_executor_memory_result = param.keyMap.getOrElse("spark.executor.memory.result", "7g").toString
    val spark_port_maxRetries_result = param.keyMap.getOrElse("spark.port.maxRetries.result", "500").toString
    val spark_default_parallelism_result = param.keyMap.getOrElse("spark.default.parallelism.result", "500").toString
    val spark_executor_cores_result = param.keyMap.getOrElse("spark.executor.cores.result", "6").toString


    val batchId = param.batchId
    val jsonSvDay =
      s"""$master#--#{"analyzeModel":{"inputs":[
         |{"cnName":"","enName":"spark.executor.memory","desc":"","inputVal":"$spark_executor_memory_day"},
         |{"cnName":"","enName":"spark.port.maxRetries","desc":"","inputVal":"$spark_port_maxRetries_day"},
         |{"cnName":"","enName":"spark.default.parallelism","desc":"","inputVal":"$spark_default_parallelism_day"},
         |{"cnName":"","enName":"spark.executor.cores","desc":"","inputVal":"$spark_executor_cores_day"},
         |{"cnName":"关系类别","enName":"relType","desc":"关系类别,如：imei-imsi,imsi-imsi","inputVal":"$relType"},
         |{"cnName":"碰撞时间分片","enName":"secondsSeriesThreshold","desc":"碰撞时间分片，单位秒","inputVal":"$secondsSeriesThreshold"},
         |{"cnName":"时间压缩间隔","enName":"timeSeriesThreshold","desc":"碰撞时间分片，单位秒","inputVal":"$timeSeriesThreshold"},
         |{"cnName":"数据库名称","enName":"dataBaseName","desc":"数据库名称","inputVal":"$dataBaseName"},
         |{"cnName":"数据库类型","enName":"databaseType","desc":"数据库类型","inputVal":"$databaseType"},
         |{"cnName":"数据时间范围","enName":"prevHourNums","desc":"获取多长时间的数据，单位是小时","inputVal":"$prevHourNums"},
         |{"cnName":"数据来源","enName":"dataSource","desc":"数据来源","inputVal":"$dataSource"},
         |{"cnName":"分片对象阙值","enName":"seriesCount","desc":"分片对象阙值","inputVal":"$seriesCount"},
         |{"cnName":"伴随分析场所类型","enName":"svType","desc":"伴随分析场所类型(TSW TZ)","inputVal":"$svType"}],
         |"isTag":1,"tagInfo":[],"mainClass":"com.suntek.algorithm.process.accompany.SvAccompanyDayHandler",
         |"modelId":99300,"modelName":"${svType}场所按天分析","stepId": $stepId,"taskId": $taskId,"descInfo":""},
         |"batchId":$batchId}
       """.stripMargin.trim

    val svDayParam = new Param(jsonSvDay)
    SvAccompanyDayHandler.process(svDayParam)
    val jsonSvResult =
      s"""$master#--#{"analyzeModel":{"inputs":[
         |{"cnName":"","enName":"spark.executor.memory","desc":"","inputVal":"$spark_executor_memory_result"},
         |{"cnName":"","enName":"spark.port.maxRetries","desc":"","inputVal":"$spark_port_maxRetries_result"},
         |{"cnName":"","enName":"spark.default.parallelism","desc":"","inputVal":"$spark_default_parallelism_result"},
         |{"cnName":"","enName":"spark.executor.cores","desc":"","inputVal":"$spark_executor_cores_result"},
         |{"cnName":"保存数据分区数","enName":"numPartitions","desc":"保存数据分区数","inputVal":"$numPartitions"},
         |{"cnName":"同一分片内关联对象数","enName":"maxAssembleTotal","desc":"同一分片内关联对象数","inputVal":"$maxAssembleTotal"},
         |{"cnName":"周期天数","enName":"aggDays","desc":"周期天数","inputVal":"$aggDays"},
         |{"cnName":"关系类别","enName":"relType","desc":"关系类别,如：imei-imsi,imsi-imsi","inputVal":"$relType"},
         |{"cnName":"数据库名称","enName":"dataBaseName","desc":"数据库名称","inputVal":"$dataBaseName"},
         |{"cnName":"数据库类型","enName":"databaseType","desc":"数据库类型","inputVal":"$databaseType"},
         |{"cnName":"数据时间范围","enName":"prevHourNums","desc":"获取多长时间的数据，单位是小时","inputVal":"$prevHourNums"},
         |{"cnName":"数据来源","enName":"dataSource","desc":"数据来源","inputVal":"$dataSource"},
         |{"cnName":"伴随分析场所类型","enName":"svType","desc":"伴随分析场所类型","inputVal":"$svType"},
         |{"cnName":"最小支持度","enName":"minSupport","desc":"最小支持度","inputVal":"$minSupport"},
         |{"cnName":"最小支持次数","enName":"minCount","desc":"最小支持次数","inputVal":"$minCount"}],
         |"isTag":1,"tagInfo":[],"mainClass":"com.suntek.algorithm.process.accompany.SvAccompanyResultHandler",
         |"modelId":99300,"modelName":"${svType}场所汇总分析","stepId":$stepId,"taskId":$taskId,"descInfo":""},
         |"batchId":$batchId
         |}
       """.stripMargin.trim
    val svResultParam = new Param(jsonSvResult)
    SvAccompanyResultHandler.process(svResultParam)
  }
}
