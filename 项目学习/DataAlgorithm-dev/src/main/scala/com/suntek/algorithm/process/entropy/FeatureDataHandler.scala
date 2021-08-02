package com.suntek.algorithm.process.entropy

import com.suntek.algorithm.common.conf.Param

/**
  * Created with IntelliJ IDEA.
  * Author: yuanweilin
  * Date: 2020-12-15 10:20
  * Description:特征数据合并处理
  * 1.特征数据日处理
  * 2.特征数据汇总处理
  */
object FeatureDataHandler {

  def process(param: Param): Unit = {
    val master = param.master
    val dataBaseName = param.keyMap.getOrElse("dataBaseName", "default").toString
    val databaseType = param.keyMap.getOrElse("databaseType", "hive").toString
    val relType = param.keyMap.getOrElse("relType","objecta-objectb").toString
    val batchId = param.batchId
    val featurePreprocKeyDay = param.keyMap.getOrElse("featurePreprocKeyDay","objecta-objectb.feature.perday.data.preprocess").toString
    val featurePreprocKeyAll = param.keyMap.getOrElse("featurePreprocKeyAll","objecta-objectb.feature.longdays.data.preprocess").toString
    val timeInterval = param.keyMap.getOrElse("timeInterval","30").toString

    val prevHoursNumDay  = param.keyMap.getOrElse("prevHoursNumDay","24").toString
    val prevHoursNumAll  = param.keyMap.getOrElse("prevHoursNumAll","720").toString

    val eventTimeFormat = param.keyMap.getOrElse("eventTimeFormat","yyMMddHHmmss").toString
    val eventDateFormat = param.keyMap.getOrElse("eventDateFormat","yyyyMMdd").toString

    //特征数据日处理
    val jsonFeatureDataDayProcess =
      s"""|${master}#--#{"analyzeModel":{"inputs":[
         |{"cnName":"入库分区数","enName":"numPartitions","desc":"入库分区数","inputVal":"100"},
         |{"cnName":"关系类别","enName":"relType","desc":"关系类别,如：imei-imsi,imsi-imsi","inputVal":"${relType}"},
         |{"cnName":"数据预处理sql","enName":"featurePreprocKey","desc":"model.sql中数据预处理配置的key","inputVal":"${featurePreprocKeyDay}"},
         |{"cnName":"时间段时间间隔","enName":"TIME_INTERVAL","desc":"时间段时间间隔，单位秒，单位秒","inputVal":"${timeInterval}"},
         |{"cnName":"数据时间范围","enName":"PREV_HOUR_NUMS","desc":"获取多长时间的数据，单位是小时","inputVal":"${prevHoursNumDay}"},
         |{"cnName":"发生时间格式","enName":"EVENT_TIME_FORMAT","desc":"发生时间格式，默认是yyyy-MM-dd HH:mm:ss","inputVal":"${eventTimeFormat}"},
         |{"cnName":"发生日期格式","enName":"EVENT_DATE_FORMAT","desc":"发生日期格式，默认是yyyy-MM-dd","inputVal":"${eventDateFormat}"},
         |{"cnName":"数据库名称","enName":"dataBaseName","desc":"数据库名称","inputVal":"${dataBaseName}"},
         |{"cnName":" 数据库类型","enName":"databaseType","desc":"数据库类型","inputVal":"${databaseType}"}],
         |"isTag":1,
         |"tagInfo":[],
         |"mainClass":"com.suntek.algorithm.process.entropy.FeatureDataPreprocess",
         |"modelId":8,
         |"modelName":"熵值法特征数据预处理",
         |"stepId":15,
         |"taskId":12,
         |"descInfo":""},
         |"batchId":${batchId}
          |}
       """.stripMargin

    val featureDataDayProcessParam = new Param(jsonFeatureDataDayProcess)
    FeatureDataPreprocess.process(featureDataDayProcessParam)


    //特征数据汇总处理
    val jsonFeatureDataProcess =
      s"""|${master}#--#{"analyzeModel":{"inputs":[
         |{"cnName":"入库分区数","enName":"numPartitions","desc":"入库分区数","inputVal":"500"},
         |{"cnName":"关系类别","enName":"relType","desc":"关系类别,如：imei-imsi,imsi-imsi","inputVal":"${relType}"},
         |{"cnName":"数据预处理sql","enName":"featurePreprocKey","desc":"model.sql中数据预处理配置的key","inputVal":"${featurePreprocKeyAll}"},
         |{"cnName":"时间段时间间隔","enName":"TIME_INTERVAL","desc":"时间时>间间隔，单位秒，单位秒","inputVal":"${timeInterval}"},
         |{"cnName":"数据时间范围","enName":"PREV_HOUR_NUMS","desc":"获取多长时间的数据，单位是小时","inputVal":"${prevHoursNumAll}"},
         |{"cnName":"发生时间格式","enName":"EVENT_TIME_FORMAT","desc":"发生时间格式，默认是yyyy-MM-dd HH:mm:ss","inputVal":"${eventTimeFormat}"},
         |{"cnName":"发生日期格式","enName":"EVENT_DATE_FORMAT","desc":"发生日期格式，默认是yyyy-MM-dd","value":"${eventDateFormat}"},
         |{"cnName":"数据库名称","enName":"dataBaseName","desc":"数据库名称","inputVal":"${dataBaseName}"},
         |{"cnName":" 数据库类型","enName":"databaseType","desc":"数据库类型","inputVal":"${databaseType}"}],
         |"isTag":1,
         |"tagInfo":[],
         |"mainClass":"com.suntek.algorithm.process.entropy.FeatureDataPreprocess",
         |"modelId":8,
         |"modelName":"熵值法特征数据预处理",
         |"stepId":15,
         |"taskId":12,
         |"descInfo":""},
          |"batchId":${batchId}
          }
       """.stripMargin

    val featureDataProcessParam = new Param(jsonFeatureDataProcess)
    FeatureDataPreprocess.process(featureDataProcessParam)

  }


}
