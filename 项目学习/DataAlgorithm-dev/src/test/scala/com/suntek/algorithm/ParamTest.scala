package com.suntek.algorithm

import com.suntek.algorithm.common.conf.Param
import com.suntek.algorithm.process.dtw.DtwHandler.logger
import org.junit.{Assert, Test}

/**
  * @author zhy
  * @date 2020-12-17 18:19
  */
class ParamTest extends Assert{
  @Test
  def analysisParamTest(): Unit ={
    val jsonDtwResult =
      s"""yran#--#{"batchId":"20201112000000","analyzeModel":{"env":{"shellName":"data-algorithm.sh","deployEnvName":"ALG_DEPLOY_HOME"},"params":[{"cnName":' '"算法名",' '"enName":' '"algorithm",' '"desc":' '"算法名",' '"value":' '"lcss"},{"cnName":' '"类型",' '"enName":' '"aType",' '"desc":' '"类型",' '"value":' '"car-imsi"},' '{"cnName":' '"数据库名称",' '"enName":' '"dataBaseName",' '"desc":' '"数据库名称",' '"value":' '"default"},' '{"cnName":' '"数据库类型",' '"enName":' '"databaseType",' '"desc":' '"数据库类型",' '"value":' '"hive"},{"cnName":' '"评估类型",' '"enName":' '"evaluationType",' '"desc":' '"评估类型",' '"value":' '"rank"},{"cnName":' '"statDate",' '"enName":' '"statDate",' '"desc":' '"statDate",' '"value":' '"20210110"},{"cnName":' '"posId",' '"enName":' '"posId",' '"desc":' '"posId",' '"value":' '"fpt_car_imsi_1112"}],"isTag":1,"tagInfo":[],"mainClass":"com.suntek.algorithm.evaluation.AcompanyEvaluation","modelId":0,"modelName":"模型评估","stepId":99401,"taskId":99401,"descInfo":""}}
       """.stripMargin
    logger.info(jsonDtwResult)
    val dtwResultParam = new Param(jsonDtwResult)
    println(dtwResultParam.batchId)
  }
}
