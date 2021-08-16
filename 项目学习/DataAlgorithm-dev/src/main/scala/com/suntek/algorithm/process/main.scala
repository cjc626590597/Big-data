package com.suntek.algorithm.process
import com.suntek.algorithm.common.conf.{Constant, Param}
import com.suntek.algorithm.common.util.ConstantUtil
import com.suntek.algorithm.evaluation.AcompanyEvaluation
import com.suntek.algorithm.fusion._
import com.suntek.algorithm.process.accompany.{SvAccompanyDayHandler, SvAccompanyHandler, SvAccompanyResultHandler}
import com.suntek.algorithm.process.airport.CredentialsDetectHandler
import com.suntek.algorithm.process.device.DeviceRelations
import com.suntek.algorithm.process.distribution.Distribution
import com.suntek.algorithm.process.dtw.{DtwDayHandler, DtwHandler, DtwResultHandler}
import com.suntek.algorithm.process.entropy._
import com.suntek.algorithm.process.fptree.{FPTreeDayHandler, FPTreeHandler, FPTreeResultHandler}
import com.suntek.algorithm.process.lcss.{LCSSDayHandler, LCSSHandler, LCSSResultHandler}
import com.suntek.algorithm.process.lifecycle.HiveLifeCycleHandler
import com.suntek.algorithm.process.sql.{MppStatistics, MppStatisticsNew, SnowballStatistics, SqlAnalysisPreprocess, SynchronizeDataFromMPPToHive}
import org.slf4j.{Logger, LoggerFactory}
/**
  * @author zhy
  * @date 2020-9-24 15:39
  */
object main {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val args = new Array[String](1)
    //MppStatisticsNew
    //args(0) = new String("0#--#1#--#www.123.com#--#local#--#{\"analyzeModel\":{\"descInfo\":\"\",\"isLabel\":0,\"dataTransType\":0,\"mainClass\":\"com.suntek.algorithm.process.sql.MppStatistics\",\"modelId\":99410,\"inputs\":[{\"inputId\":0,\"cnName\":\"执行脚本\",\"enName\":\"shell_command\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"data-algorithm.sh\",\"inputType\":0,\"inputDesc\":\"执行脚本\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"部署环境变量\",\"enName\":\"deployEnvName\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"ALG_DEPLOY_HOME\",\"inputType\":0,\"inputDesc\":\"部署环境变量\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"日志路径名\",\"enName\":\"relType\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"track-count\",\"inputType\":0,\"inputDesc\":\"日志路径名\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"保存时间（天）\",\"enName\":\"retainDuration\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"90\",\"inputType\":0,\"inputDesc\":\"保存时\n间（天）\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"mpp库名\",\"enName\":\"landDataBase\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"pd_pqn\",\"inputType\":0,\"inputDesc\":\"mpp库名\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"延迟时间\",\"enName\":\"dalayTime\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"1800\",\"inputType\":0,\"inputDesc\":\"dalayTime（单位秒）\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"统计类型\",\"enName\":\"statisticsType\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"person:yyyyMMddHHmmss;car:yyyyMMddHHmmss;code:yyyyMMddHHmmss;statistics:yyyyMMddHHmmss\",\"inputType\":0,\"inputDesc\":\"统计类型（人：表时间类型)\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"nacos中轨迹统计的redis配置名称\",\"enName\":\"nacosRedisTrackCount\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"redis.nodes\",\"inputType\":0,\"inputDesc\":\"nacos中轨迹统计的redis配置名称\",\"inputCate\":2}],\"inputTable\":\"HIVE_FACE_DETECT_RL_DAS\",\"stepId\":100338,\"batchId\":0,\"uuid\":\"AC08B9D98\",\"cronExpr\":\"00/30***?\",\"exeCycle\":0,\"modelName\":\"人档轨迹统计\",\"executorName\":\"sparkSubmitJobHandler\",\"tblCNNames\":\"人脸整合数据分区表\",\"name\":\"人档轨迹统计\",\"tblENNames\":\"HIVE_FACE_DETECT_RL_DAS\",\"childStepIds\":\"\",\"from\":\"A96FDFABB\",\"streamAnalyse\":2,\"prevStepIds\":\"\",\"taskId\":100015},\"dataLanding\":[],\"transformer\":[],\"batchId\":\"20210805170000\",\"dataSource\":[{\"databaseName\":\"pd_dts\",\"datasetName\":\"HIVE_FACE_DETECT_RL_DAS\",\"uuid\":\"A96FDFABB\",\"url\":\"\",\"databaseType\":\"gaussdb\",\"datasetLabel\":\"人>脸整合数据分区表\",\"password\":\"\",\"isStreaming\":2,\"host\":\"\",\"datasetId\":\"592400852082902912\",\"from\":\"1\",\"to\":\"AC08B9D98\",\"username\":\"\"}],\"tagInfo\":[]}")
    //LCSS
//    args(0) = new String("100326#--#50649#--#http://172.25.20.62:8085/job#--#test#--#{\"analyzeModel\":{\"descInfo\":\"\",\"isLabel\":0,\"dataTransType\":0,\"mainClass\":\"com.suntek.algorithm.process.lcss.LCSSHandler\",\"modelId\":99100,\"inputs\":[{\"inputId\":0,\"cnName\":\"执行脚本\",\"enName\":\"shell_command\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"data-algorithm.sh\",\"inputType\":0,\"inputDesc\":\"执行脚本\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"部署环境变量\",\"enName\":\"deployEnvName\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"ALG_DEPLOY_HOME\",\"inputType\":0,\"inputDesc\":\"部署环境变量\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"sparkdriver内存设置\",\"enName\":\"spark.driver.memory\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"1g\",\"inputType\":0,\"inputDesc\":\"sparkdriver内存设置\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"关系类别\",\"enName\":\"relType\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"car-car\",\"inputType\":0,\"inputDesc\":\"关系类别,如：imei-imsi,imsi-imsi\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"单天执行任务的executor内存\",\"enName\":\"spark.executor.memory.day\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"4g\",\"inputType\":0,\"inputDesc\":\"单天执行>任务的executor内存\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"单天任务Driver启动端口尝试次数\",\"enName\":\"spark.port.maxRetries.day\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"500\",\"inputType\":0,\"inputDesc\":\"单天任务Driver启动端口尝试次数\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"单天任务并发数\",\"enName\":\"spark.default.parallelism.day\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"96\",\"inputType\":0,\"inputDesc\":\"单天>任务并发数\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"单天任务CPUS使用数\",\"enName\":\"spark.executor.cores.day\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"4\",\"inputType\":0,\"inputDesc\":\"单天任务CPUS使用数\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"汇总任务的executor内存\",\"enName\":\"spark.executor.memory.result\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"10g\",\"inputType\":0,\"inputDesc\":\"汇总任务的executor内存\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"汇总任务Driver启动端口尝试次数\",\"enName\":\"spark.port.maxRetries.result\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"500\",\"inputType\":0,\"inputDesc\":\"汇总任务Driver启动端口尝试>次数\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"汇总任务并发数\",\"enName\":\"spark.default.parallelism.result\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"800\",\"inputType\":0,\"inputDesc\":\"汇总任务并发数\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"汇总任务CPUS使用数\",\"enName\":\"spark.executor.cores.result\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"5\",\"inputType\":0,\"inputDesc\":\"汇总任务CPUS使用数\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"汇总任务累计计算小时数\",\"enName\":\"prev.hour.nums.result\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"360\",\"inputType\":0,\"inputDesc\":\"汇总任务累计计算小时数\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"单天结果最小碰撞次数\",\"enName\":\"minImpactCountDay\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"1\",\"inputType\":0,\"inputDesc\":\"单天结果最小碰撞次数，满足该值才可能存在关联关系\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"汇总结果最小碰撞次数\",\"enName\":\"minImpactCountResult\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"3\",\"inputType\":0,\"inputDesc\":\"汇总结果最小碰撞次数，满足该值才可能存在关联关系\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"相似度计算结果的放大因子\",\"enName\":\"mf\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"1.0\",\"inputType\":0,\"inputDesc\":\"相似度计算结果的放大因子，满足该值才可能存在关联关系\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"碰撞时间分片\",\"enName\":\"secondsSeriesThreshold\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"60\",\"inputType\":0,\"inputDesc\":\"碰撞时间分片，单位秒\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"结果落地MPP库\",\"enName\":\"landDataBase\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"pd_pqn\",\"inputType\":0,\"inputDesc\":\"结果落地MPP库\",\"inputCate\":2}],\"inputTable\":\"dm_relation_distribute_detail,dm_relation_distribute_stat,dm_id_distribute_day\",\"stepId\":100330,\"batchId\":0,\"uuid\":\"A9EECE92D\",\"cronExpr\":\"01003**?\",\"exeCycle\":0,\"modelName\":\"融合算法-LCSS\",\"executorName\":\"sparkSubmitJobHandler\",\"tblCNNames\":\"分布统计明细表,分布统计结果表,分布统计日表\",\"name\":\"融合算法-LCSS\",\"tblENNames\":\"dm_relation_distribute_detail,dm_relation_distribute_stat,dm_id_distribute_day\",\"childStepIds\":\"AFDA60DFD,A73E3FC35\",\"from\":\"A5FAF8195\",\"streamAnalyse\":2,\"prevStepIds\":\"\",\"taskId\":100031},\"dataLanding\":[],\"transformer\":[],\"batchId\":\"20210720031000\",\"dataSource\":[{\"databaseName\":\"default\",\"datasetName\":\"dm_relation_distribute_detail\",\"uuid\":\"A5FAF8195\",\"url\":\"jdbc:hive2://172.25.20.170:10000/default\",\"databaseType\":\"hive\",\"datasetLabel\":\"分布统计明细表\",\"password\":\"\",\"isStreaming\":2,\"host\":\"172.25.20.170:10000\",\"datasetId\":\"697459074546995200\",\"from\":\"1\",\"to\":\"A9EECE92D\",\"username\":\"root\"},{\"databaseName\":\"default\",\"datasetName\":\"dm_relation_distribute_stat\",\"uuid\":\"A5FAF8195\",\"url\":\"jdbc:hive2://172.25.20.170:10000/default\",\"databaseType\":\"hive\",\"datasetLabel\":\"分布统计结果表\",\"password\":\"\",\"isStreaming\":2,\"host\":\"172.25.20.170:10000\",\"datasetId\":\"697459257087299584\",\"from\":\"1\",\"to\":\"A9EECE92D\",\"username\":\"root\"},{\"databaseName\":\"default\",\"datasetName\":\"dm_id_distribute_day\",\"uuid\":\"A5FAF8195\",\"url\":\"jdbc:hive2://172.25.20.170:10000/default\",\"databaseType\":\"hive\",\"datasetLabel\":\"分>布统计日表\",\"password\":\"\",\"isStreaming\":2,\"host\":\"172.25.20.170:10000\",\"datasetId\":\"697459389186904064\",\"from\":\"1\",\"to\":\"A9EECE92D\",\"username\":\"root\"}],\"tagInfo\":[]}")
    args(0) = new String("599#--#5180#--#http://172.25.21.134:8085/job#--#test#--#{\"analyzeModel\":{\"descInfo\":\"\",\"isLabel\":0,\"dataTransType\":0,\"mainClass\":\"com.suntek.algorithm.process.dtw.DtwHandler\",\"modelId\":99800,\"inputs\":[{\"inputId\":0,\"cnName\":\"执行脚本\",\"enName\":\"shell_command\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"data-algorithm.sh\",\"inputType\":0,\"inputDesc\":\"执行脚本\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"部署环境变量\",\"enName\":\"deployEnvName\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"ALG_DEPLOY_HOME\",\"inputType\":0,\"inputDesc\":\"部署环境变量\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"sparkdriver内存设置\",\"enName\":\"spark.driver.memory\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"1g\",\"inputType\":0,\"inputDesc\":\"sparkdriver内存设置\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"关系类别\",\"enName\":\"relType\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"car-car\",\"inputType\":0,\"inputDesc\":\"关系类别,如：imei-imsi,imsi-imsi\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"单天执行任务的executor内存\",\"enName\":\"spark.executor.memory.day\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"6g\",\"inputType\":0,\"inputDesc\":\"单天执行任务的executor内存\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"单天任务Driver启动端口尝试次数\",\"enName\":\"spark.port.maxRetries.day\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"50\",\"inputType\":0,\"inputDesc\":\"单天任务Driver启动端口尝试次数\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"单天任务并发数\",\"enName\":\"spark.default.parallelism.day\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"576\",\"inputType\":0,\"inputDesc\":\"单天任务并发数\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"单天任务CPUS使用数\",\"enName\":\"spark.executor.cores.day\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"6\",\"inputType\":0,\"inputDesc\":\"单天任务CPUS使用数\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"汇总任务的executor内存\",\"enName\":\"spark.executor.memory.result\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"20g\",\"inputType\":0,\"inputDesc\":\"汇总任务的executor内存\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"汇总任务Driver启动端口尝试次数\",\"enName\":\"spark.port.maxRetries.result\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"50\",\"inputType\":0,\"inputDesc\":\"汇总任务Driver启动端口尝试次数\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"汇总任务并发数\",\"enName\":\"spark.default.parallelism.result\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"900\",\"inputType\":0,\"inputDesc\":\"汇总任务并发数\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"汇总任务CPUS使用数\",\"enName\":\"spark.executor.cores.result\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"10\",\"inputType\":0,\"inputDesc\":\"汇总任务CPUS使用数\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"单天结果最小碰撞次数\",\"enName\":\"minImpactCountDay\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"1\",\"inputType\":0,\"inputDesc\":\"单天结果最小碰撞次数，满足该值才可能存在关联关系\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"汇总结果最小碰撞次数\",\"enName\":\"minImpactCountResult\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"3\",\"inputType\":0,\"inputDesc\":\"汇总结果最小碰撞次数，满足该值才可能存在关联关系\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"累计计算天数\",\"enName\":\"days\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"15\",\"inputType\":0,\"inputDesc\":\"累计计算天数\",\"inputCate\":2}],\"inputTable\":\"dm_relation_distribute_detail,dm_relation_distribute_stat,dm_id_distribute_day\",\"stepId\":599,\"batchId\":0,\"uuid\":\"A7A9381F4\",\"cronExpr\":\"01502**?\",\"exeCycle\":0,\"modelName\":\"融合算法-DTW\",\"executorName\":\"sparkSubmitJobHandler\",\"tblCNNames\":\"分布统计明细表,分布统计结果表,分布统计日表\",\"name\":\"融合算法-DTW\",\"tblENNames\":\"dm_relation_distribute_detail,dm_relation_distribute_stat,dm_id_distribute_day\",\"childStepIds\":\"AC01B8DEF,AD9CB65E1\",\"from\":\"AB3533D16\",\"streamAnalyse\":2,\"prevStepIds\":\"\",\"taskId\":36},\"dataLanding\":[],\"transformer\":[],\"batchId\":\"20210707021500\",\"dataSource\":[{\"databaseName\":\"default\",\"datasetName\":\"dm_relation_distribute_detail\",\"uuid\":\"AB3533D16\",\"url\":\"jdbc:hive2://172.25.20.170:10000/default\",\"databaseType\":\"hive\",\"datasetLabel\":\"分布统计明细表\",\"password\":\"\",\"isStreaming\":2,\"host\":\"172.25.20.170:10000\",\"datasetId\":\"697459074546995200\",\"from\":\"1\",\"to\":\"A7A9381F4\",\"username\":\"root\"},{\"databaseName\":\"default\",\"datasetName\":\"dm_relation_distribute_stat\",\"uuid\":\"AB3533D16\",\"url\":\"jdbc:hive2://172.25.20.170:10000/default\",\"databaseType\":\"hive\",\"datasetLabel\":\"分布统计结果表\",\"password\":\"\",\"isStreaming\":2,\"host\":\"172.25.20.170:10000\",\"datasetId\":\"697459257087299584\",\"from\":\"1\",\"to\":\"A7A9381F4\",\"username\":\"root\"},{\"databaseName\":\"default\",\"datasetName\":\"dm_id_distribute_day\",\"uuid\":\"AB3533D16\",\"url\":\"jdbc:hive2://172.25.20.170:10000/default\",\"databaseType\":\"hive\",\"datasetLabel\":\"分布统计日表\",\"password\":\"\",\"isStreaming\":2,\"host\":\"172.25.20.170:10000\",\"datasetId\":\"697459389186904064\",\"from\":\"1\",\"to\":\"A7A9381F4\",\"username\":\"root\"}],\"tagInfo\":[]}")
    logger.info(s"args size={},{}", args.length, args(0))
    if (args == null || args.length < 1) throw new Exception("请求参数有误，请求参数依次为:jobid logid jobServiceUrl 任务json")

    var param: Param = null
    try {
      val argsArray = args(0).split(Constant.SEPARATOR)
      val jobId = argsArray(0) //作业ID
      val logId = argsArray(1) //执行ID
      val jobServiceUrl = argsArray(2) //任务状态更新接口
      val jsonStr = argsArray(3) + Constant.SEPARATOR + argsArray(4)  //任务执行json
      ConstantUtil.initTaskInfo(jobId, logId, jobServiceUrl)

//      val jsonStr =
//        """test#--#{"analyzeModel":{"descInfo":"","isLabel":0,"dataTransType":0,"mainClass":"com.suntek.algorithm.process.distribution.Distribution","modelId":99100,"inputs":[{"inputId":0,"cnName":"执行脚本","enName":"shell_command","stepId":0,"datasetId":0,"inputVal":"data-algorithm.sh","inputType":0,"inputDesc":"执行脚本","inputCate":2},{"inputId":0,"cnName":"部署环境变量","enName":"deployEnvName","stepId":0,"datasetId":0,"inputVal":"ALG_DEPLOY_HOME","inputType":0,"inputDesc":"部署环境变量","inputCate":2},{"inputId":0,"cnName":"sparkdriver内存设置","enName":"spark.driver.memory","stepId":0,"datasetId":0,"inputVal":"1g","inputType":0,"inputDesc":"sparkdriver内存设置","inputCate":2},{"inputId":0,"cnName":"关系类别","enName":"relType","stepId":0,"datasetId":0,"inputVal":"car-car","inputType":0,"inputDesc":"关系类别,如：imei-imsi,imsi-imsi","inputCate":2},{"inputId":0,"cnName":"单天执行任务的executor内存","enName":"spark.executor.memory.day","stepId":0,"datasetId":0,"inputVal":"4g","inputType":0,"inputDesc":"单天执>行任务的executor内存","inputCate":2},{"inputId":0,"cnName":"单天任务Driver启动端口尝试次数","enName":"spark.port.maxRetries.day","stepId":0,"datasetId":0,"inputVal":"50","inputType":0,"inputDesc":"单天任务Driver启动端口尝试次数","inputCate":2},{"inputId":0,"cnName":"单天任务并发数","enName":"spark.default.parallelism.day","stepId":0,"datasetId":0,"inputVal":"96","inputType":0,"inputDesc":"单天任务并发数","inputCate":2},{"inputId":0,"cnName":"单天任务CPUS使用数","enName":"spark.executor.cores.day","stepId":0,"datasetId":0,"inputVal":"4","inputType":0,"inputDesc":"单天任>务CPUS使用数","inputCate":2},{"inputId":0,"cnName":"汇总任务的executor内存","enName":"spark.executor.memory.result","stepId":0,"datasetId":0,"inputVal":"10g","inputType":0,"inputDesc":"汇总任务的executor内存","inputCate":2},{"inputId":0,"cnName":"汇总任务Driver启动端口尝试次数","enName":"spark.port.maxRetries.result","stepId":0,"datasetId":0,"inputVal":"50","inputType":0,"inputDesc":"汇总任务Driver启动端口尝试次数","inputCate":2},{"inputId":0,"cnName":"汇总任务并发数","enName":"spark.default.parallelism.result","stepId":0,"datasetId":0,"inputVal":"800","inputType":0,"inputDesc":"汇总任务并发数","inputCate":2},{"inputId":0,"cnName":"汇总任务CPUS使用数","enName":"spark.executor.cores.result","stepId":0,"datasetId":0,"inputVal":"5","inputType":0,"inputDesc":"汇总任务CPUS使用数","inputCate":2},{"inputId":0,"cnName":"汇总任务累计计算小时数","enName":"prev.hour.nums.result","stepId":0,"datasetId":0,"inputVal":"360","inputType":0,"inputDesc":"汇总任务累计计算小时数","inputCate":2},{"inputId":0,"cnName":"单天结果最小碰撞次数","enName":"minImpactCountDay","stepId":0,"datasetId":0,"inputVal":"1","inputType":0,"inputDesc":"单天结果最小碰撞次数，满足该值才可能存在关联关系","inputCate":1},{"inputId":0,"cnName":"汇总结果最小碰撞次数","enName":"minImpactCountResult","stepId":0,"datasetId":0,"inputVal":"1","inputType":0,"inputDesc":"汇总结果最小碰撞次数，满足该值才可能存在关联关系","inputCate":1},{"inputId":0,"cnName":"相似度计算结>果的放大因子","enName":"mf","stepId":0,"datasetId":0,"inputVal":"1.0","inputType":0,"inputDesc":"相似度计算结果的放大因子，满足该值才可能存在关>联关系","inputCate":2},{"inputId":0,"cnName":"碰撞时间分片","enName":"secondsSeriesThreshold","stepId":0,"datasetId":0,"inputVal":"60","inputType":0,"inputDesc":"碰撞时间分片，单位秒","inputCate":2}],"inputTable":"dm_relation_distribute_detail,dm_relation_distribute_stat,dm_id_distribute_day","stepId":176,"batchId":0,"uuid":"A9EECE92D","cronExpr":"0 15 3 bin conf config-template lib logs sh soft bin conf config-template lib logs sh soft ?","exeCycle":0,"modelName":"融合算法-LCSS","executorName":"sparkSubmitJobHandler","tblCNNames":"分布统计明细表,分布统计结果表,分布统计日表","name":"融合算法-LCSS","tblENNames":"dm_relation_distribute_detail,dm_relation_distribute_stat,dm_id_distribute_day","childStepIds":"AFDA60DFD,A73E3FC35","from":"A5FAF8195","streamAnalyse":2,"prevStepIds":"","taskId":22},"dataLanding":[],"transformer":[],"batchId":"20210519031500","dataSource":[{"databaseName":"default","datasetName":"dm_relation_distribute_detail","uuid":"A5FAF8195","url":"jdbc:hive2://172.25.21.2:10000/default","databaseType":"hive","datasetLabel":"分布统计明细表","password":"","isStreaming":2,"host":"172.25.21.2:10000","datasetId":"697459074546995200","from":"1","to":"A9EECE92D","username":"root"},{"databaseName":"default","datasetName":"dm_relation_distribute_stat","uuid":"A5FAF8195","url":"jdbc:hive2://172.25.21.2:10000/default","databaseType":"hive","datasetLabel":"分布统计结果表","password":"","isStreaming":2,"host":"172.25.21.2:10000","datasetId":"697459257087299584","from":"1","to":"A9EECE92D","username":"root"},{"databaseName":"default","datasetName":"dm_id_distribute_day","uuid":"A5FAF8195","url":"jdbc:hive2://172.25.21.2:10000/default","databaseType":"hive","datasetLabel":"分布统计日表","password":"","isStreaming":2,"host":"172.25.21.2:10000","datasetId":"697459389186904064","from":"1","to":"A9EECE92D","username":"root"}],"tagInfo":[]}
//          |""".stripMargin
//      System.setProperty("user.name", "hdfs")
//      val jsonStr =
//      """local#--#{"analyzeModel":{"descInfo":"","isLabel":0,"dataTransType":0,"mainClass":"com.suntek.algorithm.process.sql.MppStatistics","modelId":99410,"inputs":[{"inputId":0,"cnName":"执行脚本","enName":"shell_command","stepId":0,"datasetId":0,"inputVal":"data-algorithm.sh","inputType":0,"inputDesc":"执行脚本","inputCate":2},{"inputId":0,"cnName":"部署环境变量","enName":"deployEnvName","stepId":0,"datasetId":0,"inputVal":"ALG_DEPLOY_HOME","inputType":0,"inputDesc":"部署环境变量","inputCate":2},{"inputId":0,"cnName":"日志路径名","enName":"relType","stepId":0,"datasetId":0,"inputVal":"track-count","inputType":0,"inputDesc":"日志路径名","inputCate":2},{"inputId":0,"cnName":"保存时间（天）","enName":"retainDuration","stepId":0,"datasetId":0,"inputVal":"90","inputType":0,"inputDesc":"保存时间（天）","inputCate":2},{"inputId":0,"cnName":"mpp库名","enName":"landDataBase","stepId":0,"datasetId":0,"inputVal":"pd_pqn","inputType":0,"inputDesc":"mpp库名","inputCate":2},{"inputId":0,"cnName":"延迟时间","enName":"dalayTime","stepId":0,"datasetId":0,"inputVal":"1800","inputType":0,"inputDesc":"dalayTime（单位秒）","inputCate":2},{"inputId":0,"cnName":"统计类型","enName":"statisticsType","stepId":0,"datasetId":0,"inputVal":"person:yyyyMMddHHmmss;car:yyyyMMddHHmmss;code:yyyyMMddHHmmss;statistics:yyyyMMddHHmmss","inputType":0,"inputDesc":"统计类型（人：表时间类型)","inputCate":2},{"inputId":0,"cnName":"nacos中轨迹统计的redis配置名称","enName":"nacosRedisTrackCount","stepId":0,"datasetId":0,"inputVal":"redis.nodes","inputType":0,"inputDesc":"nacos中轨迹统计的redis配置名称","inputCate":2}],"inputTable":"HIVE_FACE_DETECT_RL_DAS","stepId":100338,"batchId":0,"uuid":"AC08B9D98","cronExpr":"00/30***?","exeCycle":0,"modelName":"人档轨迹统计","executorName":"sparkSubmitJobHandler","tblCNNames":"人脸整合数据分区表","name":"人档轨迹统计","tblENNames":"HIVE_FACE_DETECT_RL_DAS","childStepIds":"","from":"A96FDFABB","streamAnalyse":2,"prevStepIds":"","taskId":100015},"dataLanding":[],"transformer":[],"batchId":"20210805170000","dataSource":[{"databaseName":"pd_dts","datasetName":"HIVE_FACE_DETECT_RL_DAS","uuid":"A96FDFABB","url":"","databaseType":"gaussdb","datasetLabel":"人>脸整合数据分区表","password":"","isStreaming":2,"host":"","datasetId":"592400852082902912","from":"1","to":"AC08B9D98","username":""}],"tagInfo":[]}""".stripMargin
      param = new Param(jsonStr)

      val mainClass = param.keyMap("mainClass").toString
      logger.info(s"mainClass: $mainClass")
      ConstantUtil.init(param)
      mainClass match {
        // distribution
        case "com.suntek.algorithm.process.distribution.Distribution" => Distribution.process(param)

        // fptree
        case "com.suntek.algorithm.process.fptree.FPTreeDayHandler" => FPTreeDayHandler.process(param)
        case "com.suntek.algorithm.process.fptree.FPTreeResultHandler" => FPTreeResultHandler.process(param)
        case "com.suntek.algorithm.process.fptree.FPTreeHandler" => FPTreeHandler.process(param)

        // dtw
        case "com.suntek.algorithm.process.dtw.DtwDayHandler" => DtwDayHandler.process(param)
        case "com.suntek.algorithm.process.dtw.DtwResultHandler" => DtwResultHandler.process(param)
        case "com.suntek.algorithm.process.dtw.DtwHandler" => DtwHandler.process(param)
        // lcss
        case "com.suntek.algorithm.process.lcss.LCSSDayHandler" => LCSSDayHandler.process(param)
        case "com.suntek.algorithm.process.lcss.LCSSResultHandler" => LCSSResultHandler.process(param)
        case "com.suntek.algorithm.process.lcss.LCSSHandler" => LCSSHandler.process(param)

        // accompany - evaluation（伴随评价）
        case "com.suntek.algorithm.evaluation.AcompanyEvaluation" => AcompanyEvaluation.process(param)
        case "com.suntek.algorithm.fusion.Models" => Models.deal(param)
        case "com.suntek.algorithm.fusion.FusionHandler" => FusionHandler.process(param)
        case "com.suntek.algorithm.fusion.FusionRelationHiveToEs" => FusionRelationHiveToEs.process(param)


        // sv accompany（伴随）
        case "com.suntek.algorithm.process.accompany.SvAccompanyDayHandler" => SvAccompanyDayHandler.process(param)
        case "com.suntek.algorithm.process.accompany.SvAccompanyResultHandler" => SvAccompanyResultHandler.process(param)
        case "com.suntek.algorithm.process.accompany.SvAccompanyHandler" => SvAccompanyHandler.process(param)

        //熵值法
        case "com.suntek.algorithm.process.entropy.FeatureDataPreprocess" => FeatureDataPreprocess.process(param) //特征数据预处理
        case "com.suntek.algorithm.process.entropy.EntropyWeightHandler" => EntropyWeightHandler.process(param)
        case "com.suntek.algorithm.process.entropy.EntropyResultHandler" => EntropyResultHandler.process(param)
        case "com.suntek.algorithm.process.sql.SqlAnalysisPreprocess" => SqlAnalysisPreprocess.process(param)
        case "com.suntek.algorithm.process.entropy.FeatureDataHandler" => FeatureDataHandler.process(param)
        case "com.suntek.algorithm.process.entropy.EntropyHandler" => EntropyHandler.process(param)

        // 数据同步
        case "com.suntek.algorithm.process.sql.SynchronizeDataFromMPPToHive" =>SynchronizeDataFromMPPToHive.process(param)

        // 轨迹统计
        case  "com.suntek.algorithm.process.sql.MppStatistics" => {
          if(param.mppdbType == "snowballdb") {
            SnowballStatistics.process(param)
          }
          else {
            MppStatisticsNew.process(param)
          }
        }

          //一般规则统计
        case "com.suntek.algorithm.fusion.PersonFaceRelateStatHandler" => PersonFaceRelateStatHandler.process(param)
        case "com.suntek.algorithm.fusion.Car2CarSamePersonRelateHandler" => Car2CarSamePersonRelateHandler.process(param)
        case "com.suntek.algorithm.fusion.P2PSameCarRelateHandler" => P2PSameCarRelateHandler.process(param)
        //生命周期管理
        case "com.suntek.algorithm.process.lifecycle.HiveLifeCycleHandler" => HiveLifeCycleHandler.process(param)

          // 人脸Id与人员证件关联分析
        case "com.suntek.algorithm.fusion.FaceIdentificationHandler" => FaceIdentificationHandler.process(param)
        // 人员Id与车辆(主副驾驶)关联分析
        case "com.suntek.algorithm.fusion.PersonCarDriverHandler" => PersonCarDriverHandler.process(param)
        // 人脸Id与人员证件关联分析
        case "com.suntek.algorithm.fusion.PersonIdBodyHandler" => PersonIdBodyHandler.process(param)
        // 设备关联分析
        case "com.suntek.algorithm.process.device.DeviceRelations" => DeviceRelations.process(param)
        // 机场一人多证统计
        case "com.suntek.algorithm.process.airport.CredentialsDetectHandler" => CredentialsDetectHandler.process(param)

        case _ => throw new Exception("未能启动配置的任务,请确保启动类已存在.")
      }
      ConstantUtil.successTask
      ConstantUtil.success
    } catch {
      case ex: Exception =>
        logger.error("任务执行异常：" + ex.getMessage, ex)
        ex.printStackTrace()
        try {
          ConstantUtil.errorTask(ex.getMessage)
          ConstantUtil.error(ex.getMessage)
        } catch {
          case ex: Exception =>
            logger.error("任务状态更新异常：" + ex.getMessage, ex)
            ex.printStackTrace()
            throw new Exception(ex)
        }

      //        throw new Exception(ex)
    }

  }
}
