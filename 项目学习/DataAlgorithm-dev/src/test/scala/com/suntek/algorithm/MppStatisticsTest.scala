package com.suntek.algorithm;

import com.suntek.algorithm.common.bean.{DataBaseBean, QueryBean}
import com.suntek.algorithm.common.conf.{Constant, Param}
import com.suntek.algorithm.common.database.mpp.Mpp
import com.suntek.algorithm.common.util.SparkUtil
import com.suntek.algorithm.evaluation.AcompanyEvaluation
import com.suntek.algorithm.process.sql.{MppStatisticsNew, MppStatisticsParam}
import org.junit.Test

import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

 class MppStatisticsTest {



    @Test
    def testGetStartEndTime()={
        val batchID = "20210320000000"
        val json ="local#--#{\"analyzeModel\":{\"env\":{\"shellName\":\"data-algorithm.sh\",\"deployEnvName\":\"ALG_DEPLOY_HOME\"},\"params\":[{\"cnName\":\"数据库名称\",\"enName\":\"dataBaseName\",\"desc\":\"数据库名称\",\"value\":\"default\"},{\"cnName\":\"数据库类型\",\"enName\":\"databaseType\",\"desc\":\"数据库类型\",\"value\":\"mpp\"}  ,{\"cnName\":\"mpp库名\",\"enName\":\"landDataBase\",\"desc\":\"mpp库名\",\"value\":\"beehive\"} ,{\"cnName\":\"mppUrl\",\"enName\":\"url\",\"desc\":\"mppUrl\",\"value\":\"jdbc:postgresql://172.25.21.18:25308/beehive\"},{\"cnName\":\"mpp用户名\",\"enName\":\"username\",\"desc\":\"mpp用户名\",\"value\":\"suntek\"},{\"cnName\":\"mpp密码\",\"enName\":\"password\",\"desc\":\"mpp密码\",\"value\":\"suntek@123\"},{\"cnName\":\"延迟时间\",\"enName\":\"dalayTime\",\"desc\":\"延迟时间(秒)\",\"value\":\"300\"},{\"cnName\":\"统计类型\",\"enName\":\"statisticsType\",\"desc\":\"统计类型（人：表时间类型)\",\"value\":\"person:yyMMddHHmmss;car:yyMMddHHmmss;code:yyMMddHHmmss;statistics:yyyyMMddHHmmss\"}],\"isTag\":1,\"tagInfo\":[],\"mainClass\":\"com.suntek.algorithm.process.sql.MppStatistics\",\"modelId\":99410,\"modelName\":\"人档轨迹统计\",\"stepId\":99410,\"taskId\":99410,\"descInfo\":\"\"},\"batchId\":"+batchID+"}"
        val param: Param = new Param(json)
        val sParam = new MppStatisticsParam(param, "")
        val master = sParam.master
        val sparkSession = SparkUtil.getSparkSession(master, s"MPP Statistics  Job", param)

//        val mppStatic =  MppStatistics.getStartEndTime(sparkSession,sParam,null,null)
//       println(mppStatic._1 +" " +mppStatic._2)
    }

   @Test
   def testdeletePartition()={
     val batchID = "20210320000000"
     val json ="local#--#{\"analyzeModel\":{\"env\":{\"shellName\":\"data-algorithm.sh\",\"deployEnvName\":\"ALG_DEPLOY_HOME\"},\"params\":[{\"cnName\":\"数据库名称\",\"enName\":\"dataBaseName\",\"desc\":\"数据库名称\",\"value\":\"default\"},{\"cnName\":\"数据库类型\",\"enName\":\"databaseType\",\"desc\":\"数据库类型\",\"value\":\"mpp\"}  ,{\"cnName\":\"mpp库名\",\"enName\":\"landDataBase\",\"desc\":\"mpp库名\",\"value\":\"beehive\"} ,{\"cnName\":\"mppUrl\",\"enName\":\"url\",\"desc\":\"mppUrl\",\"value\":\"jdbc:postgresql://172.25.21.18:25308/beehive\"},{\"cnName\":\"mpp用户名\",\"enName\":\"username\",\"desc\":\"mpp用户名\",\"value\":\"suntek\"},{\"cnName\":\"mpp密码\",\"enName\":\"password\",\"desc\":\"mpp密码\",\"value\":\"suntek@123\"},{\"cnName\":\"延迟时间\",\"enName\":\"dalayTime\",\"desc\":\"延迟时间(秒)\",\"value\":\"300\"},{\"cnName\":\"统计类型\",\"enName\":\"statisticsType\",\"desc\":\"统计类型（人：表时间类型)\",\"value\":\"person:yyyyMMddHHmmss;car:yyyyMMddHHmmss;code:yyyyMMddHHmmss;statistics:yyyyMMddHHmmss\"}],\"isTag\":1,\"tagInfo\":[],\"mainClass\":\"com.suntek.algorithm.process.sql.MppStatistics\",\"modelId\":99410,\"modelName\":\"人档轨迹统计\",\"stepId\":99410,\"taskId\":99410,\"descInfo\":\"\"},\"batchId\":"+batchID+"}"
     val param: Param = new Param(json)
     val sParam = new MppStatisticsParam(param,batchID)
     val master = sParam.master
     val sparkSession = SparkUtil.getSparkSession(master, s"MPP Statistics  Job", param)
     val dataBaseBean = new DataBaseBean()
     dataBaseBean.setUsername(sParam.username) //suntek
     dataBaseBean.setPassword(sParam.password) //suntek@123
     dataBaseBean.setDriver(Constant.MPP_DRIVER)
     dataBaseBean.setUrl(sParam.url) //
     dataBaseBean.setNumPartitions(2)
     val mpp = new Mpp(sparkSession, dataBaseBean)
     val queryBean = new QueryBean()

//     MppStatistics.deletePartition(mpp,queryBean,"20210308164000",sParam,"tb_track_statistics",sParam.delayTime)
   }

   @Test
   def testProcess():Unit={
     val batchID = "20210320000000"
     val json ="local#--#{\"analyzeModel\":{\"env\":{\"shellName\":\"data-algorithm.sh\",\"deployEnvName\":\"ALG_DEPLOY_HOME\"},\"params\":[{\"cnName\":\"数据库名称\",\"enName\":\"dataBaseName\",\"desc\":\"数据库名称\",\"value\":\"default\"},{\"cnName\":\"数据库类型\",\"enName\":\"databaseType\",\"desc\":\"数据库类型\",\"value\":\"mpp\"}  ,{\"cnName\":\"mpp库名\",\"enName\":\"landDataBase\",\"desc\":\"mpp库名\",\"value\":\"beehive\"} ,{\"cnName\":\"mppUrl\",\"enName\":\"url\",\"desc\":\"mppUrl\",\"value\":\"jdbc:postgresql://172.25.21.18:25308/beehive\"},{\"cnName\":\"mpp用户名\",\"enName\":\"username\",\"desc\":\"mpp用户名\",\"value\":\"suntek\"},{\"cnName\":\"mpp密码\",\"enName\":\"password\",\"desc\":\"mpp密码\",\"value\":\"suntek@123\"},{\"cnName\":\"延迟时间\",\"enName\":\"dalayTime\",\"desc\":\"延迟时间(秒)\",\"value\":\"300\"},{\"cnName\":\"统计类型\",\"enName\":\"statisticsType\",\"desc\":\"统计类型（人：表时间类型)\",\"value\":\"person:yyyyMMddHHmmss;car:yyyyMMddHHmmss;code:yyyyMMddHHmmss;statistics:yyyyMMddHHmmss\"}],\"isTag\":1,\"tagInfo\":[],\"mainClass\":\"com.suntek.algorithm.process.sql.MppStatistics\",\"modelId\":99410,\"modelName\":\"人档轨迹统计\",\"stepId\":99410,\"taskId\":99410,\"descInfo\":\"\"},\"batchId\":"+batchID+"}"
     val param: Param = new Param(json)
     MppStatisticsNew.process(param)
   }

   @Test
   def testNeedUpdate()={
     val batchID = "20210320000000"
     val json ="local#--#{\"analyzeModel\":{\"env\":{\"shellName\":\"data-algorithm.sh\",\"deployEnvName\":\"ALG_DEPLOY_HOME\"},\"params\":[{\"cnName\":\"是否需要更新库存\",\"enName\":\"needUpdate\",\"desc\":\"是否需要更新库存\",\"value\":\"true\"},{\"cnName\":\"数据库名称\",\"enName\":\"dataBaseName\",\"desc\":\"数据库名称\",\"value\":\"default\"},{\"cnName\":\"数据库类型\",\"enName\":\"databaseType\",\"desc\":\"数据库类型\",\"value\":\"mpp\"}  ,{\"cnName\":\"mpp库名\",\"enName\":\"landDataBase\",\"desc\":\"mpp库名\",\"value\":\"beehive\"} ,{\"cnName\":\"mppUrl\",\"enName\":\"url\",\"desc\":\"mppUrl\",\"value\":\"jdbc:postgresql://172.25.21.18:25308/beehive\"},{\"cnName\":\"mpp用户名\",\"enName\":\"username\",\"desc\":\"mpp用户名\",\"value\":\"suntek\"},{\"cnName\":\"mpp密码\",\"enName\":\"password\",\"desc\":\"mpp密码\",\"value\":\"suntek@123\"},{\"cnName\":\"延迟时间\",\"enName\":\"dalayTime\",\"desc\":\"延迟时间(秒)\",\"value\":\"300\"},{\"cnName\":\"统计类型\",\"enName\":\"statisticsType\",\"desc\":\"统计类型（人：表时间类型)\",\"value\":\"person:yyyyMMddHHmmss;car:yyyyMMddHHmmss;code:yyyyMMddHHmmss;statistics:yyyyMMddHHmmss\"}],\"isTag\":1,\"tagInfo\":[],\"mainClass\":\"com.suntek.algorithm.process.sql.MppStatistics\",\"modelId\":99410,\"modelName\":\"人档轨迹统计\",\"stepId\":99410,\"taskId\":99410,\"descInfo\":\"\"},\"batchId\":"+batchID+"}"
     val param: Param = new Param(json)
     MppStatisticsNew.process(param)
   }

   @Test
   def testRedist()={
     val batchID = "20210320000000"
//     val json ="local#--#{\"analyzeModel\":{\"env\":{\"shellName\":\"data-algorithm.sh\",\"deployEnvName\":\"ALG_DEPLOY_HOME\"},\"params\":[{\"cnName\":\"数据库名称\",\"enName\":\"dataBaseName\",\"desc\":\"数据库名称\",\"value\":\"default\"},{\"cnName\":\"数据库类型\",\"enName\":\"databaseType\",\"desc\":\"数据库类型\",\"value\":\"mpp\"}  ,{\"cnName\":\"mpp库名\",\"enName\":\"landDataBase\",\"desc\":\"mpp库名\",\"value\":\"beehive\"} ,{\"cnName\":\"mppUrl\",\"enName\":\"url\",\"desc\":\"mppUrl\",\"value\":\"jdbc:postgresql://172.25.21.18:25308/beehive\"},{\"cnName\":\"mpp用户名\",\"enName\":\"username\",\"desc\":\"mpp用户名\",\"value\":\"suntek\"},{\"cnName\":\"mpp密码\",\"enName\":\"password\",\"desc\":\"mpp密码\",\"value\":\"suntek@123\"},{\"cnName\":\"延迟时间\",\"enName\":\"dalayTime\",\"desc\":\"延迟时间(秒)\",\"value\":\"300\"},{\"cnName\":\"统计类型\",\"enName\":\"statisticsType\",\"desc\":\"统计类型（人：表时间类型)\",\"value\":\"person:yyyyMMddHHmmss;car:yyyyMMddHHmmss;code:yyyyMMddHHmmss;statistics:yyyyMMddHHmmss\"}],\"isTag\":1,\"tagInfo\":[],\"mainClass\":\"com.suntek.algorithm.process.sql.MppStatistics\",\"modelId\":99410,\"modelName\":\"人档轨迹统计\",\"stepId\":99410,\"taskId\":99410,\"descInfo\":\"\"},\"batchId\":"+batchID+"}"
//     val json = "local#--#{\"analyzeModel\":{\"descInfo\":\"\",\"isLabel\":0,\"dataTransType\":0,\"mainClass\":\"com.suntek.algorithm.process.sql.MppStatistics\",\"modelId\":99410,\"inputs\":[{\"inputId\":0,\"cnName\":\"mpp库名\",\"enName\":\"landDataBase\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"pd_dts\",\"inputType\":0,\"inputDesc\":\"mpp库名\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"延迟时间\",\"enName\":\"dalayTime\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"300\",\"inputType\":0,\"inputDesc\":\"dalayTime（单位秒）\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"redis的host\",\"enName\":\"redisHost\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"172.25.21.104\",\"inputType\":0,\"inputDesc\":\"redis的host\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"redis的port\",\"enName\":\"redisPort\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"6379\",\"inputType\":0,\"inputDesc\":\"redis的port\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"统计类型\",\"enName\":\"statisticsType\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"person:yyyyMMddHHmmss;car:yyyyMMddHHmmss;code:yyyyMMddHHmmss;statistics:yyyyMMddHHmmss\",\"inputType\":0,\"inputDesc\":\"统计类型（人：表时间类型)\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"执行脚本\",\"enName\":\"shell_command\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"data-algorithm.sh\",\"inputType\":0,\"inputDesc\":\"执行脚本\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"部署环境>变量\",\"enName\":\"deployEnvName\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"ALG_DEPLOY_HOME\",\"inputType\":0,\"inputDesc\":\"部署环境变量\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"日志路径名\",\"enName\":\"relType\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"track-count\",\"inputType\":0,\"inputDesc\":\"日志路径名\",\"inputCate\":2}],\"inputTable\":\"hive_face_detect_rl,archive_code_detect_rl,archive_vehicle_detect_rl\",\"stepId\":137,\"batchId\":0,\"uuid\":\"A0FB86186\",\"cronExpr\":\"00/5***?\",\"exeCycle\":0,\"modelName\":\"人档轨迹统计\",\"executorName\":\"sparkSubmitJobHandler\",\"tblCNNames\":\"archive_vehicle_detect_rl,人脸整合表,archive_code_detect_rl\",\"name\":\"人档轨迹统计\n\",\"tblENNames\":\"archive_vehicle_detect_rl,hive_face_detect_rl,archive_code_detect_rl\",\"childStepIds\":\"\",\"from\":\"AB401B850\",\"streamAnalyse\":2,\"prevStepIds\":\"\",\"taskId\":89},\"dataLanding\":[],\"transformer\":[],\"batchId\":\"20210409172034\",\"dataSource\":[{\"databaseName\":\"pd_dts\",\"datasetName\":\"hive_face_detect_rl\",\"uuid\":\"AB401B850\",\"url\":\"jdbc:postgresql://172.25.21.18:25308/pd_dts?socket_timeout=3000000\",\"databaseType\":\"gaussdb\",\"datasetLabel\":\"人脸整合表\",\"password\":\"suntek@123\",\"isStreaming\":2,\"host\":\"172.25.21.18:25308\",\"datasetId\":\"697763614867187648\",\"from\":\"1\",\"to\":\"A0FB86186\",\"username\":\"suntek\"},{\"databaseName\":\"pd_dts\",\"datasetName\":\"archive_code_detect_rl\",\"uuid\":\"AB401B850\",\"url\":\"jdbc:postgresql://172.25.21.18:25308/pd_dts?socket_timeout=3000000\",\"databaseType\":\"gaussdb\",\"datasetLabel\":\"archive_code_detect_rl\",\"password\":\"suntek@123\",\"isStreaming\":2,\"host\":\"172.25.21.18:25308\",\"datasetId\":\"697766393216100288\",\"from\":\"1\",\"to\":\"A0FB86186\",\"username\":\"suntek\"},{\"databaseName\":\"pd_dts\",\"datasetName\":\"archive_vehicle_detect_rl\",\"uuid\":\"AB401B850\",\"url\":\"jdbc:postgresql://172.25.21.18:25308/pd_dts?socket_timeout=3000000\",\"databaseType\":\"gaussdb\",\"datasetLabel\":\"archive_vehicle_detect_rl\",\"password\":\"suntek@123\",\"isStreaming\":2,\"host\":\"172.25.21.18:25308\",\"datasetId\":\"697766479417436096\",\"from\":\"1\",\"to\":\"A0FB86186\",\"username\":\"suntek\"}],\"tagInfo\":[]}"
     val json ="local#--#{\"analyzeModel\":{\"env\":{\"shellName\":\"data-algorithm.sh\",\"deployEnvName\":\"ALG_DEPLOY_HOME\"},\"params\":[{\"cnName\":\"数据库名称\",\"enName\":\"dataBaseName\",\"desc\":\"数据库名称\",\"value\":\"default\"},{\"cnName\":\"数据库类型\",\"enName\":\"databaseType\",\"desc\":\"数据库类型\",\"value\":\"mpp\"}  ,{\"cnName\":\"mpp库名\",\"enName\":\"landDataBase\",\"desc\":\"mpp库名\",\"value\":\"beehive\"} ,{\"cnName\":\"mppUrl\",\"enName\":\"url\",\"desc\":\"mppUrl\",\"value\":\"jdbc:postgresql://172.25.21.18:25308/beehive\"},{\"cnName\":\"mpp用户名\",\"enName\":\"username\",\"desc\":\"mpp用户名\",\"value\":\"suntek\"},{\"cnName\":\"mpp密码\",\"enName\":\"password\",\"desc\":\"mpp密码\",\"value\":\"suntek@123\"},{\"cnName\":\"延迟时间\",\"enName\":\"dalayTime\",\"desc\":\"延迟时间(秒)\",\"value\":\"3600\"},{\"cnName\":\"保存时间（天）\",\"enName\":\"retainDuration\",\"desc\":\"保存时间（天）\",\"value\":\"90\"},{\"cnName\":\"统计类型\",\"enName\":\"statisticsType\",\"desc\":\"统计类型（人：表时间类型)\",\"value\":\"person:yyyyMMddHHmmss;car:yyyyMMddHHmmss;code:yyyyMMddHHmmss;statistics:yyyyMMddHHmmss\"}],\"isTag\":1,\"tagInfo\":[],\"mainClass\":\"com.suntek.algorithm.process.sql.MppStatistics\",\"modelId\":99410,\"modelName\":\"人档轨迹统计\",\"stepId\":99410,\"taskId\":99410,\"descInfo\":\"\"},\"batchId\":"+batchID+"}"
     println(json)
     val param: Param = new Param(json)
     val sParam = new MppStatisticsParam(param,batchID)
     val master = sParam.master
     val sparkSession = SparkUtil.getSparkSession(master, s"MPP Statistics  Job", param)
     val dataBaseBean = new DataBaseBean()
     dataBaseBean.setUsername(sParam.username) //suntek
     dataBaseBean.setPassword(sParam.password) //suntek@123
     dataBaseBean.setDriver(Constant.MPP_DRIVER)
     dataBaseBean.setUrl(sParam.url) //
     dataBaseBean.setNumPartitions(2)
     val mpp = new Mpp(sparkSession, dataBaseBean)
     val queryBean = new QueryBean()
     queryBean.isPage=false
     queryBean.batchSize=10000
//     (262 to 264).foreach(i=>{
//       val sql=s"insert into tb_track_statistics  select  CONCAT(id,'$i'),first_time ,first_device_id ,first_device_type ,last_time ,last_device_id ,last_device_type ,all_cnt ,cnt ,data_type ,20210310155500 as pt from tb_track_statistics where  pt = 20210310155500"
//       mpp.excute(sql,queryBean)
//     })

     //197276672
      MppStatisticsNew.process(param)
//     MppStatistics.writeToRedis(mpp,queryBean,"20210409200000",sParam)

   }


   @Test
   def test ()={
     val batchID = "20210320000000"
//          val json ="local#--#{\"analyzeModel\":{\"env\":{\"shellName\":\"data-algorithm.sh\",\"deployEnvName\":\"ALG_DEPLOY_HOME\"},\"params\":[{\"cnName\":\"数据库名称\",\"enName\":\"dataBaseName\",\"desc\":\"数据库名称\",\"value\":\"default\"},{\"cnName\":\"数据库类型\",\"enName\":\"databaseType\",\"desc\":\"数据库类型\",\"value\":\"mpp\"}  ,{\"cnName\":\"mpp库名\",\"enName\":\"landDataBase\",\"desc\":\"mpp库名\",\"value\":\"beehive\"} ,{\"cnName\":\"mppUrl\",\"enName\":\"url\",\"desc\":\"mppUrl\",\"value\":\"jdbc:postgresql://172.25.21.18:25308/beehive\"},{\"cnName\":\"mpp用户名\",\"enName\":\"username\",\"desc\":\"mpp用户名\",\"value\":\"suntek\"},{\"cnName\":\"mpp密码\",\"enName\":\"password\",\"desc\":\"mpp密码\",\"value\":\"suntek@123\"},{\"cnName\":\"延迟时间\",\"enName\":\"dalayTime\",\"desc\":\"延迟时间(秒)\",\"value\":\"300\"},{\"cnName\":\"保存时间（天）\",\"enName\":\"retainDuration\",\"desc\":\"保存时间（天）\",\"value\":\"90\"},{\"cnName\":\"统计类型\",\"enName\":\"statisticsType\",\"desc\":\"统计类型（人：表时间类型)\",\"value\":\"person:yyyyMMddHHmmss;car:yyyyMMddHHmmss;code:yyyyMMddHHmmss;statistics:yyyyMMddHHmmss\"}],\"isTag\":1,\"tagInfo\":[],\"mainClass\":\"com.suntek.algorithm.process.sql.MppStatistics\",\"modelId\":99410,\"modelName\":\"人档轨迹统计\",\"stepId\":99410,\"taskId\":99410,\"descInfo\":\"\"},\"batchId\":"+batchID+"}"
     val json = "local#--#{\"analyzeModel\":{\"descInfo\":\"\",\"isLabel\":0,\"dataTransType\":0,\"mainClass\":\"com.suntek.algorithm.process.sql.MppStatistics\",\"modelId\":99410,\"inputs\":[{\"inputId\":0,\"cnName\":\"mpp库名\",\"enName\":\"landDataBase\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"beehive\",\"inputType\":0,\"inputDesc\":\"mpp库名\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"debug\",\"enName\":\"debug\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"true\",\"inputType\":0,\"inputDesc\":\"debug\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"延迟时间\",\"enName\":\"dalayTime\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"1800\",\"inputType\":0,\"inputDesc\":\"dalayTime（单位秒）\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"redis的host\",\"enName\":\"redisHost\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"172.25.21.104\",\"inputType\":0,\"inputDesc\":\"redis的host\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"redis的port\",\"enName\":\"redisPort\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"6379\",\"inputType\":0,\"inputDesc\":\"redis的port\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"统计类型\",\"enName\":\"statisticsType\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"person:yyyyMMddHHmmss;car:yyyyMMddHHmmss;code:yyyyMMddHHmmss;statistics:yyyyMMddHHmmss\",\"inputType\":0,\"inputDesc\":\"统计类型（人：表时间类型)\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"执行脚本\",\"enName\":\"shell_command\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"data-algorithm.sh\",\"inputType\":0,\"inputDesc\":\"执行脚本\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"部署环境>变量\",\"enName\":\"deployEnvName\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"ALG_DEPLOY_HOME\",\"inputType\":0,\"inputDesc\":\"部署环境变量\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"日志路径名\",\"enName\":\"relType\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"track-count\",\"inputType\":0,\"inputDesc\":\"日志路径名\",\"inputCate\":2}],\"inputTable\":\"hive_face_detect_rl,archive_code_detect_rl,archive_vehicle_detect_rl\",\"stepId\":137,\"batchId\":0,\"uuid\":\"A0FB86186\",\"cronExpr\":\"00/5***?\",\"exeCycle\":0,\"modelName\":\"人档轨迹统计\",\"executorName\":\"sparkSubmitJobHandler\",\"tblCNNames\":\"archive_vehicle_detect_rl,人脸整合表,archive_code_detect_rl\",\"name\":\"人档轨迹统计\n\",\"tblENNames\":\"archive_vehicle_detect_rl,hive_face_detect_rl,archive_code_detect_rl\",\"childStepIds\":\"\",\"from\":\"AB401B850\",\"streamAnalyse\":2,\"prevStepIds\":\"\",\"taskId\":89},\"dataLanding\":[],\"transformer\":[],\"batchId\":\"20210409172034\",\"dataSource\":[{\"databaseName\":\"pd_dts\",\"datasetName\":\"hive_face_detect_rl\",\"uuid\":\"AB401B850\",\"url\":\"jdbc:postgresql://172.25.21.18:25308/pd_dts?socket_timeout=3000000\",\"databaseType\":\"gaussdb\",\"datasetLabel\":\"人脸整合表\",\"password\":\"suntek@123\",\"isStreaming\":2,\"host\":\"172.25.21.18:25308\",\"datasetId\":\"697763614867187648\",\"from\":\"1\",\"to\":\"A0FB86186\",\"username\":\"suntek\"},{\"databaseName\":\"pd_dts\",\"datasetName\":\"archive_code_detect_rl\",\"uuid\":\"AB401B850\",\"url\":\"jdbc:postgresql://172.25.21.18:25308/pd_dts?socket_timeout=3000000\",\"databaseType\":\"gaussdb\",\"datasetLabel\":\"archive_code_detect_rl\",\"password\":\"suntek@123\",\"isStreaming\":2,\"host\":\"172.25.21.18:25308\",\"datasetId\":\"697766393216100288\",\"from\":\"1\",\"to\":\"A0FB86186\",\"username\":\"suntek\"},{\"databaseName\":\"pd_dts\",\"datasetName\":\"archive_vehicle_detect_rl\",\"uuid\":\"AB401B850\",\"url\":\"jdbc:postgresql://172.25.21.18:25308/pd_dts?socket_timeout=3000000\",\"databaseType\":\"gaussdb\",\"datasetLabel\":\"archive_vehicle_detect_rl\",\"password\":\"suntek@123\",\"isStreaming\":2,\"host\":\"172.25.21.18:25308\",\"datasetId\":\"697766479417436096\",\"from\":\"1\",\"to\":\"A0FB86186\",\"username\":\"suntek\"}],\"tagInfo\":[]}"
     val param: Param = new Param(json)
//     val sparkSession = SparkUtil.getSparkSession("local", s"MPP Statistics  Job", param)
//     sparkSession.sql("show partitions dm_lcss").show()
//     MppStatistics.process(param)

     println(param.redisNode)
   }

   @Test
   def testEvaluation ()={
     val batchID = "20210320000000"
//          val json ="local#--#{\"analyzeModel\":{\"env\":{\"shellName\":\"data-algorithm.sh\",\"deployEnvName\":\"ALG_DEPLOY_HOME\"},\"params\":[{\"cnName\":\"数据库名称\",\"enName\":\"dataBaseName\",\"desc\":\"数据库名称\",\"value\":\"default\"},{\"cnName\":\"数据库类型\",\"enName\":\"databaseType\",\"desc\":\"数据库类型\",\"value\":\"mpp\"}  ,{\"cnName\":\"mpp库名\",\"enName\":\"landDataBase\",\"desc\":\"mpp库名\",\"value\":\"beehive\"} ,{\"cnName\":\"mppUrl\",\"enName\":\"url\",\"desc\":\"mppUrl\",\"value\":\"jdbc:postgresql://172.25.21.18:25308/beehive\"},{\"cnName\":\"mpp用户名\",\"enName\":\"username\",\"desc\":\"mpp用户名\",\"value\":\"suntek\"},{\"cnName\":\"mpp密码\",\"enName\":\"password\",\"desc\":\"mpp密码\",\"value\":\"suntek@123\"},{\"cnName\":\"延迟时间\",\"enName\":\"dalayTime\",\"desc\":\"延迟时间(秒)\",\"value\":\"300\"},{\"cnName\":\"统计类型\",\"enName\":\"statisticsType\",\"desc\":\"统计类型（人：表时间类型)\",\"value\":\"person:yyyyMMddHHmmss;car:yyyyMMddHHmmss;code:yyyyMMddHHmmss;statistics:yyyyMMddHHmmss\"}],\"isTag\":1,\"tagInfo\":[],\"mainClass\":\"com.suntek.algorithm.process.sql.MppStatistics\",\"modelId\":99410,\"modelName\":\"人档轨迹统计\",\"stepId\":99410,\"taskId\":99410,\"descInfo\":\"\"},\"batchId\":"+batchID+"}"
//     val json = "local#--#{\"batchId\":20210320000000,\"analyzeModel\":{\"env\":{\"shellName\":\"data-algorithm.sh\",\"deployEnvName\":\"ALG_DEPLOY_HOME\"},\"params\":[{\"cnName\": \"算法名\", \"enName\": \"algorithm\", \"desc\": \"算法名\", \"value\": \"LCSS\"},{\"cnName\": \"类型\", \"enName\": \"aType\", \"desc\": \"类型,字母必须大写\", \"value\": \"CAR-IMSI\"}, {\"cnName\": \"数据库名称\", \"enName\": \"dataBaseName\", \"desc\": \"数据库名称\", \"value\": \"default\"}, {\"cnName\": \"数据库类型\", \"enName\": \"databaseType\", \"desc\": \"数据库类型\", \"value\": \"hive\"},{\"cnName\": \"评估类型\", \"enName\": \"evaluationType\", \"desc\": \"评估类型\", \"value\": \"rank\"},{\"cnName\": \"statDate\", \"enName\": \"statDate\", \"desc\": \"statDate\", \"value\": \"20201112\"},{\"cnName\": \"posId\", \"enName\": \"posId\", \"desc\": \"posId\", \"value\": \"fpt_car_imsi_1112\"}],\"isTag\":1,\"tagInfo\":[],\"mainClass\":\"com.suntek.algorithm.evaluation.AcompanyEvaluation\",\"modelId\":0,\"modelName\":\"模型评估\",\"stepId\":99401,\"taskId\":99401,\"descInfo\":\"\"}}"
     val json = "local#--#{\"analyzeModel\":{\"descInfo\":\"\",\"isLabel\":0,\"dataTransType\":0,\"mainClass\":\"com.suntek.algorithm.evaluation.AcompanyEvaluation\",\"modelId\":99401,\"inputs\":[{\"inputId\":0,\"cnName\":\"评估类型\",\"enName\":\"evaluationType\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"rank\",\"inputType\":0,\"inputDesc\":\"评估类型\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"执行脚本\",\"enName\":\"shell_command\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"data-algorithm.sh\",\"inputType\":0,\"inputDesc\":\"执>行脚本\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"部署环境变量\",\"enName\":\"deployEnvName\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"ALG_DEPLOY_HOME\",\"inputType\":0,\"inputDesc\":\"部署环境变量\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"日志路径名\",\"enName\":\"relType\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"model-evaluate\",\"inputType\":0,\"inputDesc\":\"日志路径名\",\"inputCate\":2}],\"inputTable\":\"dm_lcss,dm_entropy_result,dm_dtw,dm_fptree,tb_acc_sample,dm_relation\",\"stepId\":244,\"batchId\":0,\"uuid\":\"A16711459\",\"cronExpr\":\"0311**?\",\"exeCycle\":0,\"modelName\":\"模型评估\",\"executorName\":\"sparkSubmitJobHandler\",\"tblCNNames\":\"dtw结果表,lcss结果表,fptree结\n果表,融合分析结果表,熵值法结果表,评估随机选择结果表\",\"name\":\"模型评估\",\"tblENNames\":\"dm_dtw,dm_lcss,dm_fptree,dm_relation,dm_entropy_result,tb_acc_sample\",\"childStepIds\":\"\",\"from\":\"A5DBB30E6\",\"streamAnalyse\":2,\"prevStepIds\":\"\",\"taskId\":127},\"dataLanding\":[],\"transformer\":[],\"batchId\":\"20210413170848\",\"dataSource\":[{\"databaseName\":\"default\",\"datasetName\":\"dm_lcss\",\"uuid\":\"A5DBB30E6\",\"url\":\"jdbc:hive2://172.25.20.170:10000/default\",\"databaseType\":\"hive\",\"datasetLabel\":\"lcss结果表\",\"password\":\"\",\"isStreaming\":2,\"host\":\"172.25.20.170:10000\",\"datasetId\":\"697460168727662592\",\"from\":\"1\",\"to\":\"A16711459\",\"username\":\"root\"},{\"databaseName\":\"default\",\"datasetName\":\"dm_entropy_result\",\"uuid\":\"A5DBB30E6\",\"url\":\"jdbc:hive2://172.25.20.170:10000/default\",\"databaseType\":\"hive\",\"datasetLabel\":\"熵值法结果表\",\"password\":\"\",\"isStreaming\":2,\"host\":\"172.25.20.170:10000\",\"datasetId\":\"697460523389620224\",\"from\":\"1\",\"to\":\"A16711459\",\"username\":\"root\"},{\"databaseName\":\"default\",\"datasetName\":\"dm_dtw\",\"uuid\":\"A5DBB30E6\",\"url\":\"jdbc:hive2://172.25.20.170:10000/default\",\"databaseType\":\"hive\",\"datasetLabel\":\"dtw结果表\",\"password\":\"\",\"isStreaming\":2,\"host\":\"172.25.20.170:10000\",\"datasetId\":\"697460782882820096\",\"from\":\"1\",\"to\":\"A16711459\",\"username\":\"root\"},{\"databaseName\":\"default\",\"datasetName\":\"dm_fptree\",\"uuid\":\"A5DBB30E6\",\"url\":\"jdbc:hive2://172.25.20.170:10000/default\",\"databaseType\":\"hive\",\"datasetLabel\":\"fptree结果表\",\"password\":\"\",\"isStreaming\":2,\"host\":\"172.25.20.170:10000\",\"datasetId\":\"697461258072297472\",\"from\":\"1\",\"to\":\"A16711459\",\"username\":\"root\"},{\"databaseName\":\"default\",\"datasetName\":\"tb_acc_sample\",\"uuid\":\"A5DBB30E6\",\"url\":\"jdbc:hive2://172.25.20.170:10000/default\",\"databaseType\":\"hive\",\"datasetLabel\":\"评估随机选择结果表\",\"password\":\"\",\"isStreaming\":2,\"host\":\"172.25.20.170:10000\",\"datasetId\":\"697462118634426368\",\"from\":\"1\",\"to\":\"A16711459\",\"username\":\"root\"},{\"databaseName\":\"default\",\"datasetName\":\"dm_relation\",\"uuid\":\"A5DBB30E6\",\"url\":\"jdbc:hive2://172.25.20.170:10000/default\",\"databaseType\":\"hive\",\"datasetLabel\":\"融合分析结果表\",\"password\":\"\",\"isStreaming\":2,\"host\":\"172.25.20.170:10000\",\"datasetId\":\"697461733320495104\",\"from\":\"1\",\"to\":\"A16711459\",\"username\":\"root\"}],\"tagInfo\":[]}"
     val param: Param = new Param(json)
     val sparkSession = SparkUtil.getSparkSession(param.master, s"MPP Statistics  Job", param)
     AcompanyEvaluation.process(param)


   }
//   @Test
//   def testEvaluation ()={
//     val batchID = "20210320000000"
//     //     val json ="local#--#{\"analyzeModel\":{\"env\":{\"shellName\":\"data-algorithm.sh\",\"deployEnvName\":\"ALG_DEPLOY_HOME\"},\"params\":[{\"cnName\":\"数据库名称\",\"enName\":\"dataBaseName\",\"desc\":\"数据库名称\",\"value\":\"default\"},{\"cnName\":\"数据库类型\",\"enName\":\"databaseType\",\"desc\":\"数据库类型\",\"value\":\"mpp\"}  ,{\"cnName\":\"mpp库名\",\"enName\":\"landDataBase\",\"desc\":\"mpp库名\",\"value\":\"beehive\"} ,{\"cnName\":\"mppUrl\",\"enName\":\"url\",\"desc\":\"mppUrl\",\"value\":\"jdbc:postgresql://172.25.21.18:25308/beehive\"},{\"cnName\":\"mpp用户名\",\"enName\":\"username\",\"desc\":\"mpp用户名\",\"value\":\"suntek\"},{\"cnName\":\"mpp密码\",\"enName\":\"password\",\"desc\":\"mpp密码\",\"value\":\"suntek@123\"},{\"cnName\":\"延迟时间\",\"enName\":\"dalayTime\",\"desc\":\"延迟时间(秒)\",\"value\":\"300\"},{\"cnName\":\"统计类型\",\"enName\":\"statisticsType\",\"desc\":\"统计类型（人：表时间类型)\",\"value\":\"person:yyyyMMddHHmmss;car:yyyyMMddHHmmss;code:yyyyMMddHHmmss;statistics:yyyyMMddHHmmss\"}],\"isTag\":1,\"tagInfo\":[],\"mainClass\":\"com.suntek.algorithm.process.sql.MppStatistics\",\"modelId\":99410,\"modelName\":\"人档轨迹统计\",\"stepId\":99410,\"taskId\":99410,\"descInfo\":\"\"},\"batchId\":"+batchID+"}"
//     val json = "local#--#{\"analyzeModel\":{\"descInfo\":\"\",\"isLabel\":0,\"dataTransType\":0,\"mainClass\":\"com.suntek.algorithm.evaluation.AcompanyEvaluation\",\"modelId\":99401,\"inputs\":[{\"inputId\":0,\"cnName\":\"评估类型\",\"enName\":\"evaluationType\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"rank\",\"inputType\":0,\"inputDesc\":\"评估类型\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"执行脚本\",\"enName\":\"shell_command\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"data-algorithm.sh\",\"inputType\":0,\"inputDesc\":\"执>行脚本\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"部署环境变量\",\"enName\":\"deployEnvName\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"ALG_DEPLOY_HOME\",\"inputType\":0,\"inputDesc\":\"部署环境变量\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"日志路径名\",\"enName\":\"relType\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"model-evaluate\",\"inputType\":0,\"inputDesc\":\"日志路径名\",\"inputCate\":2}],\"inputTable\":\"dm_lcss,dm_entropy_result,dm_dtw,dm_fptree,tb_acc_sample,dm_relation\",\"stepId\":244,\"batchId\":0,\"uuid\":\"A16711459\",\"cronExpr\":\"0311**?\",\"exeCycle\":0,\"modelName\":\"模型评估\",\"executorName\":\"sparkSubmitJobHandler\",\"tblCNNames\":\"dtw结果表,lcss结果表,fptree结\n果表,融合分析结果表,熵值法结果表,评估随机选择结果表\",\"name\":\"模型评估\",\"tblENNames\":\"dm_dtw,dm_lcss,dm_fptree,dm_relation,dm_entropy_result,tb_acc_sample\",\"childStepIds\":\"\",\"from\":\"A5DBB30E6\",\"streamAnalyse\":2,\"prevStepIds\":\"\",\"taskId\":127},\"dataLanding\":[],\"transformer\":[],\"batchId\":\"20210413170848\",\"dataSource\":[{\"databaseName\":\"default\",\"datasetName\":\"dm_lcss\",\"uuid\":\"A5DBB30E6\",\"url\":\"jdbc:hive2://172.25.20.170:10000/default\",\"databaseType\":\"hive\",\"datasetLabel\":\"lcss结果表\",\"password\":\"\",\"isStreaming\":2,\"host\":\"172.25.20.170:10000\",\"datasetId\":\"697460168727662592\",\"from\":\"1\",\"to\":\"A16711459\",\"username\":\"root\"},{\"databaseName\":\"default\",\"datasetName\":\"dm_entropy_result\",\"uuid\":\"A5DBB30E6\",\"url\":\"jdbc:hive2://172.25.20.170:10000/default\",\"databaseType\":\"hive\",\"datasetLabel\":\"熵值法结果表\",\"password\":\"\",\"isStreaming\":2,\"host\":\"172.25.20.170:10000\",\"datasetId\":\"697460523389620224\",\"from\":\"1\",\"to\":\"A16711459\",\"username\":\"root\"},{\"databaseName\":\"default\",\"datasetName\":\"dm_dtw\",\"uuid\":\"A5DBB30E6\",\"url\":\"jdbc:hive2://172.25.20.170:10000/default\",\"databaseType\":\"hive\",\"datasetLabel\":\"dtw结果表\",\"password\":\"\",\"isStreaming\":2,\"host\":\"172.25.20.170:10000\",\"datasetId\":\"697460782882820096\",\"from\":\"1\",\"to\":\"A16711459\",\"username\":\"root\"},{\"databaseName\":\"default\",\"datasetName\":\"dm_fptree\",\"uuid\":\"A5DBB30E6\",\"url\":\"jdbc:hive2://172.25.20.170:10000/default\",\"databaseType\":\"hive\",\"datasetLabel\":\"fptree结果表\",\"password\":\"\",\"isStreaming\":2,\"host\":\"172.25.20.170:10000\",\"datasetId\":\"697461258072297472\",\"from\":\"1\",\"to\":\"A16711459\",\"username\":\"root\"},{\"databaseName\":\"default\",\"datasetName\":\"tb_acc_sample\",\"uuid\":\"A5DBB30E6\",\"url\":\"jdbc:hive2://172.25.20.170:10000/default\",\"databaseType\":\"hive\",\"datasetLabel\":\"评估随机选择结果表\",\"password\":\"\",\"isStreaming\":2,\"host\":\"172.25.20.170:10000\",\"datasetId\":\"697462118634426368\",\"from\":\"1\",\"to\":\"A16711459\",\"username\":\"root\"},{\"databaseName\":\"default\",\"datasetName\":\"dm_relation\",\"uuid\":\"A5DBB30E6\",\"url\":\"jdbc:hive2://172.25.20.170:10000/default\",\"databaseType\":\"hive\",\"datasetLabel\":\"融合分析结果表\",\"password\":\"\",\"isStreaming\":2,\"host\":\"172.25.20.170:10000\",\"datasetId\":\"697461733320495104\",\"from\":\"1\",\"to\":\"A16711459\",\"username\":\"root\"}],\"tagInfo\":[]}"
//     val param: Param = new Param(json)
//     val sparkSession = SparkUtil.getSparkSession(param.master, s"MPP Statistics  Job", param)
//     AcompanyEvaluation.process(param)
//
//
//   }

   @Test
   def pln() = {
     val source = Source.fromFile("E:\\code\\DataAlgorithm\\src\\main\\resources\\conf\\snowballStatistics\\init.sql")
     val execSqls: Array[String] = source.getLines().mkString(" ")
       .split(";").map(sql => sql.trim + ";")
       .filter(x => x.nonEmpty && !x.startsWith("--"))


     (0 until execSqls.length - 1).foreach(i => println(execSqls(i)))


     source.close()

     execSqls.foreach(println(_))
   }

   @Test
   def mytest() = {
     val batchIdDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
     /*val startDate = batchIdDateFormat.parse("20210405020312")

     val calendar = Calendar.getInstance()
     calendar.setTime(startDate)
     calendar.set(Calendar.HOUR, 0)
     calendar.set(Calendar.MINUTE, 0)
     calendar.set(Calendar.SECOND, 0)
     println(batchIdDateFormat.format(calendar.getTime))

     println(batchIdDateFormat.format(calendar.getTime.getTime + 1800L * 1000L))*/

     val start = "20210624000000"
     val end = "20210624003000"

     val startDate = batchIdDateFormat.parse(start)
     val endDate = batchIdDateFormat.parse(end)
     println(endDate.getTime)
     println(startDate.getTime)
     println(endDate.getTime - startDate.getTime)
     println(endDate.getTime - startDate.getTime > 24 * 3600 * 1000L)


     val arrayBuffer = ArrayBuffer[(String, Int)]()

     val calendar = Calendar.getInstance()
     calendar.setTime(startDate)
     var continue = true
     var isZeroFlag = false
     if(start.endsWith("000000")) {
       if(!continue) {
         calendar.add(Calendar.SECOND, 24 * 3600)
       }
       isZeroFlag = true
     }
     else {
       if(!continue) {
         calendar.add(Calendar.SECOND, 1800)
       }
     }

     var lastTime = startDate.getTime

     while (calendar.getTime.getTime <= endDate.getTime) {
       if(isZeroFlag) {
         // 判断是否大于1天
         if (isZeroFlag && endDate.getTime - lastTime > 1 * 24 * 3600 * 1000L) {
           arrayBuffer.append((batchIdDateFormat.format(calendar.getTime), 1))
           calendar.add(Calendar.SECOND, 24 * 3600)
           isZeroFlag = true
         }
         else {
           if(arrayBuffer.nonEmpty) {
             val lastTuple = arrayBuffer(arrayBuffer.size - 1)
             val date = batchIdDateFormat.parse(lastTuple._1)
             if(calendar.getTime.getTime() - date.getTime() >= (24*3600 * 1000L)) {
               arrayBuffer.append((batchIdDateFormat.format(calendar.getTime), 2))
             }
             else {
               arrayBuffer.append((batchIdDateFormat.format(calendar.getTime), 0))
             }
           }
           else {
             arrayBuffer.append((batchIdDateFormat.format(calendar.getTime), 0))
           }

           calendar.add(Calendar.SECOND, 1800)
           isZeroFlag = false
         }
       }
       else {
         if(calendar.get(Calendar.HOUR_OF_DAY) == 0 &&
           calendar.get(Calendar.MINUTE) == 0 &&
           calendar.get(Calendar.MINUTE) == 0) {
           arrayBuffer.append((batchIdDateFormat.format(calendar.getTime), 1))
           calendar.add(Calendar.SECOND, 24 * 3600)
           isZeroFlag = true
         }
         else {
           arrayBuffer.append((batchIdDateFormat.format(calendar.getTime), 0))
           calendar.add(Calendar.SECOND, 1800)
           isZeroFlag = false
         }
       }
       lastTime = calendar.getTime.getTime
     }

     arrayBuffer.foreach(println(_))

     /*val cacheTableArr = ArrayBuffer("tb_track_statistics")
     cacheTableArr.append("tb_track_statistics_hour")
     cacheTableArr.foreach(println(_))

     val json = "local#--#{\"analyzeModel\":{\"descInfo\":\"\",\"isLabel\":0,\"dataTransType\":0,\"mainClass\":\"com.suntek.algorithm.process.sql.MppStatistics\",\"modelId\":99410,\"inputs\":[{\"inputId\":0,\"cnName\":\"mpp库名\",\"enName\":\"landDataBase\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"beehive\",\"inputType\":0,\"inputDesc\":\"mpp库名\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"debug\",\"enName\":\"debug\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"true\",\"inputType\":0,\"inputDesc\":\"debug\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"延迟时间\",\"enName\":\"dalayTime\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"1800\",\"inputType\":0,\"inputDesc\":\"dalayTime（单位秒）\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"redis的host\",\"enName\":\"redisHost\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"172.25.21.104\",\"inputType\":0,\"inputDesc\":\"redis的host\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"redis的port\",\"enName\":\"redisPort\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"6379\",\"inputType\":0,\"inputDesc\":\"redis的port\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"统计类型\",\"enName\":\"statisticsType\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"person:yyyyMMddHHmmss;car:yyyyMMddHHmmss;code:yyyyMMddHHmmss;statistics:yyyyMMddHHmmss\",\"inputType\":0,\"inputDesc\":\"统计类型（人：表时间类型)\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"执行脚本\",\"enName\":\"shell_command\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"data-algorithm.sh\",\"inputType\":0,\"inputDesc\":\"执行脚本\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"部署环境>变量\",\"enName\":\"deployEnvName\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"ALG_DEPLOY_HOME\",\"inputType\":0,\"inputDesc\":\"部署环境变量\",\"inputCate\":2},{\"inputId\":0,\"cnName\":\"日志路径名\",\"enName\":\"relType\",\"stepId\":0,\"datasetId\":0,\"inputVal\":\"track-count\",\"inputType\":0,\"inputDesc\":\"日志路径名\",\"inputCate\":2}],\"inputTable\":\"hive_face_detect_rl,archive_code_detect_rl,archive_vehicle_detect_rl\",\"stepId\":137,\"batchId\":0,\"uuid\":\"A0FB86186\",\"cronExpr\":\"00/5***?\",\"exeCycle\":0,\"modelName\":\"人档轨迹统计\",\"executorName\":\"sparkSubmitJobHandler\",\"tblCNNames\":\"archive_vehicle_detect_rl,人脸整合表,archive_code_detect_rl\",\"name\":\"人档轨迹统计\n\",\"tblENNames\":\"archive_vehicle_detect_rl,hive_face_detect_rl,archive_code_detect_rl\",\"childStepIds\":\"\",\"from\":\"AB401B850\",\"streamAnalyse\":2,\"prevStepIds\":\"\",\"taskId\":89},\"dataLanding\":[],\"transformer\":[],\"batchId\":\"20210409172034\",\"dataSource\":[{\"databaseName\":\"pd_dts\",\"datasetName\":\"hive_face_detect_rl\",\"uuid\":\"AB401B850\",\"url\":\"jdbc:postgresql://172.25.21.18:25308/pd_dts?socket_timeout=3000000\",\"databaseType\":\"gaussdb\",\"datasetLabel\":\"人脸整合表\",\"password\":\"suntek@123\",\"isStreaming\":2,\"host\":\"172.25.21.18:25308\",\"datasetId\":\"697763614867187648\",\"from\":\"1\",\"to\":\"A0FB86186\",\"username\":\"suntek\"},{\"databaseName\":\"pd_dts\",\"datasetName\":\"archive_code_detect_rl\",\"uuid\":\"AB401B850\",\"url\":\"jdbc:postgresql://172.25.21.18:25308/pd_dts?socket_timeout=3000000\",\"databaseType\":\"gaussdb\",\"datasetLabel\":\"archive_code_detect_rl\",\"password\":\"suntek@123\",\"isStreaming\":2,\"host\":\"172.25.21.18:25308\",\"datasetId\":\"697766393216100288\",\"from\":\"1\",\"to\":\"A0FB86186\",\"username\":\"suntek\"},{\"databaseName\":\"pd_dts\",\"datasetName\":\"archive_vehicle_detect_rl\",\"uuid\":\"AB401B850\",\"url\":\"jdbc:postgresql://172.25.21.18:25308/pd_dts?socket_timeout=3000000\",\"databaseType\":\"gaussdb\",\"datasetLabel\":\"archive_vehicle_detect_rl\",\"password\":\"suntek@123\",\"isStreaming\":2,\"host\":\"172.25.21.18:25308\",\"datasetId\":\"697766479417436096\",\"from\":\"1\",\"to\":\"A0FB86186\",\"username\":\"suntek\"}],\"tagInfo\":[]}"
      println(json)*/




   }
}
