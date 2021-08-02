package com.suntek.algorithm

import java.text.SimpleDateFormat
import java.util
import java.util.Calendar

import com.suntek.algorithm.common.bean.{DataBaseBean, QueryBean}
import com.suntek.algorithm.common.conf.{Constant, Param}
import com.suntek.algorithm.common.database.mpp.Mpp
import com.suntek.algorithm.common.util.SparkUtil
import com.suntek.algorithm.process.distribution.Distribution.registerUDF
import com.suntek.algorithm.process.sql.{MppStatisticsNew, MppStatisticsParam, SynchronizeDataFromMPPToHive, SynchronizeDataFromMPPToHiveParam}
import org.junit.Test
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

class TestCase {

  @Test
  def testFormat: Unit = {
    val str = "20200902000121"
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val timestamp =  dateFormat.parse(str).getTime
    val errorDateFormat = new SimpleDateFormat("yyMMddHHmmss")
    println("  " +errorDateFormat.format(timestamp))
    println(str)
  }

  @Test
  def testJson = {

//    val str = "yarn#--#{\"analyzeModel\":{\"env\":{\"shellName\":\"data-algorithm.sh\",\"deployEnvName\":\"ALG_DEPLOY_HOME\"},\"params\":[{\"cnName\":\"数据库名称\",\"enName\":\"dataBaseName\",\"desc\":\"数据库名称\",\"value\":\"default\"},{\"cnName\":\"数据库类型\",\"enName\":\"databaseType\",\"desc\":\"数据库类型\",\"value\":\"mpp\"},{\"cnName\":\"mppUrl\",\"enName\":\"url\",\"desc\":\"mppUrl\",\"value\":\"jdbc:postgresql://172.25.21.18:25308/pd_dts\"},{\"cnName\":\"mpp用户名\",\"enName\":\"username\",\"desc\":\"mpp用户名\",\"value\":\"suntek\"},{\"cnName\":\"mpp密码\",\"enName\":\"password\",\"desc\":\"mpp密码\",\"value\":\"suntek@123\"},{\"cnName\":\"同步方式\",\"enName\":\"method\",\"desc\":\"同步方式[jdbc，gds]\",\"value\":\"jdbc\"},{\"cnName\":\"同步表名\",\"enName\":\"tableName\",\"desc\":\"同步表名\",\"value\":\"hive_face_detect_rl\"},{\"cnName\":\"同步时间\",\"enName\":\"prevHourNums\",\"desc\":\"同步时间单位时\",\"value\":\"1\"}],\"isTag\":1,\"tagInfo\":[],\"mainClass\":\"com.suntek.algorithm.process.sql.SynchronizeDataFromMPPToHive\",\"modelId\":99405,\"modelName\":\"同步mpp数据导hive\",\"stepId\":99405,\"taskId\":99405,\"descInfo\":\"\"},\"batchId\":20210110000000}"
  val str = "yarn#--#{\"analyzeModel\":{\"env\":{\"shellName\":\"data-algorithm.sh\",\"deployEnvName\":\"ALG_DEPLOY_HOME\"},\"params\":[{\"cnName\":\"数据库名称\",\"enName\":\"dataBaseName\",\"desc\":\"数据库名称\",\"value\":\"default\"},{\"cnName\":\"数据库类型\",\"enName\":\"databaseType\",\"desc\":\"数据库类型\",\"value\":\"mpp\"},{\"cnName\":\"mppUrl\",\"enName\":\"url\",\"desc\":\"mppUrl\",\"value\":\"jdbc:postgresql://172.25.21.18:25308/pd_dts\"},{\"cnName\":\"mpp用户名\",\"enName\":\"username\",\"desc\":\"mpp用户名\",\"value\":\"suntek\"},{\"cnName\":\"mpp密码\",\"enName\":\"password\",\"desc\":\"mpp密码\",\"value\":\"suntek@123\"},{\"cnName\":\"同步方式\",\"enName\":\"method\",\"desc\":\"同步方式[jdbc，gds]\",\"value\":\"jdbc\"},{\"cnName\":\"同步表名\",\"enName\":\"tableName\",\"desc\":\"同步表名\",\"value\":\"car_detect_info\"},{\"cnName\":\"同步时间\",\"enName\":\"prevHourNums\",\"desc\":\"同步时间单位时\",\"value\":\"1\"}],\"isTag\":1,\"tagInfo\":[],\"mainClass\":\"com.suntek.algorithm.process.sql.SynchronizeDataFromMPPToHive\",\"modelId\":99405,\"modelName\":\"同步mpp数据导hive\",\"stepId\":99405,\"taskId\":99405,\"descInfo\":\"\"},\"batchId\":20200901000000}"

    val  param = new Param(str)
//    val  sParam = new SynchronizeDataFromMPPToHiveParam(param,true)
//    SynchronizeDataFromMPPToHive.process(param)
//    print()

    val  l = new util.LinkedList[Integer]();
    val ss = new util.Stack[Integer]()

  }

  @Test
  def testSpark = {

    var arrayBuffer = new ArrayBuffer[String]()
//    val start = "20210201000000"
//    val end   = "20210202000000"
    //code
//     val start = "20210307150000"
//    val end   = "20210309150000"
//    val start = "20210309150500"
//    val end   = "20210316200000"
//    val start = "20210123000000"
//    val end   = "20210124000000"
//    val start = "20210309000000"
//    val end = "20210309000000"
    //人脸
//    val start = "20200123044500"
//    val end = "20200223044500"
    //车辆
//    val start = "20210310105500"
//    val end   = "20210322114000"
    //码
    val start = "20210320101500"
    val end = "20210320103000"
    val formatStr = "yyyyMMddHHmmss"
    val simpleDateFormat = new SimpleDateFormat(formatStr)
    val startDate = simpleDateFormat.parse(start)
    val endDate = simpleDateFormat.parse(end)
    val calendar = Calendar.getInstance();
    calendar.setTime(startDate)
    while(calendar.getTime.getTime<=endDate.getTime)  {
      arrayBuffer.append(simpleDateFormat.format(calendar.getTime))
      calendar.add(Calendar.MINUTE,5)

    }

//    arrayBuffer.foreach(println)
//          val str = "local#--#{\"analyzeModel\":{\"env\":{\"shellName\":\"data-algorithm.sh\",\"deployEnvName\":\"ALG_DEPLOY_HOME\"},\"params\":[{\"cnName\":\"数据库名称\",\"enName\":\"dataBaseName\",\"desc\":\"数据库名称\",\"value\":\"default\"},{\"cnName\":\"数据库类型\",\"enName\":\"databaseType\",\"desc\":\"数据库类型\",\"value\":\"mpp\"},{\"cnName\":\"mppUrl\",\"enName\":\"url\",\"desc\":\"mppUrl\",\"value\":\"jdbc:postgresql://172.25.21.18:25308/beehive\"},{\"cnName\":\"mpp用户名\",\"enName\":\"username\",\"desc\":\"mpp用户名\",\"value\":\"suntek\"},{\"cnName\":\"mpp密码\",\"enName\":\"password\",\"desc\":\"mpp密码\",\"value\":\"suntek@123\"},{\"cnName\":\"延迟时间\",\"enName\":\"dalayTime\",\"desc\":\"延迟时间(秒)\",\"value\":\"300\"},{\"cnName\":\"统计类型\",\"enName\":\"statisticsType\",\"desc\":\"统计类型（人：表时间类型)\",\"value\":\"person:yyyyMMddHHmmss;statistics:yyyyMMddHHmmss\"}],\"isTag\":1,\"tagInfo\":[],\"mainClass\":\"com.suntek.algorithm.process.sql.MppStatistics\",\"modelId\":99406,\"modelName\":\"mpp统计\",\"stepId\":99406,\"taskId\":99406,\"descInfo\":\"\"},\"batchId\":"+batchID+"}"
    arrayBuffer.foreach(batchID=>{
          val str = "local#--#{\"analyzeModel\":{\"env\":{\"shellName\":\"data-algorithm.sh\",\"deployEnvName\":\"ALG_DEPLOY_HOME\"},\"params\":[{\"cnName\":\"数据库名称\",\"enName\":\"dataBaseName\",\"desc\":\"数据库名称\",\"value\":\"default\"},{\"cnName\":\"数据库类型\",\"enName\":\"databaseType\",\"desc\":\"数据库类型\",\"value\":\"mpp\"},{\"cnName\":\"mppUrl\",\"enName\":\"url\",\"desc\":\"mppUrl\",\"value\":\"jdbc:postgresql://172.25.21.18:25308/beehive\"},{\"cnName\":\"mpp用户名\",\"enName\":\"username\",\"desc\":\"mpp用户名\",\"value\":\"suntek\"},{\"cnName\":\"mpp密码\",\"enName\":\"password\",\"desc\":\"mpp密码\",\"value\":\"suntek@123\"},{\"cnName\":\"延迟时间\",\"enName\":\"dalayTime\",\"desc\":\"延迟时间(秒)\",\"value\":\"300\"},{\"cnName\":\"统计类型\",\"enName\":\"statisticsType\",\"desc\":\"统计类型（人：表时间类型)\",\"value\":\"person:yyMMddHHmmss;car:yyMMddHHmmss;code:yyMMddHHmmss;statistics:yyyyMMddHHmmss\"}],\"isTag\":1,\"tagInfo\":[],\"mainClass\":\"com.suntek.algorithm.process.sql.MppStatistics\",\"modelId\":99410,\"modelName\":\"人档轨迹统计\",\"stepId\":99410,\"taskId\":99410,\"descInfo\":\"\"},\"batchId\":"+batchID+"}"
          val  param = new Param(str)
          val sparm = new MppStatisticsParam(param,"")
          MppStatisticsNew.process(param)
    })







//    print()
//    val sparkSession = SparkUtil.getSparkSession("local", s"SYNCHRONIZE DATA FROM MPP TO HIVE  ${param.batchId}  Job", param)
//    val dataBaseBean = new DataBaseBean()
//    dataBaseBean.setUsername("suntek") //suntek
//    dataBaseBean.setPassword("suntek@123") //suntek@123
//    dataBaseBean.setDriver(Constant.MPP_DRIVER)
//    dataBaseBean.setUrl("jdbc:postgresql://172.25.21.18:25308/pd_dts") //
//    dataBaseBean.setNumPartitions(2)
//    val mpp = new Mpp(sparkSession, dataBaseBean)
//    val queryBean = new QueryBean()
//    queryBean.batchSize = 100
//    queryBean.isPage = true
//    val sql = "insert into tb_person_track_statistics select person_id,count(1) as cnt  from hive_face_detect_rl  where shot_time >20200901000000 group by person_id "
//     mpp.excute(sql, queryBean)

  }
  @Test
  def testUDF()={
    val batchID ="20210406000000"
    val json ="local#--#{\"analyzeModel\":{\"env\":{\"shellName\":\"data-algorithm.sh\",\"deployEnvName\":\"ALG_DEPLOY_HOME\"},\"params\":[{\"cnName\":\"数据库名称\",\"enName\":\"dataBaseName\",\"desc\":\"数据库名称\",\"value\":\"default\"},{\"cnName\":\"数据库类型\",\"enName\":\"databaseType\",\"desc\":\"数据库类型\",\"value\":\"mpp\"}  ,{\"cnName\":\"mpp库名\",\"enName\":\"landDataBase\",\"desc\":\"mpp库名\",\"value\":\"beehive\"} ,{\"cnName\":\"mppUrl\",\"enName\":\"url\",\"desc\":\"mppUrl\",\"value\":\"jdbc:postgresql://172.25.21.18:25308/beehive\"},{\"cnName\":\"mpp用户名\",\"enName\":\"username\",\"desc\":\"mpp用户名\",\"value\":\"suntek\"},{\"cnName\":\"mpp密码\",\"enName\":\"password\",\"desc\":\"mpp密码\",\"value\":\"suntek@123\"},{\"cnName\":\"延迟时间\",\"enName\":\"dalayTime\",\"desc\":\"延迟时间(秒)\",\"value\":\"300\"},{\"cnName\":\"统计类型\",\"enName\":\"statisticsType\",\"desc\":\"统计类型（人：表时间类型)\",\"value\":\"person:yyyyMMddHHmmss;car:yyyyMMddHHmmss;code:yyyyMMddHHmmss;statistics:yyyyMMddHHmmss\"}],\"isTag\":1,\"tagInfo\":[],\"mainClass\":\"com.suntek.algorithm.process.sql.MppStatistics\",\"modelId\":99410,\"modelName\":\"人档轨迹统计\",\"stepId\":99410,\"taskId\":99410,\"descInfo\":\"\"},\"batchId\":"+batchID+"}"
    val param: Param = new Param(json)
//    val sParam = new MppStatisticsParamOld(param,batchID)
    val master = param.master
    val ss = SparkUtil.getSparkSession(master, s"MPP Statistics  Job", param)

    registerUDF(ss, "isValidEntity", "com.suntek.algorithm.udf.EntityCheckUDF" )
    ss.sql(s"select '================' where isValidEntity('135a','PHONE')").show()
  }

@Test
  def test () {
    println(Array(1,2,3,4).takeRight(1).toList)

  }


  @Test
  def testLog = {
    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    logger.info("=================")
  }


  @Test
  def testList() ={
    var list = List(1,2)
    list +:= 3
    println(list)
  }
}
