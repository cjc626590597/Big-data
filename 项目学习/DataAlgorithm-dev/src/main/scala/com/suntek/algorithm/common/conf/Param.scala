package com.suntek.algorithm.common.conf

import java.io.{FileInputStream, InputStreamReader}
import java.text.SimpleDateFormat
import java.time.ZoneId
import java.util.{Date, Properties, TimeZone}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.suntek.algorithm.common.http.HttpUtil
import org.apache.commons.lang3.StringUtils
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

@SerialVersionUID(2L)
class Param(argsStr: String) extends Serializable {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val zoneId: ZoneId = ZoneId.of("Asia/Shanghai")

  val batchIdDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
  batchIdDateFormat.setTimeZone(TimeZone.getTimeZone(zoneId))

  val dateFormat = new SimpleDateFormat("yyyyMMdd")
  dateFormat.setTimeZone(TimeZone.getTimeZone(zoneId))

  val jsonSeparator = "#--#"
  //系统内置变量，动态替换
  val startDateTimeParam = "@startDateTime@"
  val endDateTimeParam = "@endDateTime@"
  val startHourParam = "@startHour@"
  val endHourParam = "@endHour@"
  val startDateParam  = "@startDate@"
  val endDateParam  = "@endDate@"
  val statDateParam  = "@statDate@"
  val startStatDayParam = "@startStatDay@"
  val endStatDayParam = "@endStatDay@"

  val NUMPARTITIONS = "numPartitions"

  val nowDateParam = "@nowDate@"

  logger.info(s"argsStr = $argsStr")

  val argsArray: Array[String] = argsStr.split(jsonSeparator)

  val master: String = argsArray(0)
  logger.info(s"master = $master")

  val json: String = argsArray(1)
  logger.info(s"json = $json")

  val keyMap: mutable.Map[String, Object] = doAnalysis(json)

  //关联类型
  val releTypeStr: String =  keyMap.getOrElse("relType", "imei-imsi").toString.toLowerCase
  keyMap += "type" -> releTypeStr
  logger.info(s"releTypeStr = $releTypeStr")
  val releTypes: Array[String] = releTypeStr.split("-")

  //关系类型
  var relationType = 1 //默认为要素关联

  //如果关联类型一样，则关系类型为2，即实体关系
  if(releTypes(0).equals(releTypes(1))){
    relationType = 2
  }

  keyMap .put("relationType" ,relationType.toString)

  val nowDate = new Date()
  val now: String = keyMap.getOrElse("NOW_DATE", dateFormat.format(nowDate)).toString

  logger.info(s"now : $now")
  val timeStamp: Long = dateFormat.parse(now).getTime

  var prevHourNum: Int = Integer.parseInt(keyMap.getOrElse("prevHourNums", "24").toString)

  val batchId: String = keyMap("batchId").toString

  // 按天计算
  val batchIdByDay: String = s"${keyMap("batchId").toString.substring(0, 8)}000000"
  var startTimeStamp: Long = batchIdDateFormat.parse(batchIdByDay).getTime - prevHourNum * 60 * 60 * 1000L
  var startTime: String = batchIdDateFormat.format(startTimeStamp)
  logger.info(s"startTime: $startTime")
  val batchTimestamp: Long = batchIdDateFormat.parse(startTime).getTime  / 1000
  logger.info(s"batchTimestamp: $batchTimestamp")
  logger.info(s"batchIdByDay: $batchIdByDay")
  var endTimeStamp: Long = batchIdDateFormat.parse(batchIdByDay).getTime -  60 * 60 * 1000L
  logger.info(s"endTimeStamp: $endTimeStamp")
  var endTime: String = batchIdDateFormat.format(endTimeStamp)
  logger.info(s"endTime: $endTime")


  val seqSeparator: String = keyMap.getOrElse("SEQ_SEPARATOR", "#").toString

  val mf: Double = keyMap.getOrElse("mf", "1.0").toString.toDouble

  var svType : String = _

  var category1: String = Constant.PRIORITY_LEVEL.getOrElse(releTypes(0), "-1").toString
  var category2: String = Constant.PRIORITY_LEVEL.getOrElse(releTypes(1), "-1").toString

  val nowFormat: String = batchIdDateFormat.format(nowDate)

  val lastStatDateStamp: Long = batchIdDateFormat.parse(keyMap("batchId").toString).getTime - prevHourNum * 2 * 60 * 60 * 1000L

  val lastStatDate: String = batchIdDateFormat.format(lastStatDateStamp).substring(0, 8)

  val deployEnvName = keyMap.getOrElse("deployEnvName", "ALG_DEPLOY_HOME").toString

  loadModelSql()
  loadConf()
  var yarnApplicationId: String = ""

  //admin的任务状态更新接口地址
  val adminTaskStatusUpdateUrl: String = keyMap.getOrElse("xxl.job.admin.addresses", "") + "/service/manage/task/backgroundTaskStatusUpdate"

  val nacosAddr = keyMap.getOrElse("nacos.config.server.addr", "nacos-center.v-base:30848")
  val nacosNamespace = keyMap.getOrElse("nacos.config.namespace", "a85a37ef-5bec-478c-a60f-0b11f10b3da4")
  val nacosUrl = s"http://${nacosAddr}/nacos/v1/cs/configs?dataId=@dataId@&group=@group@&tenant=${nacosNamespace}"

  val mainClass = keyMap("mainClass").toString

  var esNodesTcp: String = _
  var esNodesHttp: String = _
  var esUserName: String = _
  var esPassword: String = _

  var mysqlIp: String = _
  var mysqlPort: String = _
  var mysqlUserName: String = _
  var mysqlPassWord: String = _

  var mppdbType: String = _
  var mppdbIp: String = _
  var mppdbPort: String = _
  var mppdbUsername: String = _
  var mppdbPassword: String = _

  var redisNode: String = _
  var redisPassword: String = _
  var redisDataBase: Int = 0

  loadNacosOrLocal(nacosUrl, mainClass)


  /** *
    * 参数解析
    *
    * @param jsonStr
    */
  def doAnalysis(jsonStr: String): mutable.Map[String, Object] = {
    val json = JSON.parseObject(jsonStr)
    // 参数解析
    val analyzeModel = json.getJSONObject("analyzeModel") // 模型相关
    val map = doParam(analyzeModel)
    val  batchId = json.getString("batchId") // 获取batchId
    map.put("batchId", batchId)
    logger.info(s"分析参数:batchId  $batchId")
    //通过batchId计算now值
    val now = dateFormat.format(batchIdDateFormat.parse(batchId))
    map.put("now", now)
    logger.info(s"分析参数:now  $now")

    map
  }
  /**
    * 输入解析
    *
    * @param jsonObject json结构
    */
  def doParam(jsonObject: JSONObject): mutable.Map[String, Object] = {
    val paramMap = mutable.Map[String, Object]()

    val mainClass = jsonObject.getString("mainClass")
    paramMap.put("mainClass", mainClass)
    logger.info(s"分析参数:mainClass  $mainClass")

    val taskId = jsonObject.getString("taskId")
    paramMap.put("taskId", taskId)
    logger.info(s"分析参数:taskId  $taskId")
    val stepId = jsonObject.getString("stepId")
    paramMap.put("stepId", stepId)
    logger.info(s"分析参数:stepId  $stepId")

    val isTag = jsonObject.getIntValue("isTag")
    paramMap.put("isTag", isTag + "")
    logger.info(s"分析参数:isTag  $isTag")

    val params = jsonObject.getJSONArray("inputs").iterator()
    while (params.hasNext) {
      val objectV = params.next().asInstanceOf[JSONObject]
      val name = objectV.get("enName").toString
      val cnName = objectV.get("cnName").toString
      var value = ""
      if (objectV.containsKey("inputVal")) value = objectV.get("inputVal").toString
      paramMap.put(name, value)
      logger.info(s"分析参数: $name(${cnName})=, $value")
    }

    paramMap
  }


  def readConf(conf: java.io.File)={
    @transient
    val properties = new Properties()
    val istr = new InputStreamReader(new FileInputStream(conf), "UTF-8")
    properties.load(istr)
    istr.close()

    val piterator = properties.entrySet().iterator()
    while (piterator.hasNext){
      val p = piterator.next()
      val key = p.getKey.toString
      val value = p.getValue.toString
      keyMap += (key -> value)
    }
  }
  /***
    * 加载model.sql文件中的模型sql，并存放到keyMap中
    */
  def loadModelSql() = {
    // sql配置文件
    var path: String = System.getenv(deployEnvName)
    if (path == null){
      //读取不到环境变量时，为本地调试模式，为避免集群也忘了配置环境变量，导致相对路径能用，此处定为各自开发的绝对路径
//      path = "D:\\workpace\\PCI\\analyse-model\\DataAlgorithm\\src\\main\\resources\\conf\\model.sql"
      path = "C:\\Users\\suntek\\Desktop\\数据开发\\Big-data\\项目学习\\DataAlgorithm-dev\\src\\main\\resources"
    }

    var modelSqlFilePath = s"$path/conf/model.sql"
    println(s"modelSqlFilePath: ${modelSqlFilePath}")
    var conf = new java.io.File(modelSqlFilePath)
    if (!conf.exists()) {
//      modelSqlFilePath = "/opt/data-algorithm/conf/model.sql"
      modelSqlFilePath = "C:\\Users\\suntek\\Desktop\\数据开发\\Big-data\\项目学习\\DataAlgorithm-dev\\src\\main\\resources\\conf\\model.sql"
      conf = new java.io.File(modelSqlFilePath)
    }
    readConf(conf)

  }

  def loadConf() = {
    var path: String = System.getenv(deployEnvName)
    if (path == null){
      //读取不到环境变量时，为本地调试模式，为避免集群也忘了配置环境变量，导致相对路径能用，此处定为各自开发的绝对路径
//      path = "D:\\workpace\\PCI\\analyse-model\\DataAlgorithm\\src\\main\\resources"
      path = "C:\\Users\\suntek\\Desktop\\数据开发\\Big-data\\项目学习\\DataAlgorithm-dev\\src\\main\\resources"
    }

    var commonConfFilePath = s"$path/conf/common.conf"
    var conf = new java.io.File(commonConfFilePath)
    if (!conf.exists()) {
//      commonConfFilePath = "/opt/data-algorithm/conf/common.conf"
      commonConfFilePath = "C:\\Users\\suntek\\Desktop\\数据开发\\Big-data\\项目学习\\DataAlgorithm-dev\\src\\main\\resources"
      conf = new java.io.File(commonConfFilePath)
    }
    logger.info("common.conf path = " + conf.getAbsolutePath)
    readConf(conf)


    var algSelfConfFilePath = s"$path/conf/alg-self.conf"
    var selfConf = new java.io.File(algSelfConfFilePath)
    if (!selfConf.exists()) {
      algSelfConfFilePath = "/opt/data-algorithm/conf/alg-self.conf"
//      algSelfConfFilePath = "D:\\workpace\\PCI\\analyse-model\\DataAlgorithm\\src\\main\\resources\\conf\\alg-self.conf"
      selfConf = new java.io.File(algSelfConfFilePath)
    }
    logger.info("alg-self.conf path = " + selfConf.getAbsolutePath)
    readConf(selfConf)
  }


  def loadHive(mainClass: String, confMap: mutable.Map[String, String]) = {
    if(StringUtils.isNoneBlank(confMap.getOrElse("hive.nodes","").toString)){
      keyMap.put("hive.nodes", confMap("hive.nodes").toString)
    }
  }

  /**
    * 加载基础组件配置:ES/HIVE
    *
    * @param str
    * @param str1
    * @return
    */
  def loadBaseComponents(nacosUrl: String, mainClass: String) = {
    val dataId = "base-components"
    val group = "prophet"

    val baseComponentsUrl = nacosUrl
      .replace("@dataId@", dataId)
      .replace("@group@", group)

    logger.info(s"${baseComponentsUrl}, ${mainClass}")
    val confMap = HttpUtil.sendGet(baseComponentsUrl)
    loadES( mainClass, confMap)
    loadMysql( mainClass, confMap)
    loadMppdb( mainClass, confMap)
    loadHive(mainClass, confMap)
    loadRedis(confMap)
    confMap.clear()
  }

  def loadxlJob(nacosUrl: String) = {

    val dasDataId = "das"
    val dasGroup = "das"

    val dasUrl = nacosUrl
      .replace("@dataId@", dasDataId)
      .replace("@group@", dasGroup)

    logger.info(s"${dasUrl}")
    val confMap = HttpUtil.sendGet(dasUrl)

    val xxlJobServerIp = confMap.getOrElse("xxl.job.server.ip", "")
    val xxlJobServerPort = confMap.getOrElse("xxl.job.server.port", "")
    val xxlJobServerContextPath = confMap.getOrElse("xxl.job.server.context-path", "")
    val faceDetectRlRealTime = confMap.getOrElse("face.detect.rl.real.time", "1")
    keyMap.put("faceDetectRlRealTime", faceDetectRlRealTime);
    if(StringUtils.isNoneBlank(xxlJobServerIp)
      && StringUtils.isNoneBlank(xxlJobServerPort)
      && StringUtils.isNoneBlank(xxlJobServerContextPath)){
      val xxlConsoleUrl = s"http://${xxlJobServerIp}:${xxlJobServerPort}${xxlJobServerContextPath}"
      keyMap.put("xxl.console.url", xxlConsoleUrl)
    }

    // 覆盖 base compment
    loadDasConfigCover(confMap)

    confMap.clear()

  }

  /**
    * 加载依赖服务配置
    *
    * @param str
    * @param str1
    */
  def loadApplications(nacosUrl: String, mainClass: String): Unit = {

    loadxlJob(nacosUrl)
  }

  /**
    * 每次启动的时候，初始化配置：优先从nacos获取，如果nacos获取不到，则从本地配置文件中读取
    */
  def loadNacosOrLocal(url : String, mainClass: String): Unit ={

    logger.info(s"${url}")
    //加载基础组件配置
    loadBaseComponents(url : String, mainClass: String)

    //加载依赖服务配置
    loadApplications(url : String, mainClass: String)

  }

  def loadMysql(mainClass: String,
             configMap:mutable.Map[String, String])
  :(String, String, String, String) = {
    val mysqlIp = configMap.getOrElse("mysql.ip", keyMap("mysql.ip").toString)
    val mysqlPort = configMap.getOrElse("mysql.port", keyMap("mysql.port").toString)
    val mysqlUsername = configMap.getOrElse("mysql.username", keyMap("mysql.username").toString)
    val mysqlPassword =configMap.getOrElse("mysql.password", keyMap("mysql.password").toString)

    this.mysqlIp = mysqlIp
    this.mysqlPort = mysqlPort
    this.mysqlUserName = mysqlUsername
    this.mysqlPassWord = mysqlPassword

    (mysqlIp, mysqlPort, mysqlUsername, mysqlPassword)
  }

  def loadMppdb(mainClass: String,
                configMap:mutable.Map[String, String])
  :(String, String, String, String, String) = {

    val mppdbType = configMap.getOrElse("mppdb.type", keyMap("mppdb.type").toString)
    val mppdbIp = configMap.getOrElse("mppdb.ip", keyMap("mppdb.ip").toString)
    val mppdbPort = configMap.getOrElse("mppdb.port", keyMap("mppdb.port").toString)
    val mppdbUsername = configMap.getOrElse("mppdb.username", keyMap("mppdb.username").toString)
    val mppdbPassword = configMap.getOrElse("mppdb.password", keyMap("mppdb.password").toString)

    this.mppdbType = mppdbType
    this.mppdbIp = mppdbIp
    this.mppdbPort = mppdbPort
    this.mppdbUsername = mppdbUsername
    this.mppdbPassword = mppdbPassword

    (mppdbType, mppdbIp, mppdbPort, mppdbUsername, mppdbPassword)
  }
  // redis.nodes redis.track.count.nodes
  // nacos.redis.track.count = redis.track.count.nodes
  def loadRedis(configMap:mutable.Map[String, String]):(String, String, String) = {

    val redisNodeDefault = configMap.getOrElse("redis.nodes", keyMap("redis.nodes").toString)
    val redisPasswordDefault = configMap.getOrElse("redis.password", keyMap("redis.password").toString)
    val redisDatabaseDefault = configMap.getOrElse("redis.database", "0")

    /*if (keyMap("mainClass").toString.contains("MppStatistics")) {
      configMap.getOrElse(keyMap("nacosRedisTrackCount").toString, keyMap("redis.nodes").toString)
    }else {
      configMap.getOrElse("redis.nodes", keyMap("redis.nodes").toString)
    }*/

    this.redisNode = redisNodeDefault
    this.redisPassword = redisPasswordDefault
    this.redisDataBase = if(redisDatabaseDefault == null || redisDatabaseDefault == "") 0 else Integer.parseInt(redisDatabaseDefault)

    (redisNodeDefault, redisPasswordDefault, redisDatabaseDefault)
  }

  def loadES(mainClass: String,
             configMap:mutable.Map[String, String])
  :(String, String, String, String) = {

    var esNodesTcpDefault = configMap.getOrElse("elasticsearch7.nodes.tcp","").toString
    var esNodesHttpDefault = configMap.getOrElse("elasticsearch7.nodes.http","").toString
    var esUserNameDefault = configMap.getOrElse("elasticsearch7.username","").toString
    var esPasswordDefault = configMap.getOrElse("elasticsearch7.password","").toString
    logger.info(s"es1: ${esNodesTcpDefault},${esNodesHttpDefault},${esUserNameDefault},${esPasswordDefault}")
    if(esNodesHttpDefault.isEmpty && esNodesTcpDefault.isEmpty){
      esNodesTcpDefault =  configMap.getOrElse("elasticsearch2.nodes.tcp","").toString
      esNodesHttpDefault =  configMap.getOrElse("elasticsearch2.nodes.http","").toString
      esUserNameDefault =  configMap.getOrElse("elasticsearch2.username","").toString
      esPasswordDefault =  configMap.getOrElse("elasticsearch2.password","").toString
    }
    logger.info(s"es2: ${esNodesTcpDefault},${esNodesHttpDefault},${esUserNameDefault},${esPasswordDefault}")
    if(StringUtils.isNoneBlank(esNodesTcpDefault)){
      configMap.put("elasticsearch.nodes.tcp", esNodesTcpDefault)
    }

    if(StringUtils.isNoneBlank(esNodesHttpDefault)){
      configMap.put("elasticsearch.nodes.http", esNodesHttpDefault)
    }

    if(StringUtils.isNoneBlank(esUserNameDefault)){
      configMap.put("elasticsearch.username", esUserNameDefault)
    }

    if(StringUtils.isNoneBlank(esPasswordDefault)){
      configMap.put("elasticsearch.password", esPasswordDefault)
    }

    esNodesTcpDefault = configMap.getOrElse("elasticsearch.nodes.tcp", keyMap("elasticsearch.nodes.tcp").toString)
    esNodesHttpDefault = configMap.getOrElse("elasticsearch.nodes.http", keyMap("elasticsearch.nodes.http").toString)
    esUserNameDefault = configMap.getOrElse("elasticsearch.username", keyMap("elasticsearch.username").toString)
    esPasswordDefault =configMap.getOrElse("elasticsearch.password", keyMap("elasticsearch.password").toString)
    logger.info(s"es3: ${esNodesTcpDefault},${esNodesHttpDefault},${esUserNameDefault},${esPasswordDefault}")

    this.esNodesTcp = esNodesTcpDefault
    this.esNodesHttp = esNodesHttpDefault
    this.esUserName = esUserNameDefault
    this.esPassword = esPasswordDefault

    (esNodesTcpDefault, esNodesHttpDefault, esUserNameDefault, esPasswordDefault)
  }

  def loadDasConfigCover(configMap:mutable.Map[String, String]) = {
    val redisNodeCover = configMap.getOrElse("redis.nodes", "")
    val redisPasswordCover = configMap.getOrElse("redis.password", "")
    val redisDatabaseCover = configMap.getOrElse("redis.database", "")

    val esNodesTcpCover = configMap.getOrElse("elasticsearch7.nodes.tcp","")
    val esNodesHttpCover = configMap.getOrElse("elasticsearch7.nodes.http","")
    val esUserNameCover = configMap.getOrElse("elasticsearch7.username","")
    val esPasswordCover = configMap.getOrElse("elasticsearch7.password","")

    val mppdbTypeCover = configMap.getOrElse("mppdb.type", "")
    val mppdbIpCover = configMap.getOrElse("mppdb.ip", "")
    val mppdbPortCover = configMap.getOrElse("mppdb.port", "")
    val mppdbUsernameCover = configMap.getOrElse("mppdb.username", "")
    val mppdbPasswordCover = configMap.getOrElse("mppdb.password", "")

    val mysqlIpCover = configMap.getOrElse("mysql.ip", "")
    val mysqlPortCover = configMap.getOrElse("mysql.port", "")
    val mysqlUsernameCover = configMap.getOrElse("mysql.username", "")
    val mysqlPasswordCover =configMap.getOrElse("mysql.password", "")

    val hiveNodesCover =configMap.getOrElse("hive.nodes", "")

    if (redisNodeCover != "")     this.redisNode = redisNodeCover
    if (redisPasswordCover != "") this.redisPassword = redisPasswordCover
    if (redisDatabaseCover != "") this.redisDataBase = if(redisDatabaseCover == null || redisDatabaseCover == "") 0 else Integer.parseInt(redisDatabaseCover)

    if (esNodesTcpCover != "")  this.esNodesTcp = esNodesTcpCover
    if (esNodesHttpCover != "") this.esNodesHttp = esNodesHttpCover
    if (esUserNameCover != "")  this.esUserName = esUserNameCover
    if (esPasswordCover != "")  this.esPassword = esPasswordCover

    if (mppdbTypeCover != "")     this.mppdbType = mppdbTypeCover
    if (mppdbIpCover != "")       this.mppdbIp = mppdbIpCover
    if (mppdbPortCover != "")     this.mppdbPort = mppdbPortCover
    if (mppdbUsernameCover != "") this.mppdbUsername = mppdbUsernameCover
    if (mppdbPasswordCover != "") this.mppdbPassword = mppdbPasswordCover

    if (mysqlIpCover != "")       this.mysqlIp = mysqlIpCover
    if (mysqlPortCover != "")     this.mysqlPort = mysqlPortCover
    if (mysqlUsernameCover != "") this.mysqlUserName = mysqlUsernameCover
    if (mysqlPasswordCover != "") this.mysqlPassWord = mysqlPasswordCover
  }
}
