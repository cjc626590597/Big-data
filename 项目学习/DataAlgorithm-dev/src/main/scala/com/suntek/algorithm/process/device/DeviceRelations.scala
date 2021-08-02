package com.suntek.algorithm.process.device

import com.alibaba.druid.pool.DruidDataSource
import com.alibaba.druid.util.JdbcUtils
import com.suntek.algorithm.common.bean.SaveParamBean
import com.suntek.algorithm.common.conf.{Constant, Param}
import com.suntek.algorithm.common.database.DataBaseFactory
import com.suntek.algorithm.common.util.{ConstantUtil, DruidUtil, SparkUtil}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import java.util.Properties
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
  * @author zhy
  * @date 2021-2-19 10:39
  */
object DeviceRelations {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val DEVICE_CAMERA_SQL = "device.camera.sql"
  val DEVICE_VEHICLE_SQL = "device.vehicle.sql"
  val DEVICE_WIFI_EFENSE_SQL = "device.wifi.efense.sql"
  val DEVICE_DOORACCESS_SQL = "device.dooraccess.sql"
  val CAR = "CAR"
  val PHONE = "PHONE"
  val IMEI = "IMEI"
  val IMSI = "IMSI"
  val MAC = "MAC"
  val PERSON = "PERSON"
  val batchSize = 100000

  def genPageSql(sql: String,
                 ds: DruidDataSource)
  : Array[String] = {
    val sqlTotal = s"select count(*) as nums from ( ${sql} ) a"
    logger.info(sqlTotal)
    val retTotal = JdbcUtils.executeQuery(ds, sqlTotal)
    val total =  Integer.parseInt(retTotal.get(0).get("nums").toString)
    val pageSize = if(total % batchSize == 0) total/batchSize else total/batchSize + 1
    val array = ArrayBuffer[(Int, Int)]()
    for (i <- 0 until pageSize){
      val start = i * batchSize
      val end = batchSize
      array += start -> end
    }
    array.map{
      case (start, end) => {
        s" ${sql} limit $start, $end"
      }
    }.toArray
  }

  def loadDooraccessData(ss: SparkSession,
                         sql: String,
                         param: Param,
                         ds: DruidDataSource)
  : RDD[(String, (Float, Float, String, String))] = {
    val sqlList = genPageSql(sql, ds)
    val lists = sqlList.map(v1=>{
      val values = JdbcUtils.executeQuery(ds, v1)
        .asScala.map(v2=>{
        var deviceId = ""
        var jd = 0f
        var wd = 0f
        var glsxjbm = ""
        var glmjb = ""
        try{
          deviceId = v2.get("BM").toString
          jd = v2.get("JD").toString.toFloat
          wd = v2.get("WD").toString.toFloat
          if(v2.get("GLSXJBM") != null){
            glsxjbm = v2.get("GLSXJBM").toString // 关联摄像机
          }
          if(v2.get("GLMJBM") != null){
            glmjb = v2.get("GLMJBM").toString// 关联门禁
          }
        }catch {
          case _: Exception=> {
            jd = 0f
            wd = 0f
          }
        }
        (deviceId, (jd, wd, glsxjbm, glmjb))
      })
      ss.sparkContext.makeRDD(values)
    })
    var ret = lists(0)
    for(i <- 1 until lists.size){
      ret = ret.union(lists(i))
    }
    logger.info("777vloadDooraccessData:", ret.count())
    ret.reduceByKey((x, _) => x)
  }

  def loadWifiEfenseData(ss: SparkSession,
                         sql: String,
                         param: Param,
                         properties: Properties,
                         ds: DruidDataSource)
  : RDD[(String, (Float, Float, String, String, String))] = {
    val sqlList = genPageSql(sql, ds)
    val lists = sqlList.map(v1=>{
      logger.info(s"loadWifiEfenseData: ${v1}")
      val values = JdbcUtils.executeQuery(ds, v1)
        .asScala.map(v2=>{
        var deviceId = ""
        var jd = 0f
        var wd = 0f
        var glkkbm = ""
        var glsxjbm = ""
        var glmjb = ""
        try{
          deviceId = v2.get("BM").toString
          jd = v2.get("JD").toString.toFloat
          wd = v2.get("WD").toString.toFloat
          if(v2.get("GLKKBH") != null){
            glkkbm = v2.get("GLKKBH").toString // 关联卡口
          }
          if(v2.get("GLSXJBM") != null){
            glsxjbm = v2.get("GLSXJBM").toString // 关联摄像机
          }
          if(v2.get("GLMJBM") != null){
            glmjb = v2.get("GLMJBM").toString// 关联门禁
          }
        }catch {
          case _: Exception=> {
            jd = 0f
            wd = 0f
          }
        }
        (deviceId, (jd, wd, glkkbm, glsxjbm, glmjb))
      })
      val rd = ss.sparkContext.makeRDD(values)
      rd
    })
    var ret = lists(0)
    for(i <- 1 until lists.size){
      ret = ret.union(lists(i))
    }
    ret.reduceByKey((x, _) => x).coalesce(20)
  }

  def loadData(ss: SparkSession,
               sql: String,
               param: Param,
               properties: Properties,
               ds: DruidDataSource)
  : RDD[(String, (Float, Float))] = {
    val sqlList = genPageSql(sql, ds)
    val lists = sqlList.map(v1=>{
      logger.info(s"loadData:${v1}")
      val values = JdbcUtils.executeQuery(ds, v1)
        .asScala.map(v2=>{
        var deviceId = ""
        var jd = 0f
        var wd = 0f
        try{
          deviceId = v2.get("BM").toString
          jd = v2.get("JD").toString.toFloat
          wd = v2.get("WD").toString.toFloat
        }catch {
          case ex: Exception=>
        }
        (deviceId, (jd, wd))
      }).filter(r => r._1.nonEmpty && r._2._1 > 0f && r._2._2 > 0f)
      val rd = ss.sparkContext.makeRDD(values)
      rd
    })
    var ret = lists(0)
    for(i <- 1 until lists.length){
      ret = ret.union(lists(i))
    }
    ret.reduceByKey((x, _) => x).coalesce(20)
  }

  def getRelations1(ss: SparkSession,
                    param: Param,
                    properties: Properties,
                    ds: DruidDataSource)
  : RDD[Row]={
    val data = loadWifiEfenseData(ss, param.keyMap(DEVICE_WIFI_EFENSE_SQL).toString, param, properties, ds)
    data.filter(r => r._1.nonEmpty && r._2._3.nonEmpty)
        .flatMap(v=> {
          Array[Row](
            Row.fromTuple(v._2._3, CAR, v._1, PHONE),
            Row.fromTuple(v._2._3, CAR, v._1, IMEI),
            Row.fromTuple(v._2._3, CAR, v._1, IMSI),
            Row.fromTuple(v._2._3, CAR, v._1, MAC),
            Row.fromTuple(v._2._3, PERSON, v._1, PHONE),
            Row.fromTuple(v._2._3, PERSON, v._1, IMEI),
            Row.fromTuple(v._2._3, PERSON, v._1, IMSI),
            Row.fromTuple(v._2._3, PERSON, v._1, MAC)
          )
        })
  }

  def rad(d: Double): Double = {
    d * (Math.PI / 180.0)
  }

  def distance(lat1: Double,
               lng1: Double,
               lat2: Double,
               lng2: Double): Double = {
    val radLat1 = rad(lat1)
    val radLat2 = rad(lat2)
    val radLng1 = rad(lng1)
    val radLng2 = rad(lng2)
    val dlng = radLng2 - radLng1
    val dlat = radLat2 - radLat1
    val a = Math.pow(Math.sin(dlat/2), 2) +Math.cos(radLat1) * Math.cos(radLat2) *  Math.pow(Math.sin(dlng/2), 2)
    val c = 2* Math.asin(Math.sqrt(a))
    c * 6371 * 1000
  }

  def getRelations2(ss: SparkSession,
                    param: Param,
                    properties: Properties,
                    ds: DruidDataSource)
  : RDD[Row]={
    val maxDis = param.keyMap.getOrElse("maxDistance", "100").toString.toFloat
    val maxCount = param.keyMap.getOrElse("maxRelDeviceCount", "5").toString.toInt
    val maxCountBC = ss.sparkContext.broadcast(maxCount)
    val data = loadWifiEfenseData(ss, param.keyMap(DEVICE_WIFI_EFENSE_SQL).toString, param, properties, ds)
                      .filter(_._1.nonEmpty)
                      .map(v=>(v._1, (v._2._1, v._2._2)))

    logger.info(s"wifidata:${data.count()}")
      //.filter(v=> v._2._1 > 0 && v._2._2 > 0)
    val vehicleData = loadData(ss, param.keyMap(DEVICE_VEHICLE_SQL).toString, param, properties, ds)
    logger.info(s"vehicleData:${vehicleData.count()}")
    val cameraData = loadData(ss, param.keyMap(DEVICE_CAMERA_SQL).toString, param, properties, ds)
    logger.info(s"cameraData:${cameraData.count()}")
    // 车和码
    val car_code = vehicleData.cartesian(data)
      .map(v => {
        val id1 = v._1._1
        val jd1 = v._1._2._1
        val wd1 = v._1._2._2
        val id2 = v._2._1
        val jd2 = v._2._2._1
        val wd2 = v._2._2._2
        val dis = distance(jd1, wd1,jd2, wd2)
        if(dis <= maxDis){
          (id1, (id2, dis))
        }else{
          null
        }
      })
      .filter(_ != null)
      .groupByKey()
      .flatMap(v=>{
        val id1 = v._1
        v._2.toList.sortBy(v=> v._2).take(maxCountBC.value)
          .flatMap(r=> {
            Array[Row](
              Row.fromTuple(id1, CAR, r._1, PHONE),
              Row.fromTuple(id1, CAR, r._1, IMEI),
              Row.fromTuple(id1, CAR, r._1, IMSI),
              Row.fromTuple(id1, CAR, r._1, MAC)
            )
          })
      })
    logger.info(s"car_code:${car_code.count()}")
    // 人和码
    val person_code = cameraData.cartesian(data)
      .map(v => {
        val id1 = v._1._1
        val jd1 = v._1._2._1
        val wd1 = v._1._2._2
        val id2 = v._2._1
        val jd2 = v._2._2._1
        val wd2 = v._2._2._2
        val dis = distance(jd1, wd1,jd2, wd2)
        if(dis <= maxDis){
          (id1, (id2, dis))
        }else{
          null
        }
      })
      .filter(_ != null)
      .groupByKey()
      .flatMap(v=>{
        val id1 = v._1
        v._2.toList.sortBy(v=> v._2).take(maxCountBC.value)
          .flatMap(r=> {
            Array[Row](
              Row.fromTuple(id1, PERSON, r._1, PHONE),
              Row.fromTuple(id1, PERSON, r._1, IMEI),
              Row.fromTuple(id1, PERSON, r._1, IMSI),
              Row.fromTuple(id1, PERSON, r._1, MAC)
            )
          })
      })
    logger.info(s"person_code:${person_code.count()}")
    car_code.union(person_code)
  }

  def insertData(ss: SparkSession,
                 rowRdd: RDD[Row],
                 tableName: String,
                 databaseTypeBc: Broadcast[String],
                 numPartitionsBc: Broadcast[Int])
  : Unit = {
    if(rowRdd == null){
      return
    }

    // (ID1, ID1类型, ID2, ID2类型, 支持度, 次数, 首次关联时间, 首次关联设备ID, 最近关联时间, 最近关联设备ID)
    val params = "DEVICE_ID1,DEVICE_TYPE1,DEVICE_ID2,DEVICE_TYPE2"
    val schema = StructType(List(
      StructField("DEVICE_ID1", StringType, nullable = true),
      StructField("DEVICE_TYPE1", StringType, nullable = true),
      StructField("DEVICE_ID2", StringType, nullable = true),
      StructField("DEVICE_TYPE2", StringType, nullable = true)
    ))

    //转换成Row
    val df = ss.createDataFrame(rowRdd, schema)

    val database = DataBaseFactory(ss, new Properties(), databaseTypeBc.value)
    val saveParamBean = new SaveParamBean(tableName, Constant.OVERRIDE, database.getDataBaseBean)
    saveParamBean.dataBaseBean =  database.getDataBaseBean
    saveParamBean.params = params
    saveParamBean.numPartitions = numPartitionsBc.value
    database.save(saveParamBean, df.coalesce(numPartitionsBc.value))
  }

  def process(param: Param): Unit = {
    val ss: SparkSession = SparkUtil.getSparkSession(param.master, "DeviceRelations", param)

    val way = param.keyMap.getOrElse("getWay", "1").toString.toInt // 获取方式：1、根据资源管理系统中设备关联关系获取，2、根据设备经纬度计算设备关联关系

    val md_device_mgr = param.keyMap.getOrElse("dataBaseDevice", "md_device_mgr")

    val properties = new Properties()
    val mysql_url = s"jdbc:mysql://${param.mysqlIp}:${param.mysqlPort}/${md_device_mgr}?characterEncoding=UTF-8&autoReconnect=true&useSSL=false&zeroDateTimeBehavior=convertToNull&serverTimezone=UTC"
    properties.setProperty("jdbc.url", mysql_url)
    properties.setProperty("username", param.mysqlUserName)
    properties.setProperty("password", param.mysqlPassWord)
    properties.put("driver", Constant.MYSQL_DRIVER)
    logger.info(mysql_url)

    val dataBase = DataBaseFactory(ss, properties, "mysql")
    val ds = DruidUtil.getDruidDataSource(dataBase.getDataBaseBean)
    val databaseTypeBc = ss.sparkContext.broadcast(param.keyMap.getOrElse("databaseType", "hive").toString)
    val numPartitionsBc = ss.sparkContext.broadcast(param.keyMap.getOrElse("numPartitions", "10").toString.toInt)

    val ret = if(1 == way) {
                getRelations1(ss, param, properties, ds)
              } else {
                getRelations2(ss, param, properties, ds)
              }

    insertData(ss, ret, "TB_DEVICE_ROUND", databaseTypeBc, numPartitionsBc)
  }

  def main(args: Array[String]): Unit = {

    if (args == null || args.length < 0) throw new Exception("请求参数有误，请求参数依次为:任务json")
    var param: Param = null
    try {
      val jsonStr = args(0) //任务执行json
      param = new Param(jsonStr)
      val mainClass = param.keyMap("mainClass").toString
      logger.info(s"mainClass: $mainClass")
      ConstantUtil.init(param)
      DeviceRelations.process(param)
    } catch {
      case ex: Exception =>
        logger.error("任务执行异常：" + ex.getMessage, ex)
        ex.printStackTrace()
    }

  }

}
