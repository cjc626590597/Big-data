package com.suntek.algorithm.process.distribution

import com.suntek.algorithm.common.bean.{QueryBean, SaveParamBean}
import com.suntek.algorithm.common.conf.{Constant, Param}
import com.suntek.algorithm.common.database.{DataBase, DataBaseFactory}
import com.suntek.algorithm.common.util.SparkUtil
import com.suntek.algorithm.process.lcss.ObjectHourHandler
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import scala.collection.mutable.ArrayBuffer

/**
  * 对象分布计算(按小时统计)
  * @author zhy
  * 2020-10-13 14:21
  */
object Distribution {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val tableName1 = "DM_RELATION_DISTRIBUTE_DETAIL"
  val tableName2 = "DM_RELATION_DISTRIBUTE_STAT"
  val DATA_SQL_A = "data_sql_template_1"
  val DATA_SQL_B = "data_sql_template_2"
  val DEVICE_REL_SQL = "device_round_sql_template"
  val CATEGORY1 = "category1"
  val CATEGORY2 = "category2"
  val TIMESERIESTHRESHOLD = "timeSeriesThreshold"
  val SECONDSSERIESTHRESHOLD = "secondsSeriesThreshold"
  def process(param: Param): Unit =
  {
    var category1 = param.releTypes(0)
    var category2 = param.releTypes(1)
    if(Constant.PRIORITY_LEVEL(category1) > Constant.PRIORITY_LEVEL(category2)){
      category1 = param.releTypes(1)
      category2 = param.releTypes(0)
    }
    val batchId = param.keyMap("batchId").toString
    val ss: SparkSession = SparkUtil.getSparkSession(param.master, s"${category1}_${category2}_DisHour_$batchId", param)
    param.keyMap.put(DATA_SQL_A, s"$category1.load.hour")
    param.keyMap.put(CATEGORY1, category1)
    param.keyMap.put(CATEGORY2, category2)
    param.keyMap.put("dateFormat", "yyyyMMddHHmmss")
    param.keyMap.put("isDiffCategory", "false")
    if(!category1.equals(category2)){
      param.keyMap.put("isDiffCategory", "true")
      param.keyMap.put(DATA_SQL_B, s"$category2.load.hour")
      param.keyMap.put(DEVICE_REL_SQL, s"$category1.$category2.device")
    }
    //注册UDF：isValidEntity
    registerUDF(ss, "isValidEntity", "com.suntek.algorithm.udf.EntityCheckUDF" )
    distribution(ss, param)
    ss.close()
  }

  def distribution(ss: SparkSession,
                   param: Param
                  ): Unit = {
    val isDiffCategory =  param.keyMap("isDiffCategory").toString.toBoolean
    genTime(param)
//    val startDateTime = param.keyMap("startDateTime").toString
//    val endDateTime = param.keyMap("endDateTime").toString
//    val startDate = param.keyMap("statDate").toString
//    val hour = param.keyMap("hour").toString
    val startDateTime = "20210518120000"
    val endDateTime = "20210518130000"
    val startDate = "2021051800"
    val hour = "12"

    // 获取设备与设备的关系
    val retDeviceRound = loadDeviceRound(ss, param) // (设备ID1, 设备ID2)
    val retDeviceRoundBC = ss.sparkContext.broadcast(retDeviceRound)
    // 获取数据结果1
    val data_sql_1 = param.keyMap(param.keyMap(DATA_SQL_A).toString).toString
      .replaceAll("@endDateTime@", endDateTime.substring(2, 14))
      .replaceAll("@startDateTime@", startDateTime.substring(2, 14))
      .replaceAll("@startDate@", startDate.substring(2, 8))
      .replaceAll("@hour@", hour)
    logger.info(data_sql_1)
    val ret1 = loadData(ss, data_sql_1, retDeviceRoundBC, param) // (关联设备, (id, 时间戳, 设备))
    val array0 = ret1.collect()

    val t1 = ret1.map(v=> {(v._2._1, v._1, v._2._2)}).map(v=>(v._1, 1)).reduceByKey(_+_).collect().toMap
    val ret1CountBC = ss.sparkContext.broadcast(t1)

    // 获取数据结果2
    var ret2CountBC = ret1CountBC
    var ret2 = ret1
    if (isDiffCategory){
      val data_sql_2 = param.keyMap(param.keyMap(DATA_SQL_B).toString).toString
        .replaceAll("@endDateTime@", endDateTime.substring(2, 14))
        .replaceAll("@startDateTime@", startDateTime.substring(2, 14))
        .replaceAll("@startDate@", startDate.substring(2, 8))
        .replaceAll("@hour@", hour)
      logger.info(data_sql_2)
      ret2 = loadData(ss, data_sql_2, retDeviceRoundBC, param, 2) // (关联设备, (id, 时间戳, 设备))
      val t2 = ret2.map(v=> {(v._2._1, v._1, v._2._2)}).map(v=>(v._1, 1)).reduceByKey(_+_).collect().toMap
      ret2CountBC = ss.sparkContext.broadcast(t2)
    }

    // 计算分布详情
    val retDetail = genDetectionDis(ss, ret1, ret2, param, isDiffCategory, retDeviceRoundBC) //设备，对象A，对象A类型，对象B，对象B类型, 对象A时间戳, 对象B时间戳, 时间差, 入库时间, 对象A InfoId, 对象B InfoId
    val array1 = retDetail.collect()
//    insertDataDetail(ss, retDetail, param, tableName1)

    // 计算分布
    // 设备，对象A，对象A类型，对象B，对象B类型, 对象A时间戳, 对象B时间戳, 时间差, 入库时间
    val dateFormat = new SimpleDateFormat(param.keyMap("dateFormat").toString)
    val nowBC = ss.sparkContext.broadcast(dateFormat.format(new Date()))
    val ret = retDetail.map(v => ((v._1, v._2, v._3, v._4, v._5), 1)) // key = (设备，对象A，对象A类型，对象B，对象B类型)
      .reduceByKey(_ + _)
      .map(v=>{
        val total1 = ret1CountBC.value.getOrElse(v._1._2, 0)
        val total2 = ret2CountBC.value.getOrElse(v._1._4, 0)
        //(设备，对象A，对象A类型，对象B，对象B类型, 关联总次数, 对象A次数, 对象B次数, 格式)
        (v._1._1, v._1._2, v._1._3,  v._1._4,  v._1._5, v._2, total1, total2, nowBC.value)
      })
    val array2 = ret.collect()

//    insertData(ss, ret, param, tableName2)

    // 个别算法，按小时有特殊处理
    val algorithmTypes = param.keyMap.getOrElse("algorithmType", "").toString.split(",")
    if(-1 != algorithmTypes.indexOf("lcss")){
      ObjectHourHandler.combineObject(ss, param, isDiffCategory, ret1, ret2)
    }
  }

  def genTime(param: Param): Unit = {
    val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
    val banchId = s"${param.keyMap("batchId").toString.substring(0, 10)}0000"
    val prevHourNum = Integer.parseInt(param.keyMap.getOrElse("prevHourNums", "2").toString)
    val startTimeStamp = sdf.parse(banchId).getTime - prevHourNum * 60 * 60 * 1000L
    val endTimeStamp = startTimeStamp + 60 * 60 * 1000L
    val startDateTime = sdf.format(startTimeStamp)
    val endDateTime = sdf.format(endTimeStamp)
    param.keyMap.put("startDateTime", startDateTime)
    param.keyMap.put("endDateTime", endDateTime)
    param.keyMap.put("statDate", startDateTime.substring(0, 10))
    param.keyMap.put("hour", startDateTime.substring(8, 10))
  }

  def loadDeviceRound(ss: SparkSession,
                      param: Param)
  : DeviceGroupS = {
    if(!param.keyMap.contains(DEVICE_REL_SQL)){
      return  new DeviceGroupS()
    }
    val sql = param.keyMap(param.keyMap(DEVICE_REL_SQL).toString).toString
    val databaseType = param.keyMap.getOrElse("databaseType","hive").toString
    val dataBase = DataBaseFactory(ss, new Properties(), databaseType)
    val rows = dataBase.query(sql, new QueryBean()).rdd

    val ret = dataBase.query(sql, new QueryBean())
      .rdd
      .map(r=> {
        try{
          val device_id1 = r.get(0).toString
          val device_id2 = r.get(1).toString
          (device_id1, device_id2) //(设备ID1, 设备ID2)
        } catch{
          case _: Exception => null
        }
      })
      .filter(v=> v!= null)
      .collect()
    val deviceGroupS = new DeviceGroupS()
    ret.foreach(v=>{
      deviceGroupS.addDevice1Device2(v._1, v._2)
    })
    logger.info(deviceGroupS.toString)
    deviceGroupS
  }

  def loadData(ss: SparkSession,
               sql: String,
               retDeviceRoundBC: Broadcast[DeviceGroupS],
               param: Param,
               types: Int = 1)
  : RDD[(String, (String, Long, String))] = {
    val queryParam = new QueryBean()
    val databaseType = param.keyMap.getOrElse("databaseType","hive").toString
    val dataBase = DataBaseFactory(ss, new Properties(), databaseType)
    val timeSeriesThreshold = param.keyMap.getOrElse(TIMESERIESTHRESHOLD, "300").toString.toLong
    val timeSeriesThresholdBC = ss.sparkContext.broadcast(timeSeriesThreshold)
    val dateFormat = ss.sparkContext.broadcast(param.keyMap("dateFormat").toString)
    val sql1 = "SELECT * FROM CAR_DETECT_INFO WHERE stat_day=210518 AND hour=12 AND JGSK>= 210518120000 AND JGSK< 210518130000 AND HPHM is not null AND length(HPHM) > 3 AND isValidEntity(HPHM,'CAR')"
    dataBase.query(sql1, queryParam).show()
      //[210518122112,蒙DC3VN8-0,440118626491017001,745598846080049753]
//SELECT DISTINCT JGSK, CONCAT(HPHM,'-',HPYS) AS HPHM, TOLLGATE_ID,INFO_ID FROM CAR_DETECT_INFO WHERE stat_day=210815 AND hour=22 AND JGSK>= 210815220000 AND JGSK< 210815230000 AND HPHM is not null AND length(HPHM) > 3 AND isValidEntity(HPHM,'CAR')
    dataBase.query(sql, queryParam)
      .rdd
      .map(v=>{
        try {
        val time = s"20${v.get(0).toString}"
        val id = v.get(1).toString.toUpperCase
        val deviceId = v.get(2).toString
        val infoId = v.get(3).toString
        val timeStamp = DateUtils.parseDate(time, dateFormat.value).getTime / 1000L
        var deviceIdR = deviceId
        if(retDeviceRoundBC.value.deviceGroups.nonEmpty){
          if(types == 2){
            deviceIdR = retDeviceRoundBC.value.find2(deviceIdR)
          }else{
            deviceIdR = retDeviceRoundBC.value.find1(deviceIdR)
          }
        }
        ((id, deviceIdR), (timeStamp, infoId))
        }catch {
          case _: Exception =>
            (("", ""), (0L, ""))
        }
      })
      .filter(v=> v._1._2.nonEmpty)
      .combineByKey(
        createCombiner =  (v: (Long, String)) => List[(Long, String)](v),
        mergeValue = (c: List[(Long, String)], v: (Long, String)) => c:+ v,
        mergeCombiners = (list1: List[(Long, String)], list2: List[(Long, String)]) => list1 ::: list2)
      .map { r=>
        val key = r._1
        val ret = ArrayBuffer[ArrayBuffer[(Long, String)]]()
        var a = ArrayBuffer[(Long, String)]()
        val list: List[(Long, String)] = r._2.sortWith(_._1 < _._1)
        a.append(list.head)
        for(i <- 0 until list.size - 1){
          val j = i + 1
          val t = list(j)._1 - list(i)._1
          if(t > timeSeriesThresholdBC.value){
            ret.append(a)
            a = ArrayBuffer[(Long, String)]()
          }
          a.append(list(j))
        }
        if(a.nonEmpty) {
          ret.append(a)
        }
        (key, ret.map(r=> (r.map(_._1).sum / r.size, r.head._2)).toList)
      }
      .flatMap(v=>{
        val id = v._1._1
        val deviceId =  v._1._2
        v._2.map(r=> {
          (deviceId, (id, r._1, r._2))
        })
      })
      .distinct()
      .persist()
  }

  def insertDataDetail(ss: SparkSession,
                       ret: RDD[(String, String, String, String, String, Long, Long, Long, String, String, String)],
                       param: Param,
                       tableName: String)
  : Unit = {
    if(ret == null){
      return
    }
    val databaseType = param.keyMap.getOrElse("databaseType","hive").toString
    val params = "DEVICE_ID,OBJECT_ID_A,TYPE_A,OBJECT_ID_B,TYPE_B,TIMESTAMP_A,TIMESTAMP_B,SUB_TIME,ADD_TIME,INFO_A,INFO_B"
    val partitions = Array[(String, String)](
      ("STAT_DATE",param.keyMap("statDate").toString),
      ("TYPE",s"${param.keyMap(CATEGORY1).toString}-${param.keyMap(CATEGORY2).toString}")
    )
    val schema = StructType(List(
      StructField("DEVICE_ID", StringType, nullable = true),
      StructField("OBJECT_ID_A", StringType, nullable = true),
      StructField("TYPE_A", StringType, nullable = true),
      StructField("OBJECT_ID_B", StringType, nullable = true),
      StructField("TYPE_B", StringType, nullable = true),
      StructField("TIMESTAMP_A", LongType, nullable = true),
      StructField("TIMESTAMP_B", LongType, nullable = true),
      StructField("SUB_TIME", LongType, nullable = true),
      StructField("ADD_TIME", StringType, nullable = true),
      StructField("INFO_A", StringType, nullable = true),
      StructField("INFO_B", StringType, nullable = true)
    ))

    //转换成Row
    val rowRdd = ret.map(r => Row.fromTuple(r))
    val df = ss.createDataFrame(rowRdd, schema)
    df.show()
    val database = DataBaseFactory(ss, new Properties(), databaseType)
    val saveParamBean = Distribution.genSaveParamBean(database, partitions, tableName, params, param.keyMap.getOrElse("dataBaseName", "default").toString)
    saveParamBean.numPartitions = param.keyMap.getOrElse("numPartitions", "10").toString.toInt
    database.save(saveParamBean, df)
  }

  def insertData(ss: SparkSession,
                 ret: RDD[(String, String, String, String, String, Int, Int, Int, String)],
                 param: Param,
                 tableName: String)
  : Unit = {
    if(ret == null){
      return
    }
    val databaseType = param.keyMap.getOrElse("databaseType","hive").toString
    val params = " DEVICE_ID,OBJECT_ID_A,TYPE_A,OBJECT_ID_B,TYPE_B,TOTAL,TOTAL_A,TOTAL_B,ADD_TIME"
    val partitions = Array[(String, String)](
      ("STAT_DATE",param.keyMap("statDate").toString),
      ("TYPE",s"${param.keyMap(CATEGORY1).toString}-${param.keyMap(CATEGORY2).toString}")
    )
    val schema = StructType(List(
      StructField("DEVICE_ID", StringType, nullable = true),
      StructField("OBJECT_ID_A", StringType, nullable = true),
      StructField("TYPE_A", StringType, nullable = true),
      StructField("OBJECT_ID_B", StringType, nullable = true),
      StructField("TYPE_B", StringType, nullable = true),
      StructField("TOTAL", IntegerType, nullable = true),
      StructField("TOTAL_A", IntegerType, nullable = true),
      StructField("TOTAL_B", IntegerType, nullable = true),
      StructField("ADD_TIME", StringType, nullable = true)
    ))

    //转换成Row
    val rowRdd = ret.map(r => Row.fromTuple(r))
    val df = ss.createDataFrame(rowRdd, schema)
    df.show()
    val database = DataBaseFactory(ss, new Properties(), databaseType)
    val saveParamBean = Distribution.genSaveParamBean(database, partitions, tableName, params, param.keyMap.getOrElse("dataBaseName", "default").toString)
    saveParamBean.numPartitions = param.keyMap.getOrElse("numPartitions", "10").toString.toInt
    database.save(saveParamBean, df)
  }

  def genDetectionDis(ss: SparkSession,
                      ret1: RDD[(String, (String, Long, String))],
                      ret2: RDD[(String, (String, Long, String))],
                      param: Param,
                      isDiffCategory: Boolean,
                      retDeviceRoundBC: Broadcast[DeviceGroupS]
                     )
  : RDD[(String, String, String, String, String, Long, Long, Long, String, String, String)] = {
    // ret1：(关联设备, (id1, 时间戳，设备))
    // ret2：(关联设备, (id2, 时间戳，设备))
    val secondsSeriesThreshold = param.keyMap.getOrElse(SECONDSSERIESTHRESHOLD, "60").toString.toLong
    val secondsSeriesThresholdBC = ss.sparkContext.broadcast(secondsSeriesThreshold)
    val category1 = param.keyMap(CATEGORY1).toString
    val category2 = param.keyMap(CATEGORY2).toString

    val category1Level = ss.sparkContext.broadcast(Constant.PRIORITY_LEVEL(category1))
    val category2Level = ss.sparkContext.broadcast(Constant.PRIORITY_LEVEL(category2))
    val dateFormat = new SimpleDateFormat(param.keyMap("dateFormat").toString)
    val nowBC = ss.sparkContext.broadcast(dateFormat.format(new Date()))
    val ret = if(isDiffCategory){
      ret1.join(ret2)
          .map{ v=>
            try{
              val idTimeStamp2 = v._2._2._2
              val idTimeStamp1 = v._2._1._2
              //  infoId
              val infoIdA = v._2._1._3
              val infoIdB = v._2._2._3
              if(idTimeStamp1 - idTimeStamp2 < secondsSeriesThresholdBC.value
                && idTimeStamp1 - idTimeStamp2 > secondsSeriesThresholdBC.value * -1){
                //(设备，id1，id2, 时间戳)
               (( v._1, v._2._1._1, v._2._2._1,  idTimeStamp2), ((idTimeStamp1, 1), (infoIdA, infoIdB)))
              }else{
                (("", "", "", 0L), ((0L, 0), ("", "")))
              }
            }catch{
              case _: Exception=> (("", "", "", 0L), ((0L, 0), ("", "")))
            }
          }
    }else{
      ret1.join(ret2)
        // (关联设备, ((id1, 时间戳，设备), (id2, 时间戳，设备)))  (关联设备, ((id3, 时间戳，设备), (id4, 时间戳，设备)))
        .filter(r => r._2._1._1 != r._2._2._1) // 过滤id1 = id2 数据
        .map(v=> {
          try{
            val idTimeStamp1 = v._2._1._2
            val idTimeStamp2 = v._2._2._2
            //  (设备Id, infoId)
            val infoIdA = v._2._1._3
            val infoIdB = v._2._2._3
            if(idTimeStamp1 - idTimeStamp2 < secondsSeriesThresholdBC.value
              && idTimeStamp1 - idTimeStamp2 > secondsSeriesThresholdBC.value * -1){
              //(设备，id1，id2, 时间戳)
              val (d1, d2, t1, t2) = neaten(v._2._1._1, v._2._2._1, idTimeStamp1, idTimeStamp2)
              if(v._2._2._1 < v._2._1._1) {
                ((v._1, d1, d2,  t2), ((t1, 1), (infoIdB, infoIdA)))
              } else {
                ((v._1, d1, d2,  t2), ((t1, 1), (infoIdA, infoIdB)))
              }
            }else{
              (("", "", "", 0L), ((0L, 0), ("", "")))
            }
          }catch{
            case _: Exception=> (("", "", "", 0L), ((0L, 0), ("", "")))
          }
        })
    }
    ret.filter(v=> v._1._1.nonEmpty)
        //((deviceid, d1, d2,  t2), ((t1, 1), (infoIdA, infoIdB)))
        .reduceByKey ((x, y) => ((x._1._1 + y._1._1, x._1._2 + y._1._2), x._2))
        .map{
          case ((deviceValue, d1, d2,  t2), ((timeSum, counts), (infoIdA, infoIdB))) =>
          val timeStamp1 = timeSum / counts
          //设备，对象A，对象A类型，对象B，对象B类型, 对象A时间戳, 对象B时间戳, 时间差, 入库时间, 对象A设备Id,对象AInfoId,对象B设备Id,对象B InfoId,
          var deviceId = deviceValue
          if(retDeviceRoundBC.value.deviceGroups.contains((deviceValue))){
            deviceId = retDeviceRoundBC.value.deviceGroups(deviceValue).deviceList1.head
          }
          (deviceId, d1, category1Level.value.toString, d2, category2Level.value.toString, timeStamp1, t2, math.abs(timeStamp1-t2),
            nowBC.value, infoIdA, infoIdB)
        }
        .persist(StorageLevel.MEMORY_AND_DISK)
  }


  /**
   * 数据整理：小的在前，大的在后
   *
   * */
  def neaten(data : (String, String, Long, Long)): (String, String, Long, Long) ={
    if(data._2 < data._1) {
      (data._2 , data._1, data._4, data._3)
    } else {
      data
    }
  }
  def genSaveParamBean(database: DataBase,
                       partitions: Array[(String, String)],
                       tableName: String,
                       params: String,
                       dataBaseName: String = "default"
                       )
  : SaveParamBean = {
    val saveParamBean = new SaveParamBean(tableName, Constant.OVERRIDE_DYNAMIC, database.getDataBaseBean)
    saveParamBean.dataBaseBean = database.getDataBaseBean
    saveParamBean.params = params
    saveParamBean.dataBaseBean.dataBaseName = dataBaseName
    saveParamBean.outputType = Constant.OVERRIDE_DYNAMIC
    saveParamBean.partitions = partitions
    saveParamBean
  }

  /**
   * 注册自定义函数
   * @param funcName
   * @param funClassFullName
   * @return
   */
  def registerUDF(sparkSession:SparkSession, funcName: String, funClassFullName: String) = {

    sparkSession.sql(s"CREATE TEMPORARY FUNCTION ${funcName} as '${funClassFullName}'")
  }
}