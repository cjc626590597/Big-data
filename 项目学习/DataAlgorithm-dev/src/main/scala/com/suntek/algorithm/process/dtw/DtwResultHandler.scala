package com.suntek.algorithm.process.dtw

import java.text.SimpleDateFormat
import java.util.{Properties, TimeZone}
import com.suntek.algorithm.common.bean.{QueryBean, SaveParamBean}
import com.suntek.algorithm.common.conf.{Constant, Param}
import com.suntek.algorithm.common.database.DataBaseFactory
import com.suntek.algorithm.common.util.SparkUtil
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}


/**
  * @author zhy
  * @date 2020-11-23 9:28
  */
object DtwResultHandler {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val CATEGORY1 = "category1"
  val CATEGORY2 = "category2"
  val DTW_LOAD_DATA = "dtw.all.result"
  val DTW_OBJECT_DAYS = "dtw.object.days"
  val tableName = "DM_DTW"

  def process(param: Param): Unit = {
    var category1 = param.releTypes(0)
    var category2 = param.releTypes(1)
    if (Constant.PRIORITY_LEVEL(category1) > Constant.PRIORITY_LEVEL(category2)) {
      category1 = param.releTypes(1)
      category2 = param.releTypes(0)
    }
    var isDiffCategoryFlag = false
    if(!category1.equals(category2)){
      isDiffCategoryFlag = true
    }
    val batchId = param.keyMap("batchId").toString.substring(0, 8)
    val days = param.keyMap.getOrElse("days", "180").toString
    val weight = param.keyMap.getOrElse("weight", "0.5").toString.toDouble
    val ss: SparkSession = SparkUtil.getSparkSession(param.master, s"${category1}_${category2}_DtwResultHandler_${batchId}", param)
    param.keyMap.put(CATEGORY1, category1)
    param.keyMap.put(CATEGORY2, category2)
    val sdf = new SimpleDateFormat("yyyyMMdd")
    sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    val runTimeStamp = sdf.parse(batchId).getTime -  60 * 60 * 1000L
    val state_date = sdf.format(runTimeStamp)
    val pre_180Date = sdf.format(sdf.parse(state_date).getTime -  days.toInt * 24 * 3600 * 1000L)

    val sql = param.keyMap(DTW_OBJECT_DAYS).toString
      .replaceAll("@type@", s"$category1-$category2")
      .replaceAll("@startDate@", s"${pre_180Date}00")
      .replaceAll("@endDate@", s"${state_date}23")
    val objectDays = loadObjectDays(ss, param, sql)

    val weightBC = ss.sparkContext.broadcast(weight)

    val sql2 = param.keyMap(DTW_LOAD_DATA).toString
      .replaceAll("@type@", s"$category1-$category2")
      .replaceAll("@startDate@", s"$pre_180Date")
      .replaceAll("@endDate@", s"$state_date")
    val rowRdd = loadData(ss, param, sql2)

    val rowRdd1 = rowRdd.map(v=> ((v._1._1, v._1._2), (v._2, (v._1._3, v._1._4)) )) // ((KEYA),((VALUE),(KEYB)))
      .join(objectDays)
      .map(v=>((v._1, v._2._1._2), (v._2._1._1, v._2._2)))// ((KEYA, KEYB), ((VALUE, DAYSA)))

    val rowRdd2 = rowRdd
      .map(v=> ((v._1._3, v._1._4), (v._1._1, v._1._2))) // (KEYB, KEYA)
      .join(objectDays)
      .map(v=> ((v._2._1, v._1), v._2._2)) //( (KEYA, KEYB),DAYSB)

    val ret = rowRdd1 // ((KEYA, KEYB), ((VALUE, DAYSA)))
      .join(rowRdd2)//( (KEYA, KEYB),DAYSB)
      .map(v=>{
      val keyA = v._1._1
      val keyB = v._1._2
      val daysA = v._2._1._2
      val daysB = v._2._2
      val value = v._2._1._1
      val nums = value._7
      val score = value._1
      val scoreF =  weightBC.value * (nums * 1.0) / daysA * score +  (1 - weightBC.value) *  (nums * 1.0) / daysB * score
      Row.fromTuple((keyA._1, keyA._2, keyB._1, keyB._2, value._2, scoreF, value._3, value._4, value._5, value._6, nums.toLong))
    })
    insertDataDetail(ss, ret, tableName, param, state_date)
    ss.close()
  }

  def loadObjectDays(ss: SparkSession,
                     param: Param,
                     sql: String): RDD[((String, String), Int)]  = {
    val databaseType = param.keyMap("databaseType").toString
    val dataBase = DataBaseFactory(ss, new Properties(), databaseType)
    dataBase.query(sql, new QueryBean())
      .rdd
      .map(v=>{
        val objectId = v.get(0).toString
        val objectType= v.get(1).toString
        val days = v.get(2).toString.toInt
        ((objectId, objectType), days)
      })
      .persist(StorageLevel.MEMORY_AND_DISK)
  }


  def loadDataMinDate(ss: SparkSession,
                      param: Param,
                      sql: String)
  : String  = {
    val databaseType = param.keyMap("databaseType").toString
    val dataBase = DataBaseFactory(ss, new Properties(), databaseType)
    var stat_date = ""
    val rdd = dataBase.query(sql, new QueryBean())
    if(rdd != null && rdd.count() > 0){
      val rdd1 = rdd
        .rdd
        .map(r=> {
          try{
            r.get(0).toString
          }catch {
            case _: Exception => ""
          }
        })
        .filter(_ != "")
      if(rdd1.count() > 0){
        stat_date = rdd1.collect().head
      }
    }
    stat_date
  }

  def insertDataDetail(ss: SparkSession,
                       rowRdd: RDD[Row],
                       tableName: String,
                       param: Param,
                       statDate: String)
  : Unit = {
    if(rowRdd == null){
      return
    }
    val category1 = param.keyMap(CATEGORY1).toString
    val category2 = param.keyMap(CATEGORY2).toString
    val databaseType = param.keyMap("databaseType").toString
    // (ID1, ID1类型, ID2, ID2类型, 支持度, 次数, 首次关联时间, 首次关联设备ID, 最近关联时间, 最近关联设备ID)
    val params = "OBJECT_ID_A,TYPE_A,OBJECT_ID_B,TYPE_B,TIMES,SCORE,FIRST_TIME,FIRST_DEVICE,LAST_TIME,LAST_DEVICE,DAYS"
    val schema = StructType(List(
      StructField("OBJECT_ID_A", StringType, nullable = true),
      StructField("TYPE_A", StringType, nullable = true),
      StructField("OBJECT_ID_B", StringType, nullable = true),
      StructField("TYPE_B", StringType, nullable = true),
      StructField("TIMES", LongType, nullable = true),
      StructField("SCORE", DoubleType, nullable = true),
      StructField("FIRST_TIME", LongType, nullable = true),
      StructField("FIRST_DEVICE", StringType, nullable = true),
      StructField("LAST_TIME", LongType, nullable = true),
      StructField("LAST_DEVICE", StringType, nullable = true),
      StructField("DAYS", LongType, nullable = true)
    ))

    //转换成Row
    val df = ss.createDataFrame(rowRdd, schema)

    val database = DataBaseFactory(ss, new Properties(), databaseType)
    val saveParamBean = new SaveParamBean(tableName, Constant.OVERRIDE_DYNAMIC, database.getDataBaseBean)
    saveParamBean.dataBaseBean =  database.getDataBaseBean
    saveParamBean.params = params
    saveParamBean.numPartitions = param.keyMap.getOrElse("numPartitions", "10").toString.toInt
    saveParamBean.partitions = Array[(String, String)](("STAT_DATE",s"$statDate"), ("TYPE", s"$category1-$category2"), ("REL_TYPE",s"${param.relationType}"))
    database.save(saveParamBean, df.coalesce(param.keyMap.getOrElse("numPartitions", "10").toString.toInt))
  }

  def loadData(ss: SparkSession,
               param: Param,
               sql: String
              )
  : RDD[((String, String, String, String),(Double, Long, Long, String, Long, String, Int))] = {
    val databaseType = param.keyMap("databaseType").toString
    val dataBase = DataBaseFactory(ss, new Properties(), databaseType)
    val minImpactCount = param.keyMap.getOrElse("minImpactCount", "3").toString.toInt
    logger.info(s"minImpactCount: ${minImpactCount}")
   dataBase.query(sql, new QueryBean())
      .rdd
      .map(r=> {
        // OBJECT_ID_A, TYPE_A, OBJECT_ID_B, TYPE_B, TIMES, SCORE, FIRST_TIME, FIRST_DEVICE, LAST_TIME, LAST_DEVICE
        val stateDate = r.get(0).toString.toUpperCase
        val id1 = r.get(1).toString.toUpperCase
        val id1_type = r.get(2).toString
        val id2 = r.get(3).toString.toUpperCase
        val id2_type = r.get(4).toString
        val times = r.get(5).toString.toLong
        val score = r.get(6).toString.toDouble
        val first_time = r.get(7).toString.toLong
        val first_deviceId = r.get(8).toString
        val last_time = r.get(9).toString.toLong
        val last_deviceId = r.get(10).toString
        //((id1，id1_type, id2, id2_type), (支持度，支持次数，最早设备id，最早时间， 最晚设备id，最晚时间))
        ((id1, id1_type, id2, id2_type), (score, times, first_time, first_deviceId, last_time, last_deviceId, stateDate))
      })
      .combineByKey(
        createCombiner =
          (v: (Double, Long, Long, String, Long, String, String)) =>
            (v._1, v._2, v._3, v._4, v._5, v._6, 1),
        mergeValue =
          (c: (Double, Long, Long, String, Long, String, Int),
           v: (Double, Long, Long, String, Long, String, String)) => {
        val score = c._1 + v._1
        val times = c._2 + v._2
        val first= if (v._3 < c._3) (v._3, v._4) else (c._3, c._4)
        val last = if (v._5 > c._5) (v._5, v._6) else (c._5, c._6)
        (score, times, first._1, first._2, last._1, last._2, c._7 + 1)
      },
        mergeCombiners = (c1:(Double, Long, Long, String, Long, String, Int),
       c2: (Double, Long, Long, String, Long, String, Int)) => {
        val score = c1._1 + c2._1
        val times = c1._2 + c2._2
        val first= if (c1._3 < c2._3) (c1._3, c1._4) else (c2._3, c2._4)
        val last = if (c1._5 > c2._5) (c1._5, c1._6) else (c2._5, c2._6)
        (score, times, first._1, first._2, last._1, last._2, c1._7 + c2._7)
      }
     )
     .filter(v=> v._2._2 >= minImpactCount)
     .persist(StorageLevel.MEMORY_AND_DISK)
  }
}
