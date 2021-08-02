package com.suntek.algorithm.process.fptree

import java.text.SimpleDateFormat
import java.util.{Properties, TimeZone}

import com.suntek.algorithm.common.bean.{QueryBean, SaveParamBean}
import com.suntek.algorithm.common.conf.{Constant, Param}
import com.suntek.algorithm.common.database.DataBaseFactory
import com.suntek.algorithm.common.util.SparkUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

/**
  * @author zhy
  * @date 2020-11-20 12:04
  */
object FPTreeResultHandler {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val tableName1 = "DM_FPTREE"
  val tableName2 = "DM_ID_DISTRIBUTE_DAY"
  val FPTREE_MAX_DATE_SQL = "fptree.max.date"
  val FPTREE_AGG_SQL_1 = "fptree.agg.sql1"
  val FPTREE_AGG_SQL_2 = "fptree.agg.sql2"
  val FPTREE_AGG_SQL_3 = "fptree.agg.sql3"
  val FPTREE_REL_DIS_DAY_TOTAL = "fptree.rel.distribute.day.total"
  val FPTREE_REL_DIS_TOTAL = "fptree.rel.distribute.total"
  val CATEGORY1 = "category1"
  val CATEGORY2 = "category2"


  def process(param: Param): Unit = {
    var category1 = param.releTypes(0)
    var category2 = param.releTypes(1)
    if(Constant.PRIORITY_LEVEL(category1) > Constant.PRIORITY_LEVEL(category2)){
      category1 = param.releTypes(1)
      category2 = param.releTypes(0)
    }
    val batchId = param.keyMap("batchId").toString.substring(0, 8)
    val ss: SparkSession = SparkUtil.getSparkSession(param.master, s"${category1}_${category2}_FPTreeResultHandler_${batchId}", param)
    param.keyMap.put(CATEGORY1, category1)
    param.keyMap.put(CATEGORY2, category2)
    val sdf = new SimpleDateFormat("yyyyMMdd")
    sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    val aggDays = param.keyMap.getOrElse("aggDays", "7").toString.toInt
    val days = param.keyMap.getOrElse("days", "180").toString
    logger.info(s"aggDays=${aggDays}")
    val runTimeStamp = sdf.parse(batchId).getTime -  60 * 60 * 1000L
    val curDate = sdf.format(runTimeStamp)

    val sql1 = param.keyMap(FPTREE_MAX_DATE_SQL).toString
      .replaceAll("@type@", s"$category1-$category2")
      .replaceAll("@calType@", s"$aggDays")
      .replaceAll("@startDate@", s"$curDate")
    logger.info(sql1)
    var state_date = loadDataMaxDate(ss, param, sql1)

    val pre_180Date = if (state_date.nonEmpty){
      sdf.format(sdf.parse(state_date).getTime -  days.toInt * 24 * 3600 * 1000L)
    } else {
      sdf.format(sdf.parse(curDate).getTime -  days.toInt * 24 * 3600 * 1000L)
    }
    var sql2 = ""
    if(state_date == ""){
      state_date = sdf.format(sdf.parse(curDate).getTime -  aggDays * 24 * 3600 * 1000L)
      sql2 = param.keyMap(FPTREE_AGG_SQL_1).toString
        .replaceAll("@type@", s"$category1-$category2")
        .replaceAll("@startDate@", s"$state_date")
        .replaceAll("@endDate@", s"$curDate")
        .replaceAll("@calType@", s"$aggDays")
    }else if(state_date == curDate){
      sql2 = param.keyMap(FPTREE_AGG_SQL_2).toString
        .replaceAll("@type@", s"$category1-$category2")
        .replaceAll("@startDate@", s"$pre_180Date")
        .replaceAll("@endDate@", s"$state_date")
        .replaceAll("@calType@", s"$aggDays")
    }else{
      sql2 = param.keyMap(FPTREE_AGG_SQL_3).toString
        .replaceAll("@type@", s"$category1-$category2")
        .replaceAll("@pre180Date@", pre_180Date)
        .replaceAll("@startDate@", state_date)
        .replaceAll("@endDate@", curDate)
        .replaceAll("@calType@", s"$aggDays")
    }
    logger.info(sql2)
    val rddCount =  getIDCount(ss, param, curDate, pre_180Date, category1, category2)
    val dataRet = loadDataAgg(ss, param, sql2, rddCount)
    insertDataAgg(ss, dataRet, tableName1, param, curDate)
    ss.close()
  }

  def loadDataMaxDate(ss: SparkSession,
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

  def loadDataAgg(ss: SparkSession,
                  param: Param,
                  sql: String,
                  rddCount: RDD[((String, String), Double)]
                 )
  : RDD[Row] = {
    val databaseType = param.keyMap("databaseType").toString
    val dataBase = DataBaseFactory(ss, new Properties(), databaseType)
    val rdd1 = dataBase.query(sql, new QueryBean())
      .rdd
      .map(r=> {
        val id1 = r.get(0).toString.toUpperCase
        val id1_type = r.get(1).toString
        val id2 = r.get(2).toString.toUpperCase
        val id2_type = r.get(3).toString
        val support = r.get(4).toString.toDouble
        val times = r.get(5).toString.toLong
        val first_time = r.get(6).toString.toLong
        val first_deviceId = r.get(7).toString
        val last_time = r.get(8).toString.toLong
        val last_deviceId = r.get(9).toString
        val set_num = r.get(10).toString.toDouble.toInt
        //((id1，id1_type, id2, id2_type), (支持度，支持次数，最早设备id，最早时间， 最晚设备id，最晚时间, 集合数))
        // ((id1, id1_type, id2, id2_type), (support, times, first_time, first_deviceId, last_time, last_deviceId, set_num))
        ((id1, id1_type), ((id2, id2_type), (support, times, first_time, first_deviceId, last_time, last_deviceId, set_num)))
      })
    rdd1
      .join(rddCount)
      .map(v=>{
        val totalID1 = v._2._2
        val key1 = v._1
        val value = v._2._1._2
        val key2 = v._2._1._1
        ((key2), (value, totalID1, key1))
      })
      .join(rddCount)
      .map(v=> {
        val key2 = v._1
        val key1 = v._2._1._3
        val totalID1 = v._2._1._2
        val totalID2 = v._2._2
        val value = v._2._1._1
        ((key1._1, key1._2, key2._1, key2._2, totalID1, totalID2), value)
      })
      .groupByKey()
      .map(v=>{
        val key = v._1
        val valuesF = v._2.toList.sortBy(_._3)
        val head = valuesF.head
        val valuesL = v._2.toList.sortBy(_._5)
        val last = valuesL.last
        val times = valuesF.map(_._2).sum.toInt
        val n = valuesF.map(_._7).sum//.toInt
        val totalID1 = key._5
        val totalID2 = key._6
        //        val support = times * 1.0 / n * 1.0
        val confidence1 = (times * 1.0) / (totalID1 * 1.0)
        val confidence2 = (times * 1.0) / (totalID2 * 1.0)
        val confidence = 2 * 1.0 * (confidence1 * confidence2)/(confidence1 + confidence2)
        // (ID1, ID1类型, ID2, ID2类型, 支持度, 次数, 首次关联时间, 首次关联设备ID, 最近关联时间, 最近关联设备ID)
        Row.fromTuple(key._1, key._2, key._3, key._4, Integer.valueOf(times), confidence, head._3, head._4, last._5, last._6)
      })
  }

  def insertDataAgg(ss: SparkSession,
                    rowRdd: RDD[Row],
                    tableName: String,
                    param: Param,
                    curDate: String)
  : Unit = {
    if(rowRdd == null){
      return
    }
    val category1 = param.keyMap("category1").toString
    val category2 = param.keyMap("category2").toString
    val databaseType = param.keyMap("databaseType").toString
    // (ID1, ID1类型, ID2, ID2类型, 支持度, 次数, 首次关联时间, 首次关联设备ID, 最近关联时间, 最近关联设备ID)
    val params = "OBJECT_ID_A,TYPE_A,OBJECT_ID_B,TYPE_B,TIMES,SCORE,FIRST_TIME,FIRST_DEVICE,LAST_TIME,LAST_DEVICE"
    val schema = StructType(List(
      StructField("OBJECT_ID_A", StringType, nullable = true),
      StructField("TYPE_A", StringType, nullable = true),
      StructField("OBJECT_ID_B", StringType, nullable = true),
      StructField("TYPE_B", StringType, nullable = true),
      StructField("TIMES", IntegerType, nullable = true),
      StructField("SCORE", DoubleType, nullable = true),
      StructField("FIRST_TIME", LongType, nullable = true),
      StructField("FIRST_DEVICE", StringType, nullable = true),
      StructField("LAST_TIME", LongType, nullable = true),
      StructField("LAST_DEVICE", StringType, nullable = true)
    ))

    //转换成Row
    val df = ss.createDataFrame(rowRdd, schema)
    df.show()

    val database = DataBaseFactory(ss, new Properties(), databaseType)
    val saveParamBean = new SaveParamBean(tableName, Constant.OVERRIDE_DYNAMIC, database.getDataBaseBean)
    saveParamBean.dataBaseBean =  database.getDataBaseBean
    saveParamBean.params = params
    saveParamBean.partitions = Array[(String, String)](("STAT_DATE",s"$curDate"), ("TYPE", s"$category1-$category2"), ("REL_TYPE",s"${param.relationType}"))
    database.save(saveParamBean, df)
  }

  def getIDCount(ss: SparkSession,
                 param: Param,
                 curDate: String,
                 pre180Date: String,
                 category1: String,
                 category2: String)
  : RDD[((String, String), Double)] = {
 // : Broadcast[Map[(String, String), Double]] = {

    var sql = param.keyMap(FPTREE_REL_DIS_DAY_TOTAL).toString
      .replaceAll("@type@", s"$category1-$category2")
      .replaceAll("@category1@", category1)
      .replaceAll("@category2@", category2)
      .replaceAll("@startDate@", s"${curDate}00")
      .replaceAll("@endDate@", s"${curDate}23")

    logger.info(sql)
    saveDayData(ss, sql, param, category1, curDate)

    sql = param.keyMap(FPTREE_REL_DIS_TOTAL).toString
      .replaceAll("@type@", s"'${Constant.PRIORITY_LEVEL(category1)}','${Constant.PRIORITY_LEVEL(category2)}'")
      .replaceAll("@startDate@", pre180Date)
      .replaceAll("@endDate@", curDate)

    logger.info(sql)
    loadDataTotal(ss, param, sql)
    //val rddCount = loadDataTotal(ss, param, sql)
    //ss.sparkContext.broadcast(rddCount.collect().toMap)
  }

  def saveDayData(ss: SparkSession,
                  sql: String,
                  param: Param,
                  category: String,
                  curDate: String
                 ): Unit = {

    val retDetail = loadDataTotal(ss, param, sql)
      .map(v=> (v._1._1, v._2))

    if(retDetail == null){
      return
    }
    val databaseType = param.keyMap("databaseType").toString
    val params = "OBJECT_ID,TOTAL"
    val schema = StructType(List(
      StructField("OBJECT_ID", StringType, nullable = true),
      StructField("TOTAL", DoubleType, nullable = true)
    ))

    //转换成Row
    val rowRdd = retDetail.map(r => Row.fromTuple(r))
    val df = ss.createDataFrame(rowRdd, schema)
    df.show()

    val database = DataBaseFactory(ss, new Properties(), databaseType)
    val saveParamBean = new SaveParamBean(tableName2, Constant.OVERRIDE_DYNAMIC, database.getDataBaseBean)
    saveParamBean.dataBaseBean =  database.getDataBaseBean
    saveParamBean.params = params
    saveParamBean.numPartitions = param.keyMap.getOrElse("numPartitions", "10").toString.toInt
    saveParamBean.partitions = Array[(String, String)](("STAT_DATE",s"$curDate"), ("TYPE",s"${Constant.PRIORITY_LEVEL(category)}"))
    database.save(saveParamBean, df)
  }

  def loadDataTotal(ss: SparkSession,
                    param: Param,
                    sql: String)
  : RDD[((String, String), Double)] = {
    val databaseType = param.keyMap("databaseType").toString
    val dataBase = DataBaseFactory(ss, new Properties(), databaseType)
    dataBase.query(sql, new QueryBean())
      .rdd
      .map(r=>{
        val id = r.get(0).toString
        val id_type = r.get(1).toString
        val total = r.get(2).toString.toDouble
        ((id, id_type), total)
      })
  }
}
