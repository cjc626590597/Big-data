package com.suntek.algorithm.process.lcss

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime}
import com.suntek.algorithm.common.bean.{QueryBean, SaveParamBean, TableBean}
import com.suntek.algorithm.common.conf.{Constant, Param}
import com.suntek.algorithm.common.database.DataBaseFactory
import com.suntek.algorithm.common.util.{PropertiesUtil, SparkUtil}
import com.suntek.algorithm.process.util.ReplaceSqlUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import java.util.Properties


/**
  * Created with IntelliJ IDEA.
  * Author: yuanweilin
  * Date: 2020-9-2 10:03
  * Description:伴随/同行按天分析
  */
object LCSSDayHandler {
  val logger = LoggerFactory.getLogger(this.getClass)
  //model.sql文件中配置的加载数据的sql语句的key值
  var lcssDayLoadDetailSqlKey = ""

  var lcssDayLoadSeqSqlKey = ""

  //结果表名
  var resultTableName = ""

  var objectSeqTableName: String = ""

  var eventDate:String = null


  /***
    * 从过滤出来的数据集中找出在对象组中存在的对象,并计算该对象的序列长度
    * @param objectSeqRdd ：(对象，(对象的子序列长度，对象的子序列)）
    * @param lcsRdd ((对象A，对象A类型，对象B，对象B类型),(公共子序列长度, 公共子序列, 时间差之和, 最早关联时间，最早关联设备，最近关联时间,最近关联设备))
    * @return （对象ID, （序列长度，序列, 数据源））
    */
  def combineObjectSeq(ss: SparkSession, param: Param, objectSeqRdd :RDD[(String, (Int, String))],
                       lcsRdd: RDD[((String, Int, String, Int), (Int, String, Long, Long, String, Long, String))]  )
  : RDD[(String, (Int, String))] = {

    val objectIdRdd = lcsRdd
      .flatMap { m =>
        List[String](m._1._1, m._1._3)
      }.distinct()
//
    val objectIdRddCollect = objectIdRdd.collect()
    val objectIdRddBroadcast = ss.sparkContext.broadcast(objectIdRddCollect)
//
    val objectSeqCombineRdd = objectSeqRdd
      .filter(f => objectIdRddBroadcast.value.contains(f._1))
//      .reduceByKey((x, y) => {
//        val seq_length = x._1 + y._1
//        val seq = x._2.concat(param.seqSeparator).concat(y._2)
//        (seq_length, seq)
//      }
//      )
//
//    objectSeqCombineRdd

//    val objectSeqCombineRdd = lcsRdd.flatMap(m => List[String](m._1._1, m._1._3))
//          .distinct()
//          .map((_, 1))
//          .join(objectSeqRdd)
//          .map(r => (r._1, r._2._2))
////          .reduceByKey { (x, y) =>
////            (x._1 + y._1, s"${x._2}${param.seqSeparator}${y._2}")
////          }

        objectSeqCombineRdd.persist(StorageLevel.MEMORY_AND_DISK)
  }

  /**
    * 数据整理：小的在前，大的在后
    *
    * */
  def neaten(data :(String, String, Long, Long)) :(String, String, Long, Long)={
    if(data._2 < data._1)
      (data._2 , data._1 , data._4, data._3)
    else
      data
  }

  /**
    * 数据整理：小的在前，大的在后
    *
    * */
  def neaten(data : (String, String, String , Int, Int, Int) ) :((String, String), (String , Int, Int, Int)) ={
    if(data._2 < data._1)
      ((data._2 , data._1), (data._3, data._4, data._6, data._5))
    else
      ((data._1 , data._2), (data._3, data._4, data._5, data._6))
  }

  //计算结果落库：(对象A,对象B, AB相似度, BA相似度, 调和归一化相似度，时间差之和，公共子序列长度，公共子序列，对象A的序列长度，对象A的序列，对象B的序列长度，对象B的序列, 最早关联时间，最早关联设备，最近关联时间，最近关联设备，数据源）
  def insertData(ss: SparkSession, retDetail: RDD[Row], param: Param): Unit = {
    val params = " OBJECT_ID_A, TYPE_A, OBJECT_ID_B, TYPE_B, SIM_SCORE_AB, SIM_SCORE_BA,  SIM_SCORE, TIME_SUM, SEQ_SAME_LENGTH, SIM_SEQ_STR, SEQ_LENGTH_A, SEQ_STR_A,SEQ_LENGTH_B, SEQ_STR_B,FIRST_TIME, FIRST_DEVICE, LAST_TIME, LAST_DEVICE,SEQ_STR_SAME_INFO_A,SEQ_STR_SAME_INFO_B"
    val schema = StructType(List(
      StructField("OBJECT_ID_A", StringType, nullable = true),
      StructField("TYPE_A", IntegerType, nullable = true),
      StructField("OBJECT_ID_B", StringType, nullable = true),
      StructField("TYPE_B", IntegerType, nullable = true),
      StructField("SIM_SCORE_AB", DoubleType, nullable = true),
      StructField("SIM_SCORE_BA", DoubleType, nullable = true),
      StructField("SIM_SCORE", DoubleType, nullable = true),
      StructField("TIME_SUM", LongType, nullable = true),
      StructField("SEQ_SAME_LENGTH", IntegerType, nullable = true),
      StructField("SIM_SEQ_STR", StringType, nullable = true),
      StructField("SEQ_LENGTH_A", IntegerType, nullable = true),
      StructField("SEQ_STR_A", StringType, nullable = true),
      StructField("SEQ_LENGTH_B", IntegerType, nullable = true),
      StructField("SEQ_STR_B", StringType, nullable = true),
      StructField("FIRST_TIME", StringType, nullable = true),
      StructField("FIRST_DEVICE", StringType, nullable = true),
      StructField("LAST_TIME", StringType, nullable = true),
      StructField("LAST_DEVICE", StringType, nullable = true),
      StructField("SEQ_STR_SAME_INFO_A", StringType, nullable = true),
      StructField("SEQ_STR_SAME_INFO_B", StringType, nullable = true)
    ))

    val df = ss.createDataFrame(retDetail, schema)
    df.show(true)

    val outTable = new TableBean(resultTableName,"hive",
      "","default","","","default")

    val properties = PropertiesUtil.genJDBCProperties(outTable)

    val database = DataBaseFactory(ss, properties, outTable.databaseType)
    val dataBaseBean = database.getDataBaseBean
    dataBaseBean.dataBaseName = outTable.databaseName
    val saveParamBean = new SaveParamBean(outTable.tableName, outTable.outputType, dataBaseBean)
    saveParamBean.dataBaseBean = dataBaseBean
    saveParamBean.params = params
    saveParamBean.numPartitions = param.keyMap.getOrElse("numPartitions", "10").toString.toInt
    saveParamBean.partitions = Array(("STAT_DATE",s"${param.startTime.substring(0, 8)}"), ("TYPE",s"${param.releTypeStr}"))
    //    saveParamBean.preSql = s"""alter table ${outTable.tableName} DROP PARTITION('${eventDate}')"""
    //    database.save(saveParamBean, df.coalesce(24))
    database.save(saveParamBean, df)
  }

  def insertResultDataMPP(sparkSession: SparkSession,  param: Param , resultDF: RDD[Row]): Unit = {
    val params = " OBJECT_ID_A, TYPE_A, OBJECT_ID_B, TYPE_B, SIM_SCORE_AB, SIM_SCORE_BA,  SIM_SCORE, TIME_SUM, SEQ_SAME_LENGTH, SIM_SEQ_STR, SEQ_LENGTH_A, SEQ_STR_A,SEQ_LENGTH_B, SEQ_STR_B,FIRST_TIME, FIRST_DEVICE, LAST_TIME, LAST_DEVICE,SEQ_STR_SAME_INFO_A,SEQ_STR_SAME_INFO_B,TYPE,STAT_DATE"
    val schema = StructType(List(
      StructField("OBJECT_ID_A", StringType, nullable = true),
      StructField("TYPE_A", IntegerType, nullable = true),
      StructField("OBJECT_ID_B", StringType, nullable = true),
      StructField("TYPE_B", IntegerType, nullable = true),
      StructField("SIM_SCORE_AB", DoubleType, nullable = true),
      StructField("SIM_SCORE_BA", DoubleType, nullable = true),
      StructField("SIM_SCORE", DoubleType, nullable = true),
      StructField("TIME_SUM", LongType, nullable = true),
      StructField("SEQ_SAME_LENGTH", IntegerType, nullable = true),
      StructField("SIM_SEQ_STR", StringType, nullable = true),
      StructField("SEQ_LENGTH_A", IntegerType, nullable = true),
      StructField("SEQ_STR_A", StringType, nullable = true),
      StructField("SEQ_LENGTH_B", IntegerType, nullable = true),
      StructField("SEQ_STR_B", StringType, nullable = true),
      StructField("FIRST_TIME", StringType, nullable = true),
      StructField("FIRST_DEVICE", StringType, nullable = true),
      StructField("LAST_TIME", StringType, nullable = true),
      StructField("LAST_DEVICE", StringType, nullable = true),
      StructField("SEQ_STR_SAME_INFO_A", StringType, nullable = true),
      StructField("SEQ_STR_SAME_INFO_B", StringType, nullable = true),
      StructField("TYPE", StringType, nullable = true),
      StructField("STAT_DATE", StringType, nullable = true)
    ))

    val stat_date = param.endTime.substring(0, 8)

    val rdf = resultDF.map(r => Row.fromSeq(r.toSeq:+(param.releTypeStr):+(stat_date)))
    rdf.foreach(println)

    val df = sparkSession.createDataFrame(rdf, schema).repartition(1)


    var landDataBase = param.keyMap.getOrElse("landDataBase", "pd_dts").toString
    val (databaseType, subProtocol, queryParam) =
      if (param.mppdbType == "snowballdb") { //数据落地于睿帆snowballdb
        (Constant.SNOWBALLDB, Constant.SNOWBALL_SUB_PROTOCOL, "socket_timeout=3000000")
      } else { //数据落地于华为mppdbc
        (Constant.GAUSSDB, Constant.GAUSSDB_SUB_PROTOCOL, "characterEncoding=UTF-8&autoReconnect=true&useSSL=false&zeroDateTimeBehavior=convertToNull&serverTimezone=UTC")
      }

    val properties = new Properties()
    val url = s"jdbc:$subProtocol://${param.mppdbIp}:${param.mppdbPort}/$landDataBase?$queryParam"
    logger.info("=============="+param.mppdbType)
    logger.info("jdbc url ====== "+url)
    logger.info(param.mppdbUsername)
    logger.info(param.mppdbPassword)
    properties.setProperty("jdbc.url", url)
    properties.setProperty("username", param.mppdbUsername)
    properties.setProperty("password", param.mppdbPassword)

    val preSqlTemplate = s"delete from $resultTableName where STAT_DATE='$stat_date' and TYPE = '${param.releTypeStr}'"
    val preSql = ReplaceSqlUtil.replaceSqlParam(preSqlTemplate, param)

    val database = DataBaseFactory(sparkSession, properties, databaseType)
    val saveParamBean = new SaveParamBean(resultTableName, 1, database.getDataBaseBean)
    saveParamBean.dataBaseBean = database.getDataBaseBean
    saveParamBean.params = params
    saveParamBean.preSql = preSql
    saveParamBean.tableName = resultTableName
    saveParamBean.isCreateMppdbPartition = true
    saveParamBean.mppdbPartition = s"p$stat_date"
    saveParamBean.mppdbDate = param.batchId.substring(0, 8).toLong
    database.save(saveParamBean, df)
  }

  /***
    *
    * @param sparkSession
    * @param param
    * @return ((对象A，对象A类型，对象B，对象B类型)，（常量值1，设备ID(公共子序列)，时间差，对象A发生时间（首次时间），设备ID（首次设备））
    */
  def loadDetailData(sparkSession: SparkSession, param : Param )
  : RDD[((String, Int, String, Int), (Int, String, Long, Long, String, String, String))] = {
    val tableDetail = new TableBean("","hive",
      "","default","","","default")

    val properties = PropertiesUtil.genJDBCProperties(param)
    val database = DataBaseFactory(sparkSession, properties, tableDetail.databaseType)

    val queryParamBean = new QueryBean()
    queryParamBean.isPage = false
    val sql = param.keyMap(lcssDayLoadDetailSqlKey).toString
    val execSql  = ReplaceSqlUtil.replaceSqlParam(sql, param)
//    val execSql  = "SELECT object_id_a,type_a,object_id_b,type_b,device_id,timestamp_a,timestamp_b,sub_time,info_id_a,info_id_b FROM DM_RELATION_DISTRIBUTE_DETAIL  WHERE stat_date>=2021051800 and stat_date<=2021051823 and type='car-car' limit 2"
    val queryRet = database.query(execSql, queryParamBean)
    queryRet.show()
    val loadDataRdd = queryRet.rdd.map{ r =>
      val object_id_a = r.get(0).toString
      val object_a_type = r.get(1).toString.toInt
      val object_id_b = r.get(2).toString
      val object_b_type = r.get(3).toString.toInt
      val device_id = r.get(4).toString
      val event_time_a = r.get(5).toString.toLong
//      val event_time_b = r.get(6).toString.toLong
      val sub_time = r.get(7).toString.toLong
      val info_id_a  = r.getString(8)
      val info_id_b  = r.getString(9)
      ((object_id_a, object_a_type, object_id_b, object_b_type), (1, device_id, sub_time, event_time_a, device_id, info_id_a, info_id_b))
    }
    loadDataRdd
  }


  /***
    *
    * @param sparkSession
    * @param param
    * @return (对象，(对象的子序列长度，对象的子序列)）
    */
  def loadObjectSeqData(sparkSession: SparkSession, param : Param  )
  : RDD[(String, (Int, String))] = {
    val tableDetail = new TableBean("","hive",
      "","default","","","default")

    val properties = PropertiesUtil.genJDBCProperties(param)
    val database = DataBaseFactory(sparkSession, properties, tableDetail.databaseType)

    val queryParamBean = new QueryBean()
    queryParamBean.isPage = false
    queryParamBean.orderByParam = "stat_date"
    val sql = param.keyMap(lcssDayLoadSeqSqlKey).toString

    val execSql  = ReplaceSqlUtil.replaceSqlParam(sql, param)

    val queryRet = database.query(execSql, queryParamBean)
    queryRet.show()

    queryRet.rdd.map{ r =>
            val object_id = r.get(0).toString
            val seq_length = r.get(1).toString.toInt
            val seq_str = r.get(2).toString
            (object_id, (seq_length, seq_str))
          }
          .reduceByKey((x, y )=> {
            (x._1 + y._1, s"${x._2}${param.seqSeparator}${y._2}")
          })
          .persist(StorageLevel.MEMORY_AND_DISK)
  }


  /**合并每个小时的LCS
    * @param param: Param
    * @param dataRdd ((对象A，对象A类型，对象B，对象B类型)，（常量值1，设备ID(公共子序列)，时间差，对象A发生时间（首次时间），设备ID（首次设备），对象B发生时间（最近时间），设备ID（最近设备)）
    * @return ((对象A，对象A类型，对象B，对象B类型),(公共子序列长度, 公共子序列, 时间差之和, 最早关联时间，最早关联设备，最近关联时间,最近关联设备))
    */
  def combineLCS(param: Param, dataRdd: RDD[((String, Int, String, Int), (Int, String, Long, Long, String, String, String))])
  : RDD[((String,Int, String, Int), (Int, String,String,String, Long, Long, String, Long, String))] = {

    val minImpactCount = param.keyMap.getOrElse("minImpactCount", "1").toString.toInt
    //每个小时进行合并
    dataRdd.combineByKey(
      createCombiner = (value: (Int, String, Long, Long, String, String, String)) => List[(Int, String, Long, Long, String, String, String)](value),
      mergeValue = (list: List[(Int, String, Long, Long, String, String, String)], value: (Int, String, Long, Long, String, String, String)) => list:+ value,
      mergeCombiners = (list1: List[(Int, String, Long, Long, String, String, String)], list2: List[(Int, String, Long, Long, String, String, String)]) => list1 ::: list2
    )
//    ((对象A，对象A类型，对象B，对象B类型)，（常量值1，设备ID(公共子序列)，时间差，对象A发生时间（首次时间），设备ID（首次设备），对象B发生时间（最近时间），设备ID（最近设备)）
//    ((冀B3P40N-0, 2, 粤Q17PA3-0, 2), List((1, 440118626491017001, 0, 1621308072,               440118626491017001, 1621308072, 440118626491017001),
      //                                   (1, 440118626491017001, 0, 1621308072,               440118626491017001, 1621308072, 440118626491017001)))
   .mapValues{ row =>
     // 相同的key进来，意味着两辆车的所有轨迹
     //    （常量值1，设备ID(公共子序列)，时间差，对象A发生时间（首次时间），设备ID（首次设备），对象B发生时间（最近时间），设备ID（最近设备)
     //     (1, 440118626491017001, 0, 1621308072,               440118626491017001, 1621308072, 440118626491017001)
     val seq_same_length = row.map(_._1).sum //1 + 1 = 2
     val sub_time_sum = row.map(_._3).sum // 0 + 0 = 0

     //根据时间排序，（对象A发生时间（首次时间），设备ID（首次设备），设备ID(公共子序列)，info_a，info_b）
//                  (1621308072,           440118626491017001 440118626491017001, info_a，info_b) 1
//                  (1621308072,           440118626491017001 440118626491017001, info_a，info_b) 2
     val timeList = row.map(r => (r._4, r._5,r._2,r._6,r._7)).sortWith(_._1 < _._1)
     val seq_str_same = timeList.map(_._3).mkString(param.seqSeparator) // 公共子序列 (440118626491017001, 440118626491017001)
     val seq_str_same_info_a = timeList.map(_._4).mkString(param.seqSeparator) // (info_a，info_a)
     val seq_str_same_info_b = timeList.map(_._5).mkString(param.seqSeparator) // (info_b，info_b)

     val first = timeList.head //(1621308072,           440118626491017001 440118626491017001, info_a，info_b)  1
     val last = timeList.last //(1621308072,           440118626491017001 440118626491017001, info_a，info_b)  2

     //P相同卡口次数 2        公共子序列   info_a，info_a      info_b，info_b    时间差（单位：s）的绝对值的和 0     对象A发生时间（首次时间） 对象A末次时间 对象B发生时间（首次时间） 对象B末次时间）
     (seq_same_length, seq_str_same,seq_str_same_info_a,seq_str_same_info_b, sub_time_sum, first._1, first._2, last._1, last._2)
   }
   .filter(_._2._1 >= minImpactCount)
  }

  /**
    * 计算两两之间每天的相似度:通过单个对象分别与对象组的A,B两个对象进行join操作来计算，该对象组的伴随相似度
    * @param lcsRdd  ((对象A，对象A类型，对象B，对象B类型),(公共子序列长度, 公共子序列, 时间差之和, 最早关联时间， 最早关联设备，最近关联时间, 最近关联设备))
    * @param objectSeqFilterCombineRdd  （对象ID, （序列长度，序列））
    *@return (对象A, 对象A类型， 对象B, 对象B类型，相似度AB，相似度BA, 调和相似度， 时间差之和，公共子序列长度，公共子序列，对象A的序列长度，对象A的序列，对象B的序列长度，对象B的序列,  最早关联时间，最早关联设备，最近关联时间，最近关联设备）
    */
  def computeSimDay(param:Param, lcsRdd:RDD[((String,Int, String, Int), (Int, String,String,String, Long, Long, String, Long, String))],
                    objectSeqRdd: RDD[(String, (Int, String))] ): RDD[Row] = {

    lcsRdd.map {
              case ( (object_id_a, object_a_type, object_id_b, object_b_type),
                     (seq_same_length, seq_same_str,seq_str_same_info_a,seq_str_same_info_b, timeSum, ft,first_device, lt, last_device)
                ) =>
                val first_time = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.ofInstant(Instant.ofEpochMilli(ft * 1000), param.zoneId))
                val last_time = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.ofInstant(Instant.ofEpochMilli(lt * 1000), param.zoneId))
                (object_id_a, (object_a_type, object_id_b, object_b_type, timeSum, seq_same_length, seq_same_str,seq_str_same_info_a,seq_str_same_info_b, first_time, first_device, last_time, last_device))
          }
          .join(objectSeqRdd)
          .map{
            case ( object_id_a, ((object_a_type, object_id_b, object_b_type, timeSum, seq_same_length, seq_same_str,seq_str_same_info_a,seq_str_same_info_b, first_time,first_device, last_time, last_device),
            (seq_length_a, seq_str_a))) =>
              (object_id_b, (object_id_a, object_a_type, object_b_type, seq_length_a, seq_str_a, timeSum, seq_same_length, seq_same_str,seq_str_same_info_a,seq_str_same_info_b, first_time, first_device, last_time, last_device))
          }
          .join(objectSeqRdd)
          .map {
            case (object_id_b, ((object_id_a, object_a_type, object_b_type, seq_length_a, seq_str_a, timeSum, seq_same_length,seq_same_str,seq_str_same_info_a,seq_str_same_info_b, first_time, first_device, last_time, last_device),
            (seq_length_b, seq_str_b))) =>

              //计算AB
              val timeExpectValue: Double = timeSum / seq_same_length
              val part2: Double = (1 / (1 + (timeExpectValue / (param.keyMap("secondsSeriesThreshold").toString.toInt * 2)))) * param.mf

              val part1ab: Double = seq_same_length * 1.0 / seq_length_a * 1.0
              val sim_ab = (part1ab * part2).formatted("%.4f").toDouble

              //计算BA
              val part1ba: Double = seq_same_length * 1.0 / seq_length_b * 1.0
              val sim_ba = (part1ba * part2).formatted("%.4f").toDouble

              //调和函数计算
              val sim_harmonic_mean = ((2 * sim_ab * sim_ba) / (sim_ab + sim_ba)).formatted("%.4f").toDouble

              Row.fromTuple(object_id_a, object_a_type, object_id_b, object_b_type, sim_ab, sim_ba, sim_harmonic_mean,
                timeSum, seq_same_length, seq_same_str, seq_length_a, seq_str_a, seq_length_b, seq_str_b, first_time, first_device, last_time, last_device,seq_str_same_info_a,seq_str_same_info_b)
          }
  }


  def insertObjectSeqData(sparkSession: SparkSession,  param: Param , resultRdd: RDD[(String,( Int, String))]): Unit = {
    val params = "OBJECT_ID,SEQ_LENGTH, SEQ_STR"
    val schema = StructType(List(
      StructField("OBJECT_ID", StringType, nullable = true),
      StructField("SEQ_LENGTH", IntegerType, nullable = true),
      StructField("SEQ_STR", StringType, nullable = true)
    ))

    //转换成Row
    val rowRdd = resultRdd.map(r => Row.fromTuple(r._1, r._2._1, r._2._2))
    val df = sparkSession.createDataFrame(rowRdd, schema)

    val outTable = new TableBean(objectSeqTableName, "hive",
      "", "", "", "", "default")

    val properties = PropertiesUtil.genJDBCProperties(outTable)

    val database = DataBaseFactory(sparkSession, properties, outTable.databaseType)
    val dataBaseBean = database.getDataBaseBean
    dataBaseBean.dataBaseName = outTable.databaseName
    val saveParamBean: SaveParamBean = new SaveParamBean(outTable.tableName, outTable.outputType, dataBaseBean)
    saveParamBean.params = params
    saveParamBean.numPartitions = param.keyMap.getOrElse("numPartitions", "10").toString.toInt
    saveParamBean.partitions = Array(("STAT_DATE", s"${param.startTime.substring(0, 8)}"), ("TYPE",s"${param.releTypeStr}"))

    database.save(saveParamBean, df)

  }


  def process(param: Param): Unit = {

    val master = param.master

    val batchId = param.keyMap("batchId").toString

    val sparkSession = SparkUtil.getSparkSession(master, s"LCSS ${}param.releTypeStr ${batchId} Day Job", param)

    processSub(param, sparkSession)

    sparkSession.close()
  }

  def processSub(param: Param, sparkSession: SparkSession): Unit ={


    eventDate = param.dateFormat.format(param.startTimeStamp)

    lcssDayLoadDetailSqlKey = param.keyMap.getOrElse("LCSS_DAY_LOAD_DETAIL_SQL_KEY", "lcss.day.load.detail").toString

    lcssDayLoadSeqSqlKey = param.keyMap.getOrElse("LCSS_DAY_LOAD_SEQ_SQL_KEY", "lcss.day.load.seq").toString

    resultTableName = param.keyMap.getOrElse("RESULT_TABLE", "DM_LCSS_DAY").toString

    objectSeqTableName = param.keyMap.getOrElse("OBJECT_SEQ_TABLE", "DM_LCSS_OBJECT_DAY").toString

    //按天加载数据
    val relateDetailRdd = loadDetailData(sparkSession, param)

    //计算两两之间的LCS
    val lcsRdd = combineLCS(param, relateDetailRdd)

    val objectSeqRdd = loadObjectSeqData(sparkSession, param)

    //过滤合并lcsRdd中的对象的序列
    //    val objectSeqFilterCombineRdd = combineObjectSeq(sparkSession, param, objectSeqRdd, lcsRdd)

    //计算相似度
    val resultDF = computeSimDay(param, lcsRdd, objectSeqRdd)
    resultDF.saveAsTextFile("output/LcssResultDF")

    //数据落库
    insertData(sparkSession, resultDF, param)

    //子序列汇总落库
    insertObjectSeqData(sparkSession, param, objectSeqRdd)

    //结果数据MPP落库
    insertResultDataMPP(sparkSession, param, resultDF)
  }
}