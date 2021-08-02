package com.suntek.algorithm.process.accompany

import com.suntek.algorithm.algorithm.association.fptreenonecpb.FPTreeNoneCpb
import com.suntek.algorithm.common.bean.{QueryBean, SaveParamBean}
import com.suntek.algorithm.common.conf.{Constant, Param}
import com.suntek.algorithm.common.database.DataBaseFactory
import com.suntek.algorithm.common.util.SparkUtil
import com.suntek.algorithm.evaluation.DataManager
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

import java.util.Properties
import scala.collection.JavaConverters._

/**
  * @author chenb
  * @date 2020-11-30 12:04
  */
//noinspection SpellCheckingInspection
object SvAccompanyResultHandler {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val resultTableName = "SV_ACCOMPANY"
  val relationTableName = "DM_RELATION"
  val SV_ACCOMPANY_STAT_DATA = "sv.accompany.stat.load"
  val SV_ACCOMPANY_ALL_DATA = "sv.accompany.load.all"
  val SVTYPE = "svType"
  val AGGDAYS = "aggDays"
  val MINCOUNT = "minCount"
  val MINSUPPORT = "minSupport"
  val MAXASSEMBLETOTAL = "maxAssembleTotal"

  @throws[Exception]
  def process(param: Param): Unit = {
    try {
      param.svType = param.keyMap.getOrElse(SVTYPE, "TSW").toString
      val aggDays = param.keyMap.getOrElse(AGGDAYS, "3").toString.toInt
      val minCount = param.keyMap.getOrElse(MINCOUNT, "2").toString.toInt
      val minSupport = param.keyMap.getOrElse(MINSUPPORT, "0.3").toString.toDouble
      val maxAssembleTotal = param.keyMap.getOrElse(MAXASSEMBLETOTAL, "30").toString.toInt


      val spark: SparkSession = SparkUtil.getSparkSession(param.master, s"${param.releTypeStr}_${param.svType}_SvAccompanyResultHandler ${param.batchId}", param)
      // 判断上一个批次是否运行
      val (startDate, endDate, flag) = getBatchId(spark, param, aggDays)

      logger.info(s"aggDays:$aggDays")
      logger.info(s"minCount:$minCount")
      logger.info(s"minSupport:$minSupport")
      logger.info(s"maxAssembleTotal:$maxAssembleTotal")
      logger.info(s"startDate:$startDate")
      logger.info(s"endDate:$endDate")
      logger.info(s"flag:$flag")

      val statSql = param.keyMap(SV_ACCOMPANY_STAT_DATA).toString
                         .replaceAll("@type@", param.svType)
                         .replaceAll("@category1@", param.category1)
                         .replaceAll("@category2@", param.category2)
                         .replaceAll("@startDate@", startDate)
                         .replaceAll("@endDate@", endDate)
      val minCountBc = spark.sparkContext.broadcast(minCount)
      val minSupportBc = spark.sparkContext.broadcast(minSupport)
      val maxAssembleTotalBc = spark.sparkContext.broadcast(maxAssembleTotal)

      val statRdd = loadStatData(spark, param, statSql, minCountBc, maxAssembleTotalBc)
      val fpTreeFrequent = fPGrowthNoneCpb(statRdd, minCountBc, minSupportBc)
      val rddCountSc = getIDCount(spark, statRdd)
      val aFrequentItemSet = getAFrequentItemSet(statRdd, fpTreeFrequent, rddCountSc)

      val lastAggDaysStamp: Long = param.batchIdDateFormat.parse(param.keyMap("batchId").toString).getTime -
        param.prevHourNum * (aggDays + 1) * 60 * 60 * 1000L
      val lastAggDaysBatchId: String = param.batchIdDateFormat.format(lastAggDaysStamp).substring(0, 8)
      val allSql = param.keyMap(SV_ACCOMPANY_ALL_DATA).toString
                        .replaceAll("@type@", param.svType)
                        .replaceAll("@category1@", param.category1)
                        .replaceAll("@category2@", param.category2)
                        .replaceAll("@shotDate@", lastAggDaysBatchId)

      val dataRet = loadDataAgg(spark, param, aFrequentItemSet, allSql, flag)
      insertDataAgg(spark, dataRet, resultTableName, param)
      insertRelationData(spark, dataRet, relationTableName, param)

      spark.close()
    } catch {
      case ex: Exception =>
        logger.error(ex.getLocalizedMessage)
        throw ex
    }

  }

  @throws
  def getBatchId(spark: SparkSession,
                 param: Param,
                 aggDays: Int): (String, String, Boolean) = {

    val sql = s"SELECT DISTINCT stat_date  FROM $resultTableName WHERE stat_date < '${param.startTime.substring(0, 8)}' AND type = '${param.svType}' ORDER BY stat_date"

    val databaseType = param.keyMap("databaseType").toString

    val partitions = DataBaseFactory(spark, new Properties(), databaseType)
                            .query(sql, new QueryBean())
                            .rdd
                            .map { r =>
                              try {
                                r.get(0).toString
                              }catch {
                                case _ : Exception => ""
                              }
                            }
                            .filter(_.nonEmpty)
                            .collect()
    logger.info(s"partitions:${partitions.mkString("|++|")}")
    /*if (partitions.nonEmpty && partitions.last != param.lastStatDate){
      logger.info(s"${param.lastStatDate}批次未执行,无法执行当前批次${param.startTime.substring(0, 8)}")
      throw new Exception(s"${param.lastStatDate}批次未执行,无法执行当前批次${param.startTime.substring(0, 8)}")
    }*/
    logger.info(s"partitions.length:${partitions.length}")
    var flag = false
    val lastAggDaysStamp = if (partitions.length > aggDays * 2){ //使用一个周期内的数据计算
                              flag = true
                              param.batchIdDateFormat.parse(param.keyMap("batchId").toString).getTime -
                                param.prevHourNum * (aggDays + 1) * 60 * 60 * 1000L
                           }else { // 全部分区数据参与计算
                              param.batchIdDateFormat.parse(param.keyMap("batchId").toString).getTime -
                                param.prevHourNum * (aggDays * 2 + 2) * 60 * 60 * 1000L
                           }

    val lastAggDaysBatchId = param.batchIdDateFormat.format(lastAggDaysStamp).substring(0, 8)
    logger.info(s"lastAggDaysBatchId:$lastAggDaysBatchId")

    (lastAggDaysBatchId, param.startTime.substring(0, 8), flag)
  }

  def loadStatData(spark: SparkSession,
                   param: Param,
                   sql: String,
                   minCountBc: Broadcast[Int],
                   maxAssembleTotalBc: Broadcast[Int])
  // 设备,人员对象A,人员对象B,关联时间
  : RDD[(String, (String, String, Long))] = {

    val databaseType = param.keyMap("databaseType").toString

    DataBaseFactory(spark, new Properties(), databaseType)
            .query(sql, new QueryBean())
            .rdd
            .map { r =>
              //object_id_a,object_id_b,device_id,today_times,timestamp_a,timestamp_b
              try {
                ((r.get(1).toString, r.get(2).toString), (r.get(0).toString, r.get(3).toString.toLong,
                  r.get(4).toString, r.get(5).toString))
              }catch {
                case _ : Exception => (("", ""), ("", 0L, "", ""))
              }
            }
            .filter(_._1._1.nonEmpty)
            .groupByKey()
            .filter(_._2.map(_._2).sum >= minCountBc.value)
            //((设备,人员对象A,对象A经过时间字符串),(人员对象B,对象A经过时间字符串,当天关联次数))
            .flatMap{ row =>
              var list = List[((String, String, Long), (String, Long))]()
              row._2.foreach { r =>
                  val timeA = r._3.split("#").toList
                  val timeB = r._4.split("#").toList
                  for (i <- timeA.indices){
                    list +:= (((r._1, row._1._1, timeA(i).toLong), (row._1._2, timeB(i).toLong)))
                  }
                }
              list
            }
            .groupByKey()
            .flatMap { v =>
                var list = v._2
                if (v._2.size > maxAssembleTotalBc.value){
                  //该设备下该时间分片对应的数超过50个，则按时间差排序缩小分片范围
                  list = v._2.toList
                             .map(r=>(r._1, Math.abs(r._2 - v._1._3))) //人员对象B,时间差,当天关联次数
                             .sortBy(_._2)
                             .take(maxAssembleTotalBc.value)
                }
                // ((人员对象A),(设备,人员对象B,时间戳))
                list.map( r=> (v._1._2, (v._1._1, r._1, v._1._3)))
            }
            .distinct()
            .persist(StorageLevel.MEMORY_AND_DISK_SER)
  }

  def fPGrowthNoneCpb( // ((人员对象A),(设备,人员对象B,时间戳))
                       statRdd: RDD[(String, (String, String, Long))],
                       minCountBc: Broadcast[Int],
                       minSupportBc: Broadcast[Double]
                     )
  : RDD[((String, String), Integer)] = {

    statRdd.combineByKey((id: (String, String, Long)) => List[(String, String, Long)](id),
              (idList: List[(String, String, Long)], id: (String, String, Long)) => idList:+ id,
              (idList1: List[(String, String, Long)], idList2: List[(String, String, Long)]) => idList1:::idList2)
           .flatMap { v=>
              val id = v._1
              val matrix = v._2.distinct
                               .map(v=>((v._1, v._3), v._2))
                               .groupBy(_._1)
                               .map(v=> v._2.map(_._2).asJava)
                               .toList.asJava
              val fpTreeNoneCpb = new FPTreeNoneCpb()
              fpTreeNoneCpb.setMinCount(minCountBc.value)
              fpTreeNoneCpb.setIsFindMaxFrequentSet(false)
              fpTreeNoneCpb.setMinSupport(minSupportBc.value)
              fpTreeNoneCpb.setN(matrix.size())
              val root = fpTreeNoneCpb.buildTree(matrix)
              fpTreeNoneCpb.getHeaders(root)
              val currentTimeStamp: Long = System.currentTimeMillis
              fpTreeNoneCpb.fPMining(id, currentTimeStamp)
              val transformTable = fpTreeNoneCpb.getTransformTable
              transformTable.getFrequentMap
                            .values()
                            .asScala
                            .map { v =>
                              // ((id1, id2),支持次数)
                              ((id, v.getItem), v.getCount)
                            }
           }
    }

  private def getAFrequentItemSet(
                                   // ((人员对象A),(设备,人员对象B,时间戳))
                                   statRdd: RDD[(String, (String, String, Long))],
                                   //((id1, id2),(支持次数))
                                   ret: RDD[((String, String), Integer)],
                                   rddCountSc :Broadcast[collection.Map[String, Double]])
  : RDD[((String, String), (Long, Double, Long, String, Long, String))] = {
             //((id1,id2),(设备, 时间戳))
    statRdd.map( row => ((row._1, row._2._2), (row._2._1, row._2._3)))
           .groupByKey()
           .map { v=>
             val list = v._2.toList.sortWith(_._2 < _._2) // 从小到大
             //((id1,id2), (最早关联设备,最早时间,最晚关联设备,最晚时间))
             (v._1, (list.head._1, list.head._2, list.last._1, list.last._2))
           }
           .join(ret)
           .map{ r =>
             val totalA = rddCountSc.value.getOrElse(r._1._1, 0.0D)
             val totalB = rddCountSc.value.getOrElse(r._1._2,  0.0D)
             val confidence1 = (r._2._2.toLong * 1.0) / (totalA * 1.0)
             val confidence2 = (r._2._2.toLong * 1.0) / (totalB * 1.0)
             val confidence = 2 * 1.0 * (confidence1 * confidence2) / (confidence1 + confidence2)
             // ((id1,id2),(关联次数,置信度,首次关联时间,首次关联设备ID,最近关联时间,最近关联设备ID))
             ((r._1._1, r._1._2), (r._2._2.toLong, confidence, r._2._1._2, r._2._1._1, r._2._1._4, r._2._1._3))
           }
  }

  def getIDCount(spark: SparkSession,
                 // ((人员对象A),(设备,人员对象B,时间戳))
                 statRdd: RDD[(String, (String, String, Long))])
  : Broadcast[collection.Map[String, Double]] = {

    val rddCount = statRdd.flatMap(r => List[(String, Double)]((r._1, 1.toDouble), (r._2._2, 1.toDouble)))
                           .reduceByKey(_ + _)
                           .collectAsMap()
    spark.sparkContext.broadcast(rddCount)
  }

  def loadDataAgg(spark: SparkSession,
                  param: Param,
                  // object_id_a,object_id_b,times,score,first_time,first_device,last_time,last_device
                  aFrequentItemSet: RDD[((String, String), (Long, Double, Long, String, Long, String))],
                  sql: String,
                  isReadALlData: Boolean)
  // 对象A,对象A类型,对象B,对象B类型,关联总次数,置信度,最早关联时间,最早设备id,最晚关联时间,最晚设备id,入库时间
  : RDD[(String, String, String, String, Long, Double, Long, String, Long, String, String)] = {

    val category1Bc = spark.sparkContext.broadcast(param.category1)
    val category2Bc = spark.sparkContext.broadcast(param.category2)
    val nowFormatBc = spark.sparkContext.broadcast(param.nowFormat)

    if (isReadALlData){ // 获取一个周期前的数据进行汇总
      val databaseType = param.keyMap("databaseType").toString
      DataBaseFactory(spark, new Properties(), databaseType)
            .query(sql, new QueryBean())
            .rdd
            .map { r=>
              //((id1, id2), (关联总次数,置信度,最早关联时间,最早设备id,最晚关联时间,最晚设备id))
              try {
                ((r.get(0).toString, r.get(1).toString), (r.get(2).toString.toLong, r.get(3).toString.toDouble,
                  r.get(4).toString.toLong, r.get(5).toString, r.get(6).toString.toLong, r.get(7).toString))
              }catch {
                case _: Exception =>
                  (("", ""), (0L, 0D, 0L, "", 0L, ""))
              }
            }
            .filter(_._1._1.nonEmpty)
            .union(aFrequentItemSet)
            .groupByKey()
            .map{ r =>
               val times = r._2.map(_._1)
               val confidence = if (times.size == 1) { // 只有一条数据
                                r._2.map(_._2).sum
                               } else {
                                val confidences = r._2.map(_._2)
                                (times.head * 1.0 / times.sum) * confidences.head + (times.last * 1.0 / times.sum) * confidences.last
                               }
               val timeSort = r._2.map(v => (v._3, v._4, v._5, v._6)).toList.sortWith(_._1 < _._1)
               // 对象A,对象A类型,对象B,对象B类型,关联总次数,置信度,最早关联时间,最早设备id,最晚关联时间,最晚设备id
               (r._1._1, category1Bc.value, r._1._2, category2Bc.value, times.sum, confidence,
                 timeSort.head._1, timeSort.head._2, timeSort.last._3, timeSort.last._4, nowFormatBc.value)
            }
    }else {
      aFrequentItemSet.map(r => (r._1._1, category1Bc.value, r._1._2, category2Bc.value, r._2._1, r._2._2,
                            r._2._3, r._2._4, r._2._5, r._2._6, nowFormatBc.value))
    }
    .persist(StorageLevel.MEMORY_AND_DISK_SER)
  }

  def insertDataAgg(spark: SparkSession,
                    // 对象A,对象A类型,对象B,对象B类型,关联总次数,置信度,最早关联时间,最早设备id,最晚关联时间,最晚设备id,入库时间
                    dataRet: RDD[(String, String, String, String, Long, Double, Long, String, Long, String, String)],
                    tableName: String,
                    param: Param)
  : Unit = {

    val databaseType = param.keyMap("databaseType").toString
    val svTypeBc = spark.sparkContext.broadcast(param.svType)
    val startTimeBc = spark.sparkContext.broadcast(param.startTime)

    // 对象A,对象A类型,对象B,对象B类型,关联总次数,置信度,最早关联时间,最早设备id,最晚关联时间,最晚设备id
    val params = "OBJECT_ID_A,TYPE_A,OBJECT_ID_B,TYPE_B,TIMES,SCORE,FIRST_TIME,FIRST_DEVICE,LAST_TIME,LAST_DEVICE,ADD_TIME"
    val schema = StructType(List[StructField](
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
      StructField("ADD_TIME", StringType, nullable = true)
    ))

    //转换成Row
    val rowRdd = dataRet.map(Row.fromTuple)
    val df = spark.createDataFrame(rowRdd, schema)
    df.show()

    val database = DataBaseFactory(spark, new Properties(), databaseType)
    val saveParamBean = new SaveParamBean(tableName, Constant.OVERRIDE_DYNAMIC, database.getDataBaseBean)
    saveParamBean.dataBaseBean = database.getDataBaseBean
    saveParamBean.params = params
    saveParamBean.numPartitions = param.keyMap.getOrElse("numPartitions", "10").toString.toInt
    saveParamBean.partitions = Array[(String, String)](("TYPE", s"${svTypeBc.value}"), ("STAT_DATE",s"${startTimeBc.value.substring(0, 8)}"))
    database.save(saveParamBean, df)
  }

  def insertRelationData(spark: SparkSession,
                         // 对象A,对象A类型,对象B,对象B类型,关联总次数,置信度,最早关联时间,最早设备id,最晚关联时间,最晚设备id,入库时间
                         dataRet: RDD[(String, String, String, String, Long, Double, Long, String, Long, String, String)],
                         tableName: String,
                         param: Param,
                         outputType: Int = Constant.OVERRIDE_DYNAMIC)
  : Unit = {

    val databaseType = param.keyMap("databaseType").toString
    val params = "OBJECT_ID_A,TYPE_A,OBJECT_ATTR_A,OBJECT_ID_B,TYPE_B,OBJECT_ATTR_B" +
      ",SCORE,FIRST_TIME,FIRST_DEVICE,LAST_TIME,LAST_DEVICE,TOTAL,REL_ATTR"
    val schema = StructType(List[StructField](
      StructField("OBJECT_ID_A", StringType, nullable = true),
      StructField("TYPE_A", StringType, nullable = true),
      StructField("OBJECT_ATTR_A", StringType, nullable = true),
      StructField("OBJECT_ID_B", StringType, nullable = true),
      StructField("TYPE_B", StringType, nullable = true),
      StructField("OBJECT_ATTR_B", StringType, nullable = true),
      StructField("SCORE", IntegerType, nullable = true),
      StructField("FIRST_TIME", LongType, nullable = true),
      StructField("FIRST_DEVICE", StringType, nullable = true),
      StructField("LAST_TIME", LongType, nullable = true),
      StructField("LAST_DEVICE", StringType, nullable = true),
      StructField("TOTAL", IntegerType, nullable = true),
      StructField("REL_ATTR", StringType, nullable = true)
    ))
    //转换成Row                                 // A,  A类型,A属性 B     B类型  B属性     相似度
    val rowRdd = dataRet.map(r => Row.fromTuple((r._1, r._2, "", r._3, r._4, "", (r._6 * 100).toInt,
      //最早关联时间,最早设备id,最晚关联时间,最晚设备id,关系属性
      r._7, r._8, r._9, r._10, r._5.toInt, "")))
    val df = spark.createDataFrame(rowRdd, schema)

    val database = DataBaseFactory(spark, new Properties(), databaseType)
    val saveParamBean = new SaveParamBean(tableName, outputType, database.getDataBaseBean)
    saveParamBean.dataBaseBean = database.getDataBaseBean
    saveParamBean.params = params
    val map = DataManager.loadRelation()
    logger.info(s"param.startTime: ${param.startTime}")
    saveParamBean.partitions = Array[(String, String)](
      ("STAT_DATE", s"${param.startTime.substring(0, 8)}"),
      ("REL_TYPE", s"${param.relationType}"), //1 要素关联 2 实体关系
      ("REL_ID", s"${map(param.svType.toLowerCase())}")
    )
    database.save(saveParamBean, df)
  }
  /**
   * 数据整理：小的在前，大的在后
   *
   * */
  def neaten(data : (String, String)): (String, String) ={
    if(data._2 < data._1) {
      (data._2, data._1)
    } else {
      data
    }
  }
}