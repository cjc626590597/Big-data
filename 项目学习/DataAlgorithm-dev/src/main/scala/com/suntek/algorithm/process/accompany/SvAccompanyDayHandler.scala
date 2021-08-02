package com.suntek.algorithm.process.accompany

import com.suntek.algorithm.common.bean.{QueryBean, SaveParamBean}
import com.suntek.algorithm.common.conf.{Constant, Param}
import com.suntek.algorithm.common.database.DataBaseFactory
import com.suntek.algorithm.common.util.SparkUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

import java.text.SimpleDateFormat
import java.util.Properties
import scala.collection.mutable.ArrayBuffer
/**
 * @author chenb
 * @date 2020-11-28 12:04
 */
//noinspection SpellCheckingInspection
object SvAccompanyDayHandler {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val tableName = "SV_ACCOMPANY_DAY"
  val DISTRIBUTE_LOAD_DATA = "sv.accompany.day.sql"
  val SV_ACCOMPANY_STAT_DATA = "sv.accompany.stat.load"
  val DATASOURCE = "dataSource"
  val SERIESCOUNT = "seriesCount"
  val SVTYPE = "svType"
  val TIMESERIESTHRESHOLD = "timeSeriesThreshold"
  val SECONDSSERIESTHRESHOLD = "secondsSeriesThreshold"

  @throws
  def process(param: Param): Unit = {

    try {
      param.svType = param.keyMap.getOrElse(SVTYPE, "TSW").toString
      //数据来源[-1:其他 0:自建 1:火车 2:民航 3:汽车 4:住宿 5:网吧 6:出入境 7:涉警 8:人脸抓拍9：户政常口，10：户政流口，11政务办人口，
      // 12 政务办流口，13 门禁，14 治安，15 重点人员，16 省警综案件]
      val dataSource = param.keyMap.getOrElse(DATASOURCE, "8").toString
      val timeSeriesThreshold = param.keyMap.getOrElse(TIMESERIESTHRESHOLD, "30").toString.toLong
      val secondsSeriesThreshold = param.keyMap.getOrElse(SECONDSSERIESTHRESHOLD, "60").toString.toLong
      val seriesCount = param.keyMap.getOrElse(SERIESCOUNT, "60").toString.toLong

      val spark: SparkSession = SparkUtil.getSparkSession(param.master, s"${param.releTypeStr}_${param.svType}_SvAccompanyDayHandler ${param.batchId}", param)

      logger.info(s"数据源: $dataSource, 时间分片:$timeSeriesThreshold, 时间阈值: $secondsSeriesThreshold, 分片内最多人员数据:$seriesCount")

      // 判断上一个批次是否运行
      // 不做此类判断
      // checkLastPartitions(spark, param)

      val timeSeriesThresholdBc = spark.sparkContext.broadcast(timeSeriesThreshold)
      val seriesCountBc = spark.sparkContext.broadcast(seriesCount)

      val secondsSeriesThresholdBc = spark.sparkContext.broadcast(secondsSeriesThreshold)

      val svTypeBc = spark.sparkContext.broadcast(param.svType)

      // 获取当前批次的数据
      val nowBatchSql = param.keyMap(DISTRIBUTE_LOAD_DATA).toString
                             .replaceAll("@shotDate@", s"${param.startTime.substring(2, 8)}")
                             .replaceAll("@dataSource@", dataSource)
      val nowBatchData = loadNowBatchData(spark, param, nowBatchSql, timeSeriesThresholdBc, seriesCountBc,
        secondsSeriesThresholdBc, svTypeBc)

      insertDataDetail(spark, nowBatchData, param, tableName)

      spark.close()
    } catch {
      case ex: Exception =>
        logger.error(ex.getLocalizedMessage)
        throw ex
    }
  }

  def loadNowBatchData(spark: SparkSession,
                       param: Param,
                       sql: String,
                       timeSeriesThresholdBc: Broadcast[Long],
                       seriesCountBc: Broadcast[Long],
                       secondsSeriesThresholdBc: Broadcast[Long],
                       svTypeBc: Broadcast[String]
                      )
  //设备,人员对象A,人员对象B,当天关联次数,对象A经过时间戳,对象B经过时间戳
  : RDD[Row] = {

    val databaseType = param.keyMap("databaseType").toString
    val batchIdDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val rdd = DataBaseFactory(spark, new Properties(), databaseType)
                  .query(sql, new QueryBean())
                  .rdd
                  .map { r=> // 人员对象,device_id,shot_time
                    try {
                      val personId = r.get(0).toString
                      val deviceId = r.get(1).toString
                      val shotTime = r.get(2).toString
                      val shotTimeTimestamp = batchIdDateFormat.parse(shotTime).getTime / 1000
                      ((deviceId, shotTimeTimestamp / timeSeriesThresholdBc.value), (personId, shotTime.toLong))
                      //((deviceId,时间分片数),(人员对象,shotTime))
                    }catch {
                      case _ : Exception => (("", 0L), ("", 0L))
                    }
                  }
                  .filter(_._1._1.nonEmpty)
                  .groupByKey()
                  .filter(_._2.map(_._1).toSet.size <= seriesCountBc.value) //同一个设备,同一分片下,去掉人员对象个数大于阙值(60)的(设备,分片)
                  .flatMap( row => row._2.toList.map(r => ((row._1._1, r._1), r._2))) // ((设备,人员对象A,人员对象B),(A抓拍时间,B抓拍时间) ))
                  .combineByKey[List[Long]](
                    createCombiner = (v: Long) => List[Long](v),
                    mergeValue = (c: List[Long], v: Long) => c :+ v,
                    mergeCombiners = (list1: List[Long], list2: List[Long]) => list1 ::: list2
                  )
                  .flatMap { v =>
                      // 压缩数据，统一id和设备下前后timeSeriesThreshold时间的数据做压缩，时间取平均值
                      val ret = ArrayBuffer[ArrayBuffer[Long]]()
                      var a = ArrayBuffer[Long]()
                      val list: List[Long] = v._2.sortBy(r => r)
                      a.append(list.head)
                      for (i <- 0 until list.size - 1) {
                        val j = i + 1
                        val t = list(j) - list(i)
                        if (t > timeSeriesThresholdBc.value) {
                          ret.append(a)
                          a = ArrayBuffer[Long]()
                        }
                        a.append(list(j))
                      }
                      if (a.nonEmpty) {
                        ret.append(a)
                      }
                      ret.map(r => (v._1._1, (v._1._2.toUpperCase(), r.sum / r.size)))
                  } // 设备,人员对象, 抓拍时间
                  .distinct()
                  .persist(StorageLevel.MEMORY_AND_DISK_SER)

    genDetectionDis(spark, param, rdd, secondsSeriesThresholdBc, svTypeBc)
  }

  def genDetectionDis(spark: SparkSession,
                      param: Param,
                      ret: RDD[(String, (String, Long))],
                      secondsSeriesThresholdBc: Broadcast[Long],
                      svTypeBc: Broadcast[String]
                     )
  // 设备,人员对象A,人员对象B,当天关联次数,对象A最早经过时间戳,对象B最早经过时间戳,对象A最近经过时间戳,对象B最近经过时间戳, 关联时间列表(以A的时间为准)
  : RDD[Row] = {
    val category1Bc = spark.sparkContext.broadcast(param.category1)
    val category2Bc = spark.sparkContext.broadcast(param.category2)
    val nowFormatBc = spark.sparkContext.broadcast(param.nowFormat)
    val joinRdd = ret.join(ret)
                     .filter(r => r._2._1._1 != r._2._2._1) // 过滤人员对象相同的数据
                     .map{ v =>
                        try {
                          val idTimeStamp2 = v._2._2._2
                          val idTimeStamp1 = v._2._1._2
                          if (idTimeStamp1 - idTimeStamp2 < secondsSeriesThresholdBc.value
                            && idTimeStamp1 - idTimeStamp2 > secondsSeriesThresholdBc.value * -1) {
                            //(设备，id1，id2, 时间戳)
                            val (d1, d2, t1, t2) = neaten(v._2._1._1, v._2._2._1, idTimeStamp1, idTimeStamp2)
                            ((v._1, d1, d2, t2), (t1, 1))
                          } else {
                            (("", "", "", 0L), (0L, 0))
                          }
                        } catch {
                          case _: Exception => (("", "", "", 0L), (0L, 0))
                        }
                      }
                     .filter(_._1._1.nonEmpty)
                     .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
                     .map { v =>
                        val key = v._1
                        val timeStamp1 = v._2._1 / v._2._2
                        //设备，对象A，对象B, 对象A时间戳, 对象B时间戳
                        ((key._1, key._2, key._3), (timeStamp1, key._4))
                     }
                     .groupByKey()
    if (svTypeBc.value == "TZ") { // 针对同住,同一个设备下,当天出现多次,只能算一次,出现时间取平均值
      joinRdd.map { row =>
        val timeSort = row._2.toList.sortWith(_._1 < _._1)
        val timeA = timeSort.map(_._1)
        val timeB = timeSort.map(_._2)
        // 设备,人员对象A,对象A类型,人员对象B,对象B类型,当天关联次数,对象A经过时间戳,对象B经过时间戳, 入库时间
        Row.fromTuple(row._1._1, row._1._2, category1Bc.value, row._1._3, category2Bc.value, 1.toLong,
          (timeA.sum / timeA.length).toString, (timeB.sum / timeB.length).toString, nowFormatBc.value)
      }
    } else {
      joinRdd.map { row =>
        val timeSort = row._2.toList.sortWith(_._1 < _._1)
        // 设备,人员对象A,对象A类型,人员对象B,对象B类型,当天关联次数,对象A经过时间戳,对象B经过时间戳, 入库时间
        Row.fromTuple(row._1._1, row._1._2, category1Bc.value, row._1._3, category2Bc.value, row._2.size.toLong,
          timeSort.map(_._1).mkString("#"), timeSort.map(_._2).mkString("#"), nowFormatBc.value)
      }
    }
  }

 @throws
  def checkLastPartitions(spark: SparkSession,
                          param: Param): Unit = {

    val sql = s"SELECT DISTINCT stat_date  FROM $tableName WHERE stat_date < '${param.startTime.substring(0, 8)}' AND type = '${param.svType}' ORDER BY stat_date"

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
    if (partitions.nonEmpty && partitions.last != param.lastStatDate){
      logger.info(s"${param.lastStatDate}批次未执行,无法执行当前批次${param.startTime.substring(0, 8)}")
      System.exit(-1)
      throw new Exception(s"${param.lastStatDate}批次未执行,无法执行当前批次${param.startTime.substring(0, 8)}")
    }
  }

  def insertDataDetail(spark: SparkSession,
                       rowRdd: RDD[Row],
                       param: Param,
                       tableName: String)
  : Unit = {
    val databaseType = param.keyMap("databaseType").toString
    val svTypeBc = spark.sparkContext.broadcast(param.svType)
    val startTimeBc = spark.sparkContext.broadcast(param.startTime)

    // 设备,人员对象A,人员对象A类型,人员对象B,人员对象B类型,当天关联次数,对象A经过时间戳,对象B经过时间戳,入库时间
    val params = "DEVICE_ID,OBJECT_ID_A,TYPE_A,OBJECT_ID_B,TYPE_B,TODAY_TIMES,TIMESTAMP_A,TIMESTAMP_B,ADD_TIME"
    val schema = StructType(List[StructField](
      StructField("DEVICE_ID", StringType, nullable = true),
      StructField("OBJECT_ID_A", StringType, nullable = true),
      StructField("TYPE_A", StringType, nullable = true),
      StructField("OBJECT_ID_B", StringType, nullable = true),
      StructField("TYPE_B", StringType, nullable = true),
      StructField("TODAY_TIMES", LongType, nullable = true),
      StructField("TIMESTAMP_A", StringType, nullable = true),
      StructField("TIMESTAMP_B", StringType, nullable = true),
      StructField("ADD_TIME", StringType, nullable = true)
    ))
    //转换成Row
    val df = spark.createDataFrame(rowRdd, schema)
    df.show()

    val database = DataBaseFactory(spark, new Properties(), databaseType)
    val saveParamBean = new SaveParamBean(tableName, Constant.OVERRIDE_DYNAMIC, database.getDataBaseBean)
    saveParamBean.dataBaseBean =  database.getDataBaseBean
    saveParamBean.params = params
    saveParamBean.numPartitions = param.keyMap.getOrElse("numPartitions", "10").toString.toInt
    saveParamBean.partitions = Array[(String, String)](("TYPE", s"${svTypeBc.value}"), ("STAT_DATE",s"${startTimeBc.value.substring(0, 8)}"))
    database.save(saveParamBean, df)
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
}