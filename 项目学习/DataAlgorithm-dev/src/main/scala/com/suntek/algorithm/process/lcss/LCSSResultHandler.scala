package com.suntek.algorithm.process.lcss

import com.suntek.algorithm.common.bean.{QueryBean, SaveParamBean, TableBean}
import com.suntek.algorithm.common.conf.Param
import com.suntek.algorithm.common.database.DataBaseFactory
import com.suntek.algorithm.common.util.{PropertiesUtil, SparkUtil}
import com.suntek.algorithm.process.util.ReplaceSqlUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created with IntelliJ IDEA.
  * Author: yuanweilin
  * Date: 2020-9-5 15:47
  * Description:伴随/同行分析，获取每天的同行分析数据与当前总的数据进行汇总计算得到新的数据
  */
object LCSSResultHandler {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  //model.sql文件中配置的加载伴随一天数据的sql语句的key值
  var lcssLoadDayResultSqlKey = ""

  //model.sql文件中配置的伴随结果数据的sql语句的key值
  var lcssLoadAllResultSqlKey = ""

  var lcssLoadDaySeqSqlKey = ""
  var lcssLoadAllSeqSqlKey = ""


  //结果表名
  var resultTableName = ""

  var accompanyType:String = ""

  var objectSeqTableName: String = ""

  def loadDayData(sparkSession: SparkSession, param: Param)
  : RDD[((String, Int, String, Int), (Int, Int, String, String,String, String))] = {

    val tableDetail = new TableBean("","hive",
      "","","","","default")

    val properties = PropertiesUtil.genJDBCProperties(param)

    val database = DataBaseFactory(sparkSession, properties, tableDetail.databaseType)

    val queryParamBean = new QueryBean()
    queryParamBean.isPage = false

    //加载每天相似度数据
    val sql = param.keyMap(lcssLoadDayResultSqlKey).toString

    val execSql  = ReplaceSqlUtil.replaceSqlParam(sql, param)

    val minImpactCount = param.keyMap.getOrElse("minImpactCount", "3").toString.toInt
    logger.info(s"minImpactCount: ${minImpactCount}")

    val dayDF = database
      .query(execSql, queryParamBean)

    dayDF
      .asInstanceOf[DataFrame]
      .rdd
      .map { v =>
        val object_id_a = v.get(0).toString
        val object_a_type = v.get(1).toString.toInt
        val object_id_b = v.get(2).toString
        val object_b_type = v.get(3).toString.toInt
        val time_sum = v.get(4).toString.toInt
        val seq_same_length = v.get(5).toString.toInt
        val first_time = v.get(6).toString
        val first_device = v.get(7).toString
        val last_time = v.get(8).toString
        val last_device = v.get(9).toString
        //((对象A，对象A类型， 对象B， 对象A类型)，(时间差之和，公共子序列长度，首次出现时间，首次出现设备，最近出现时间，最近出现设备))
        ((object_id_a, object_a_type, object_id_b, object_b_type), (time_sum, seq_same_length, first_time, first_device, last_time,  last_device))
      }
      .combineByKey(
        createCombiner = (value: (Int, Int, String, String,String, String)) => List[(Int, Int, String, String,String, String)](value),
        mergeValue = (list: List[(Int, Int, String, String,String, String)], value: (Int, Int, String, String,String, String)) => list:+ value,
        mergeCombiners = (list1: List[(Int, Int, String, String,String, String)], list2: List[(Int, Int, String, String,String, String)]) => list1 ::: list2
      )
      .mapValues{ row =>

        val timeSum = row.map(_._1).sum
        val seqSameLength = row.map(_._2).sum
        //首次出现时间，首次出现设备，最近出现时间，最近出现设备
        val timeList = row.map(r => (r._3, r._4, r._5, r._6)).sortWith(_._3 < _._3)

        //此处末次时间不对，有需要再改。find_by:cyl
        val firstTime: String = timeList.head._1
        val firstDevice: String = timeList.head._2
        val lastTime: String = timeList.last._3
        val lastDevice: String = timeList.last._4

        (timeSum, seqSameLength, firstTime, firstDevice, lastTime, lastDevice)
      }
      .filter(_._2._2 >= minImpactCount)
    //      .persist() //缓存，提高计算效率，数据量大可以考虑缓存到内存和磁盘的方式

  }


  def loadObjectSeqData(sparkSession: SparkSession, param: Param): RDD[(String, Int)] = {

    val tableDetail = new TableBean("", "hive",
      "", "", "", "", "default")

    val properties = PropertiesUtil.genJDBCProperties(param)

    val database = DataBaseFactory(sparkSession, properties, tableDetail.databaseType)
    val queryParamBean = new QueryBean()
    queryParamBean.isPage = false

    val minImpactCount = param.keyMap.getOrElse("minImpactCount", "2").toString.toInt
    logger.info(s"minImpactCount: ${minImpactCount}")

    //加载每天相似度数据
    val sql1 = param.keyMap(lcssLoadDaySeqSqlKey).toString
    val execSql = ReplaceSqlUtil.replaceSqlParam(sql1, param)

    //每天的进行合并
    val objectSeqRdd = database
      .query(execSql, queryParamBean)
      .rdd
      .map{m =>
        val object_id = m.getString(0)
        val object_length = m.getInt(1)
        (object_id ,object_length)
      }
      .reduceByKey( _ + _ )
      .filter(_._2 > minImpactCount)

    objectSeqRdd
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
  }

  /** *
    * 计算一段时间的总体相似度: 是否需要计算历史的相似度与当前时间段的相似结合计算？
    *
    * @param dataRdd ((对象A，对象A类型，对象B，对象B类型)，(时间差之和，公共子序列长度，首次出现时间，首次出现设备，最近出现时间，最近出现设备))
    * @param param
    * @return (对象A，对象A类型，对象B，对象B类型，相似度分数AB，相似度分数BA，相似度归一化分数，时间差之和，公共子序列长度，对象A子序列长度，对象B子序列长度，首次出现时间，首次出现设备，最近出现时间，最近出现设备)
    */
  def computeSim(param: Param,
                 dataRdd: RDD[((String, Int, String, Int), (Int, Int, String, String, String, String))],
                 objectSeqRdd: RDD[(String, Int)] )
  : RDD[Row] = {

    dataRdd.map{
             case ((object_id_a, object_a_type, object_id_b, object_b_type), (time_sum, seq_same_length, first_time, first_device,
                    last_time, last_device)) =>
              (object_id_a, (object_a_type, object_id_b, object_b_type, time_sum, seq_same_length, first_time, first_device,
                last_time, last_device))

           }
           .join(objectSeqRdd)
           .map{
              case (object_id_a, ((object_a_type, object_id_b, object_b_type, time_sum, seq_same_length, first_time, first_device,
              last_time, last_device), seq_length_a)) =>
                (object_id_b, (object_id_a, object_a_type, object_b_type, time_sum, seq_same_length, seq_length_a, first_time, first_device,
                  last_time, last_device))
           }
           .join(objectSeqRdd)
           .map{
              case (object_id_b, ((object_id_a, object_a_type, object_b_type, time_sum, seq_same_length, seq_length_a, first_time, first_device,
              last_time, last_device), seq_length_b)) =>

                val time_expect_value: Double = time_sum / seq_same_length
                val part2: Double = (1 / (1 + (time_expect_value / (param.keyMap("secondsSeriesThreshold").toString.toInt * 2)))) * param.mf.toString.toDouble
                val part1ab: Double = seq_same_length * 1.0 / seq_length_a * 1.0
                val sim_ab = (part1ab * part2).formatted("%.4f").toDouble

                //计算BA
                val part1ba: Double = seq_same_length * 1.0 / seq_length_b * 1.0
                val sim_ba = (part1ba * part2).formatted("%.4f").toDouble
                //调和函数计算
                val sim_harmonic_mean = ((2 * sim_ab * sim_ba) / (sim_ab + sim_ba)).formatted("%.4f").toDouble

                Row.fromTuple(object_id_a, object_a_type, object_id_b, object_b_type, sim_ab, sim_ba, sim_harmonic_mean,
                  time_sum, seq_same_length, seq_length_a, seq_length_b, first_time, first_device, last_time, last_device)
           }
  }

  def insertResultData(sparkSession: SparkSession,  param: Param , resultDF: RDD[Row]): Unit = {
    val params = "OBJECT_ID_A,TYPE_A,OBJECT_ID_B,TYPE_B,SIM_SCORE_AB, SIM_SCORE_BA,SIM_SCORE,TIME_SUM,SEQ_SAME_LENGTH,SEQ_LENGTH_A,SEQ_LENGTH_B,FIRST_TIME,FIRST_DEVICE,LAST_TIME,LAST_DEVICE"
    val schema = StructType(List(
      StructField("OBJECT_ID_A", StringType, nullable = true),
      StructField("TYPE_A", IntegerType, nullable = true),
      StructField("OBJECT_ID_B", StringType, nullable = true),
      StructField("TYPE_B", IntegerType, nullable = true),
      StructField("SIM_SCORE_AB", DoubleType, nullable = true),
      StructField("SIM_SCORE_BA", DoubleType, nullable = true),
      StructField("SIM_SCORE", DoubleType, nullable = true),
      StructField("TIME_SUM", IntegerType, nullable = true),
      StructField("SEQ_SAME_LENGTH", IntegerType, nullable = true),
      StructField("SEQ_LENGTH_A", IntegerType, nullable = true),
      StructField("SEQ_LENGTH_B", IntegerType, nullable = true),
      StructField("FIRST_TIME", StringType, nullable = true),
      StructField("FIRST_DEVICE", StringType, nullable = true),
      StructField("LAST_TIME", StringType, nullable = true),
      StructField("LAST_DEVICE", StringType, nullable = true)
    ))

    val df = sparkSession.createDataFrame(resultDF, schema)

    val outTable = new TableBean(resultTableName, "hive",
      "", "", "", "", "default")

    val properties = PropertiesUtil.genJDBCProperties(outTable)

    val database = DataBaseFactory(sparkSession, properties, outTable.databaseType)
    val dataBaseBean = database.getDataBaseBean
    dataBaseBean.dataBaseName = outTable.databaseName
    val saveParamBean: SaveParamBean = new SaveParamBean(outTable.tableName, outTable.outputType, dataBaseBean)
    saveParamBean.params = params
    saveParamBean.numPartitions = param.keyMap.getOrElse("numPartitions", "10").toString.toInt
    saveParamBean.partitions = Array(("STAT_DATE",s"${param.endTime.substring(0, 8)}"),("REL_TYPE", s"${param.relationType}"), ("TYPE",s"${param.releTypeStr}"))

    database.save(saveParamBean, df)


  }



  def process(param: Param): Unit = {
    var master = param.master

    //相似度计算结果的放大因子
    lcssLoadDayResultSqlKey = param.keyMap.getOrElse("LCSS_LOAD_DAY_SQL_RESULT_KEY","lcss.load.day.result").toString
    lcssLoadDaySeqSqlKey = param.keyMap.getOrElse("LCSS_LOAD_DAY_SEQ_SQL_KEY","lcss.load.day.seq").toString

    resultTableName = param.keyMap.getOrElse("RESULT_TABLE", "DM_LCSS").toString

    val batchId = param.keyMap.get("batchId").get.toString

    val sparkSession = SparkUtil.getSparkSession(master, s"LCSSResultHandler ${param.releTypeStr} ${batchId}  Job", param)

    //加载对象的子序列信息，求和序列长度
    val objectSeqRdd = loadObjectSeqData(sparkSession, param)

    //加载一段时间的数据
    val dataRdd = loadDayData(sparkSession, param)

    //计算一段时间两者之间的轨迹相似度
    val resultDF = computeSim(param, dataRdd, objectSeqRdd)

    //结果数据落库
    insertResultData(sparkSession, param, resultDF)

    sparkSession.close()

  }

}
