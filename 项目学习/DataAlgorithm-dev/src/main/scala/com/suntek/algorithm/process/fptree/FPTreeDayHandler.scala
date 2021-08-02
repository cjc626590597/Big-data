package com.suntek.algorithm.process.fptree

import java.io.{BufferedReader, FileInputStream, FileWriter, InputStreamReader}
import java.text.SimpleDateFormat
import java.util.{Properties, TimeZone}

import scala.collection.JavaConverters._
import com.suntek.algorithm.algorithm.association.fptreenonecpb.FPTreeNoneCpb
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

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * @author zhy
  * @date 2020-11-19 20:52
  */
object FPTreeDayHandler {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val tableName = "DM_FPTREE_FREREQUENT_ITEMS"
  val DISTRIBUTE_LOAD_DATA = "fptree.distribute.load.day"
  val CATEGORY1 = "category1"
  val CATEGORY2 = "category2"
  val DAYSUM = "daysSum"
  val MAXASSEMBLETOTAL = "maxAssembleTotal"
  //val TIMESERIESTHRESHOLD = "timeSeriesThreshold"
  val MINCOUNT = "minCount"
  val MINSUPPORT = "minSupport"

  def process(param: Param): Unit = {
    var category1 = param.releTypes(0)
    var category2 = param.releTypes(1)
    if(Constant.PRIORITY_LEVEL(category1) > Constant.PRIORITY_LEVEL(category2)){
      category1 = param.releTypes(1)
      category2 = param.releTypes(0)
    }

    var isDiffCategoryFlag = false
    if(!category1.equals(category2)){
      isDiffCategoryFlag = true
    }
    val batchId = param.keyMap("batchId").toString.substring(0, 8)
    val ss: SparkSession = SparkUtil.getSparkSession(param.master, s"${category1}_${category2}_FPTreeDayHandler_${batchId}", param)
    param.keyMap.put(CATEGORY1, category1)
    param.keyMap.put(CATEGORY2, category2)
    val daysSum = param.keyMap.getOrElse(DAYSUM, "7").toString.toInt
    val maxAssembleTotal = param.keyMap.getOrElse(MAXASSEMBLETOTAL, "25").toString.toInt
    var minCount = param.keyMap.getOrElse(MINCOUNT, "2").toString.toInt
    val minSupport = param.keyMap.getOrElse(MINSUPPORT, "0.3").toString.toDouble

    val (startDate, endDate, flag, taskStartDate) = startTask(param, daysSum)
    logger.info(s"$startDate, $endDate, $flag, $taskStartDate")
    if(flag == daysSum){
      if(minCount < 3) minCount = 3
      param.keyMap.put("minCount", minCount.toString)
    }
    logger.info(s"最小支持度: $minSupport; 最小支持次数：$minCount; 每个分片集合的最大数：$maxAssembleTotal, isDiffCategoryFlag:${isDiffCategoryFlag}")

    val sql =  param.keyMap(DISTRIBUTE_LOAD_DATA).toString
      .replaceAll("@startDate@", s"${startDate}00")
      .replaceAll("@endDate@", s"${endDate}23")
      .replaceAll("@type@", s"$category1-$category2")
    logger.info(sql)

    val data = loadData(ss, param, sql)
    //  data.collect().foreach(v=>println(v))

    val id1AndId2Rdd = genId1AndId2Records(ss, data, maxAssembleTotal)
    // id1AndId2Rdd.collect().foreach(v=>println(v))

    val ret = fPGrowthNoneCpb(ss, id1AndId2Rdd, minCount, minSupport, isDiffCategoryFlag)
    //  ret.collect().foreach(v=>println(v))

    //一项集
    val aFrequentItemset = getAFrequentItemset(ss, param, id1AndId2Rdd, ret)

    insertDataDetail(ss, aFrequentItemset, tableName, param, flag, endDate)

    endTask(param, taskStartDate, endDate)
    ss.close()
  }


  def startTask(param: Param,
                daysSum: Int)
  : (String, String, Int, String) = {
    val sdf = new SimpleDateFormat("yyyyMMdd")
    sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    val batchId = param.keyMap("batchId").toString.substring(0, 8)
    val runTimeStamp = sdf.parse(batchId).getTime -  60 * 60 * 1000L
    var startDate = sdf.format(runTimeStamp)
    val endDate = sdf.format(runTimeStamp)
    val filePath = getTaskFile(param)
    val taskFile = new java.io.File(filePath)
    var flag = 1
    var taskStartDate = startDate
    if(taskFile.exists()) {
      val lines = ArrayBuffer[String]()
      try {
        val inputReader = new InputStreamReader(new FileInputStream(taskFile))
        val bf = new BufferedReader(inputReader)
        var str = bf.readLine()
        // 按行读取字符串
        while (str != null) {
          lines.append(str)
          str = bf.readLine()
        }
        bf.close()
        inputReader.close()
      } catch{
        case ex: Exception => ex.printStackTrace()
      }
      if(lines.nonEmpty){
        val tmpDateS = lines.head.trim
        if(tmpDateS.nonEmpty && !tmpDateS.equals(endDate)) {
          taskStartDate = tmpDateS
          val runDays = (sdf.parse(endDate).getTime - sdf.parse(taskStartDate).getTime)/(24 * 3600 * 1000L) + 1
          logger.info(s"runDays = ${runDays}; daysSum = ${daysSum}")
          if (runDays > 0 && runDays % daysSum == 0) {
            startDate = sdf.format(runTimeStamp - ((daysSum - 1) * 24 * 3600 * 1000L))
            flag = daysSum
          }else if(runDays < 0){
            taskStartDate = endDate
            startDate = endDate
            flag = 1
          }
        }
      }
    }
    (startDate, endDate, flag, taskStartDate)
  }

  def getTaskFile(param: Param): String = {
    val category1 = param.keyMap(CATEGORY1).toString
    val category2 = param.keyMap(CATEGORY2).toString
    val path: String = System.getenv("ALG_DEPLOY_HOME")
    val fileName = s"relation_${category1}_$category2.txt"
    val filePath = s"$path/conf/$fileName"
    logger.info(filePath)
    filePath
  }

  def loadData(ss: SparkSession,
               param: Param,
               sql: String)
  : RDD[((String, String), (Long, Long, String))]  = {
    val databaseType = param.keyMap("databaseType").toString
    val dataBase = DataBaseFactory(ss, new Properties(), databaseType)
    val rdd1 = dataBase.query(sql, new QueryBean())
      .rdd
      .map(r=> {
        val id1 = r.get(0).toString
        val id2 = r.get(1).toString
        val id1_timeStamp = r.get(2).toString.toLong
        val id2_timeStamp = r.get(3).toString.toLong
        val device_id = r.get(4).toString
        ((id1, id2), (id1_timeStamp, id2_timeStamp, device_id)) //((id1，id2),(id1时间戳，id2时间戳，设备id))
      })
    val rdd2 = rdd1.map(v=>{
      (s"${v._1._1}_${v._1._2}_${Random.nextInt(5000)}", 1)
    })
      .reduceByKey(_+_)
      .map(v=> ((v._1.split("_")(0), v._1.split("_")(1)), v._2))
      .reduceByKey(_+_)
      .filter(_._2 > 1)
    val ret = rdd1.join(rdd2) //((id1，id2),(id1时间戳，id2时间戳，设备id))
      .map(v=> (v._1, v._2._1))
    // 因为分布表不会重复存储，当类型一样的时候特殊处理，ID1=A,ID2=B; ID1=B,ID2=A来存储
    /*if (!isDiffCategoryFlag){
      val ret1 = ret.map(v=>((v._1._2, v._1._1), (v._2._2, v._2._1, v._2._3)))
      ret = ret.union(ret1)
    }*/
    ret
  }

  def genId1AndId2Records(ss: SparkSession,
                          ret: RDD[((String, String), (Long, Long, String))],
                          maxAssembleTotal: Int)
  : RDD[((String, String, String), Long)]  =
  {
    //ret:  ((id1，id2),(id1时间戳，id2时间戳，设备id1，设备id2))
    val maxAssembleTotalBC = ss.sparkContext.broadcast(maxAssembleTotal)
    ret.map(v=>{
      val id1 = v._1._1
      val id2 = v._1._2
      val id1_timeStamp = v._2._1
      val id2_timeStamp = v._2._2
      val deviceId = v._2._3
      ((deviceId, id1, id1_timeStamp), (id2, id2_timeStamp))
    })
      .groupByKey()
      .flatMap(v=>{
        val deviceId = v._1._1
        val id1 = v._1._2
        var list = v._2
        if(v._2.size > maxAssembleTotalBC.value){
          //该设备下该时间分片对应的mac数超过50个，则按时间差排序缩小分片范围
          list = v._2.toList.map(r=>{
            val subTime = Math.abs(r._2 - v._1._3)
            (r._1, subTime)
          })
            .sortBy(_._2)
            .take(maxAssembleTotalBC.value)
        }
        list.map(r=>{
          ((deviceId, id1, r._1), v._1._3)//((设备，id1，id2), 时间戳)
        })
      })
      .distinct()
      .persist(StorageLevel.MEMORY_AND_DISK)
  }

  def fPGrowthNoneCpb(ss: SparkSession,
                      id1AndId2Rdd: RDD[((String, String, String), Long)],
                      minCount: Int,
                      minSupport: Double,
                      isDiffCategoryFlag: Boolean
                     )
  : RDD[((String, String), (Double, Integer, Double))] = {
    val isDiffCategoryFlagBC = ss.sparkContext.broadcast(isDiffCategoryFlag)
    id1AndId2Rdd
      .map(v=> (v._1._2, (v._1._1, v._1._3, v._2))) //(id1，(设备，id2, 时间戳) )
      .combineByKey((id: (String, String, Long)) => List[(String, String, Long)](id),
      (idList: List[(String, String, Long)], id: (String, String, Long)) => idList:+ id,
      (idList1: List[(String, String, Long)], idList2: List[(String, String, Long)]) => idList1:::idList2)
      .flatMap { v=>
        val id = v._1
        val matrix = v._2.distinct.map(v=>((v._1, v._3), v._2))
          .groupBy(_._1)
          .map(v=> v._2.map(_._2).asJava)
          .toList.asJava
        val fpTreeNoneCpb = new FPTreeNoneCpb()
        fpTreeNoneCpb.setMinCount(minCount)
        fpTreeNoneCpb.setIsFindMaxFrequentSet(false)
        fpTreeNoneCpb.setMinSupport(minSupport)
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
            val support = v.getCount * 1.0 / transformTable.getN
            // ((id1, id2),(支持度， 支持次数， 集合数))
            if(!isDiffCategoryFlagBC.value){
              if(id< v.getItem)((id, v.getItem), (support, v.getCount, transformTable.getN))
              else ((v.getItem, id), (support, v.getCount, transformTable.getN))
            }else{
              ((id, v.getItem), (support, v.getCount, transformTable.getN))
            }
          }
      }
  }

  def combineTimes(id1AndId2Rdd:  RDD[((String, String, String), Long)],
                   timeSeriesThresholdBC: Broadcast[Long])
  : RDD[((String, String), ArrayBuffer[ArrayBuffer[Long]])] = {
    // id1AndId2Rdd= ((设备1_设备2，id1，id2), id1时间戳)
    id1AndId2Rdd
      .map(v=>((v._1._1, v._1._2), v._2)) // ((设备1_设备2，id1), id1时间戳)
      .groupByKey()
      .map(v=>{
        //对同一个设备id的数据，时间差在timeseriesThreshold范围内的两个分片做压缩
        val timesArrayBuffer = new ArrayBuffer[ArrayBuffer[Long]]()
        var timesArray = new ArrayBuffer[Long]()
        val times = v._2.toList.distinct.sortBy(v=>v)
        var i = 0
        timesArray.append(times(i))
        while (i < times.size - 1){
          val j = i + 1
          if(times(j) - times(i) >= timeSeriesThresholdBC.value){
            timesArrayBuffer.append(timesArray)
            timesArray = new ArrayBuffer[Long]()
            timesArray.append(times(j))
            i = j
          }else{
            timesArray.append(times(j))
            i = i + 1
          }
        }
        if(timesArray.nonEmpty ){
          timesArrayBuffer.append(timesArray)
        }
        // ((设备1_设备2，id1), List[List[id1时间戳]]): 其中 List[id1时间戳] 为需要压缩的一个时间差组
        (v._1, timesArrayBuffer)
      })
  }

  def getAFrequentItemset(ss: SparkSession,
                          param: Param,
                          id1AndId2Rdd: RDD[((String, String, String), Long)],
                          ret: RDD[((String, String), (Double, Integer, Double))])
  : RDD[Row] ={
    // ret = ((id1, id2),(支持度， 支持次数， 集合数))
    // id1AndId2Rdd = ((设备，id1，id2), 时间戳)
    val category1 = param.keyMap(CATEGORY1).toString
    val category2 = param.keyMap(CATEGORY2).toString
    val category1Level = ss.sparkContext.broadcast(Constant.PRIORITY_LEVEL(category1))
    val category2Level = ss.sparkContext.broadcast(Constant.PRIORITY_LEVEL(category2))
    val rdd = id1AndId2Rdd.map(v=> ((v._1._2, v._1._3), (v._1._1, v._2))) // ((id1，id2), (设备，id1时间戳))
      .groupByKey()
      .map { v=>
        val list = v._2.toList.sortBy(_._2) // 从小到大
        //((id1，id2), (次数, 最早关联设备，最早时间， 最晚关联设备， 最晚时间))
        (v._1, (v._2.size, list.head._1, list.head._2, list.last._1, list.last._2))
      }
    ret.join(rdd)
       .map { v =>
          //id1,id1_type,id2,id2_type
          Row.fromTuple(v._1._1, category1Level.value.toString, v._1._2, category2Level.value.toString,
            //次数, 支持度, 首次关联时间, 首次关联设备ID, 最近关联时间, 最近关联设备ID, 集合数
            Integer.valueOf(v._2._1._2), v._2._1._1, v._2._2._3, v._2._2._2, v._2._2._5, v._2._2._4, v._2._1._3)
       }
  }

  def insertDataDetail(ss: SparkSession,
                       rowRdd: RDD[Row],
                       tableName: String,
                       param: Param,
                       flag: Int,
                       statDate: String)
  : Unit = {
    if(rowRdd == null){
      return
    }
    val category1 = param.keyMap(CATEGORY1).toString
    val category2 = param.keyMap(CATEGORY2).toString
    val databaseType = param.keyMap("databaseType").toString
    // (ID1, ID1类型, ID2, ID2类型, 支持度, 次数, 首次关联时间, 首次关联设备ID, 最近关联时间, 最近关联设备ID, 集合数)
    val params = "OBJECT_ID_A,TYPE_A,OBJECT_ID_B,TYPE_B,TIMES,SCORE,FIRST_TIME,FIRST_DEVICE,LAST_TIME,LAST_DEVICE,SET_NUM"
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
      StructField("LAST_DEVICE", StringType, nullable = true),
      StructField("SET_NUM", DoubleType, nullable = true)
    ))

    //转换成Row
    val df = ss.createDataFrame(rowRdd, schema)
    //df.show()

    val database = DataBaseFactory(ss, new Properties(), databaseType)
    val saveParamBean = new SaveParamBean(tableName, Constant.OVERRIDE_DYNAMIC, database.getDataBaseBean)
    saveParamBean.dataBaseBean =  database.getDataBaseBean
    saveParamBean.params = params
    saveParamBean.numPartitions = param.keyMap.getOrElse("numPartitions", "10").toString.toInt
    saveParamBean.partitions = Array[(String, String)](("CAL_TYPE", s"$flag"), ("TYPE", s"$category1-$category2"), ("STAT_DATE",s"$statDate"))
    database.save(saveParamBean, df)
  }

  def endTask(param: Param,
              startDate: String,
              endDate: String): Unit = {
    val filePath = getTaskFile(param)
    val fileWriter = new FileWriter(filePath, false)
    fileWriter.write(s"$startDate\n")
    fileWriter.write(s"$endDate")
    fileWriter.flush()
    fileWriter.close()
  }

}
