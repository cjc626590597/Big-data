package com.suntek.algorithm.process.dtw

import java.text.SimpleDateFormat
import java.util.{Properties, TimeZone}

import com.suntek.algorithm.algorithm.dtw.SparseDTW
import com.suntek.algorithm.common.bean.{QueryBean, SaveParamBean}
import com.suntek.algorithm.common.conf.{Constant, Param}
import com.suntek.algorithm.common.database.DataBaseFactory
import com.suntek.algorithm.common.util.SparkUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * @author zhy
  * @date 2020-11-20 16:13
  */
object DtwDayHandler {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val CATEGORY1 = "category1"
  val CATEGORY2 = "category2"
  val DISTRIBUTE_LOAD_DATA = "dtw.distribute.load.day"
  val DISTRIBUTE_LOAD_DATA_OBJECT = "dtw.distribute.load.day.object"
  val tableName = "DM_DTW_ITEMS"
  var timeSeconds = 600
  val weight = 0.4

  def processSub(param: Param, sparkSession: SparkSession): Unit ={

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
    logger.info(s"isDiffCategoryFlag: ${isDiffCategoryFlag}")

    var batchId = param.keyMap("batchId").toString.substring(0, 8)
    param.keyMap.put(CATEGORY1, category1)
    param.keyMap.put(CATEGORY2, category2)
    timeSeconds = param.keyMap.getOrElse("timeSeconds", "600").toString.toInt
    val sdf = new SimpleDateFormat("yyyyMMdd")
    sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    batchId = "20210519"
    val runTimeStamp = sdf.parse(batchId).getTime -  60 * 60 * 1000L
    val startDate = sdf.format(runTimeStamp)
    val endDate = sdf.format(runTimeStamp)

    val sql1 =  param.keyMap(DISTRIBUTE_LOAD_DATA_OBJECT).toString
      .replaceAll("@startDate@", s"${startDate}00")
      .replaceAll("@endDate@", s"${endDate}23")
      .replaceAll("@type@", s"$category1-$category2")
    logger.info(sql1)
    //((id,(List((设备id,时间戳),(设备id,时间戳)),Map((设备id->List(时间,时间))))
    //(粤Q17PA3-0,(List((440118626491017001,1621304472), (440118626491017001,1621308072), (440118626491017001,1621311672)),Map(440118626491017001 -> List(1621304472, 1621308072, 1621311672))))
    val objectRdd = loadObjectData(sparkSession, param, sql1) //((id，type),(时间戳，设备id))
    objectRdd.saveAsTextFile("output/DtwObjectRdd")

    val sql2 =  param.keyMap(DISTRIBUTE_LOAD_DATA).toString
      .replaceAll("@startDate@", s"${startDate}00")
      .replaceAll("@endDate@", s"${endDate}23")
      .replaceAll("@type@", s"$category1-$category2")
    logger.info(sql2)
    //((宁V4L2K9-0,粤GSJF16-0),(1621315272,1621315272,440118626491017001))
    val data = loadData(sparkSession, param, sql2) // ((id1，id2),(id1时间戳，id2时间戳，设备id))
    data.saveAsTextFile("output/DtwData")

    val objectID_AB = data.map(v=>(v._1._1, v._1._2)).distinct() //(id1, id2)

    // val objectID_A_RDD = getObjectID_A(data, isDiffCategoryFlag) //(id1, List(设备id, id1时间戳), Map[设备id, List[id1时间戳]])

    //val objectID_B_RDD =  if(!isDiffCategoryFlag) objectID_A_RDD else getObjectID_B(data)  //(id1, List(设备id, id1时间戳), Map[设备id, List[id1时间戳]])

    val rdd =  objectRdd
      .join(objectID_AB)
      .map(v=> (v._2._2,(v._2._1, v._1))) // (id2, (List(设备id, id1时间戳), id1))
      .join(objectRdd)//(id2, (  (List(设备id, id1时间戳), id1),      (List(设备id, id2时间戳))    )    )
      .map(v=>{
        val objectA = v._2._1._2
        val objectB = v._1
        val objectAList = v._2._1._1._1
        val objectAMap = v._2._1._1._2
        val objectBList = v._2._2._1
        val objectBMap = v._2._2._2
        val (trackSubListA, numsA) = makeDtwArray(objectAList, objectBMap) // numsA 为 A对B没有匹配到的点
        val (trackSubListB, numsB) = makeDtwArray(objectBList, objectAMap)  // numsB 为 B对A没有匹配到的点
        (objectA, objectB, trackSubListA, trackSubListB, numsA * 1.0 / objectAList.size * 1.0,
          numsB * 1.0 / objectBList.size * 1.0, numsA, numsB)
      })


    val weightBC = sparkSession.sparkContext.broadcast(weight)

    val retDtw = rdd.map(v=>{
      val dtwListA = v._3.map(r=>{
        if(r._1.size == 1 && r._2.size == 1){
          SparseDTW.euclideanDistance(r._1.head, r._2.head, timeSeconds)
        }else{
          SparseDTW.spDTW_v2(r._1.toArray, r._2.toArray, timeSeconds)
        }
      })
      val costA = dtwListA.sum.formatted("%.3f").toDouble
      val dtwListB = v._4.map(r=>{
        if(r._1.size == 1 && r._2.size == 1){
          SparseDTW.euclideanDistance(r._1.head, r._2.head, timeSeconds)
        }else{
          SparseDTW.spDTW_v2(r._1.toArray, r._2.toArray, timeSeconds)
        }
      })
      val costB = dtwListB.sum.formatted("%.3f").toDouble
      val gapAPre = v._5.formatted("%.3f").toDouble // A没有匹配的点占比
      val gapBPre = v._6.formatted("%.3f").toDouble //B没有匹配的点占比
      val scoreA = (weightBC.value * costA + (1.0 - weightBC.value) * (1.0 - gapAPre)  * costA).formatted("%.3f").toDouble
      val scoreB =  (weightBC.value * costB + (1.0 - weightBC.value) * (1.0 - gapBPre)  * costB).formatted("%.3f").toDouble
      // ((id1, id2), (A综合相似度，B综合相似度, A没有匹配的点占比，B没有匹配的点占比，A没有匹配的点个数， B没有匹配的点个数，分数A, 分数B))
      ((v._1, v._2), (scoreA, scoreB, gapAPre, gapBPre, v._7, v._8, costA, costB))
    })
    // .persist(StorageLevel.MEMORY_AND_DISK)

    //val valueRdd = retDtw.map(v=> ("key", ((v._2._1, v._2._2), (v._2._1, v._2._2)))).distinct()
    /*val min_max = ss.sparkContext.broadcast(getMaxMin(valueRdd))
    logger.info(s"最大值${min_max.value._2}，最小值${min_max.value._1}")
    */

    val retDetail = data
      .groupByKey()
      .map(v=>{
        val key = v._1
        val list = v._2.toList.sortBy(_._1)
        (key, (list.head._1, list.head._3, list.last._1, list.last._3, list.size))
      })

    val category1Level = sparkSession.sparkContext.broadcast(Constant.PRIORITY_LEVEL(category1))
    val category2Level = sparkSession.sparkContext.broadcast(Constant.PRIORITY_LEVEL(category2))

    val ret = retDetail
      .join(retDtw)
      .map(v=>{
        // id1, id1类型， id2, id2类型，分数，最早时间戳，最早关联设备，最晚时间戳，最晚关联设备
        val gapA = v._2._2._5 // A没有匹配的点个数
        val gapB = v._2._2._6 // B没有匹配的点个数
        val gapAPre = v._2._2._3 // A没有匹配的点占比
        val gapBPre = v._2._2._4 //B没有匹配的点占比
        val scoreA = v._2._2._7
        val scoreB = v._2._2._8
        // val scoreA = (v._2._2._1 - min_max.value._1._1) / (min_max.value._2._1 - min_max.value._1._1)
        // val scoreB = (v._2._2._2 - min_max.value._1._2) / (min_max.value._2._2 - min_max.value._1._2)
        val score = (scoreA + scoreB) / 2
        Row.fromTuple((v._1._1, category1Level.value.toString, v._1._2, category2Level.value.toString,
          v._2._1._5, score.formatted("%.3f").toDouble, v._2._1._1, v._2._1._2, v._2._1._3, v._2._1._4,
          gapA * 1.0, gapAPre.formatted("%.3f").toDouble,
          gapB * 1.0, gapBPre.formatted("%.3f").toDouble,
          scoreA.formatted("%.3f").toDouble, scoreA.formatted("%.3f").toDouble,
          scoreB.formatted("%.3f").toDouble, scoreB.formatted("%.3f").toDouble))
      })
      .coalesce(param.keyMap.getOrElse("numPartitions", "10").toString.toInt, false)

    insertDataDetail(sparkSession, ret, tableName, param, endDate)
  }

  def process(param: Param): Unit = {
    var category1 = param.releTypes(0)
    var category2 = param.releTypes(1)
    if(Constant.PRIORITY_LEVEL(category1) > Constant.PRIORITY_LEVEL(category2)){
      category1 = param.releTypes(1)
      category2 = param.releTypes(0)
    }
    val batchId = param.keyMap("batchId").toString.substring(0, 8)
    val ss: SparkSession = SparkUtil.getSparkSession(param.master, s"${category1}_${category2}_DtwDayHandler_${batchId}", param)

    processSub(param, ss)
    ss.close()
  }

  def getMaxMin(rdd: RDD[(String, ((Double, Double),(Double, Double)))])
  : ((Double, Double), (Double, Double)) = {
    rdd
      .reduceByKey((x, y) => {
        val min1 = Math.min(x._1._1, y._1._1)
        val max1 = Math.max(x._2._1, y._2._1)
        val min2 = Math.min(x._1._2, y._1._2)
        val max2 = Math.max(x._2._2, y._2._2)
        ((min1, min2), (max1, max2))
      })
      .collect()
      .head
      ._2
  }

  def makeDtwArray(objectAList: List[(String, Long)],
                   objectBMap:Map[String, List[Long]])
  : (List[(List[Long], List[Long])], Int) = {
    var nums = 0
    val travel = ArrayBuffer[(List[Long], List[Long])]()
    val travelA = ArrayBuffer[Long]()
    val travelB = ArrayBuffer[Long]()
    objectAList.foreach(r1 =>{
      val deviceId1 = r1._1
      val timeStamp1 = r1._2
      if(objectBMap.contains(deviceId1)){
        val minTimeStamp = objectBMap(deviceId1).map(r2 => (r2, Math.abs(r2 - r1._2))).minBy(_._2)
        if(minTimeStamp._2 > timeSeconds) {
          nums += 1
          if(travelA.size > 0 && travelB.size > 0){
            travel.append((travelA.toList, travelB.toList))
          }
          travelA.clear()
          travelB.clear()
        }else{
          travelA.append(timeStamp1)
          travelB.append( minTimeStamp._1)
        }
      }else{
        nums += 1
        if(travelA.size > 0 && travelB.size > 0){
          travel.append((travelA.toList, travelB.toList))
        }
        travelA.clear()
        travelB.clear()
      }
    })
    if(travelA.size > 0 && travelB.size > 0){
      travel.append((travelA.toList, travelB.toList))
    }
    (travel.toList, nums)
  }

  def getObjectID_A(data: RDD[((String, String), (Long, Long, String))],
                    isDiffCategoryFlag: Boolean)
  : RDD[(String, (List[(String, Long)], Map[String, List[Long]]))] = {
    var ret = data.map(v=>(v._1._1, (v._2._3, v._2._1)))
    if(!isDiffCategoryFlag){
      ret = data.flatMap(v=>List((v._1._1, (v._2._3, v._2._1)), (v._1._2, (v._2._3, v._2._2))))
    }
    ret.groupByKey()
      .map(v=>{
        val list = v._2.toList.distinct.sortBy(_._2)
        val map =  list.groupBy(_._1).map(v=>(v._1, v._2.map(_._2)))
        (v._1, (list, map))
      }) //(id1, List(设备id, id1时间戳), Map[设备id, List[id1时间戳]])
  }

  def getObjectID_B(data: RDD[((String, String), (Long, Long, String))])
  :RDD[(String, (List[(String, Long)], Map[String, List[Long]]))] = {
    data.map(v=>(v._1._2, (v._2._3, v._2._2)))
      .groupByKey()
      .map(v=>{
        val list = v._2.toList.distinct.sortBy(_._2)
        val map =  list.groupBy(_._1).map(v=>(v._1, v._2.map(_._2)))
        (v._1, (list, map))
      }) //(id1, List(设备id, id1时间戳), Map[设备id, List[id1时间戳]])
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
    val params = "OBJECT_ID_A,TYPE_A,OBJECT_ID_B,TYPE_B,TIMES,SCORE,FIRST_TIME,FIRST_DEVICE,LAST_TIME,LAST_DEVICE," +
      "GAP_A,MATCH_A_PER,GAP_B,MATCH_B_PER,COST_A,COST_A_NORMAL,COST_B,COST_B_NORMAL"
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
      StructField("GAP_A", DoubleType, nullable = true),
      StructField("MATCH_A_PER", DoubleType, nullable = true),
      StructField("GAP_B", DoubleType, nullable = true),
      StructField("MATCH_B_PER", DoubleType, nullable = true),
      StructField("COST_A", DoubleType, nullable = true),
      StructField("COST_A_NORMAL", DoubleType, nullable = true),
      StructField("COST_B", DoubleType, nullable = true),
      StructField("COST_B_NORMAL", DoubleType, nullable = true)
    ))

    //转换成Row
    val df = ss.createDataFrame(rowRdd, schema)
    // df.show()

    val database = DataBaseFactory(ss, new Properties(), databaseType)
    val saveParamBean = new SaveParamBean(tableName, Constant.OVERRIDE_DYNAMIC, database.getDataBaseBean)
    saveParamBean.dataBaseBean =  database.getDataBaseBean
    saveParamBean.params = params
    saveParamBean.numPartitions = param.keyMap.getOrElse("numPartitions", "10").toString.toInt
    saveParamBean.partitions = Array[(String, String)](("STAT_DATE", s"$statDate"), ("TYPE", s"$category1-$category2"))
    database.save(saveParamBean, df)
  }

  def loadObjectData(ss: SparkSession,
               param: Param,
               sql: String)
  : RDD[(String, (List[(String, Long)], Map[String, List[Long]]))] = {
    val databaseType = param.keyMap("databaseType").toString
    val dataBase = DataBaseFactory(ss, new Properties(), databaseType)
    //SELECT DISTINCT OBJECT_ID, TIMESTAMP_T, DEVICE_ID
    val rdd: RDD[(String, (List[(String, Long)], Map[String, List[Long]]))] = dataBase.query(sql, new QueryBean())
      .rdd
      .map(r => {
        try {
          val object_id = r.get(0).toString
          val object_timeStamp = r.get(1).toString.toLong
          val device_id = r.get(2).toString
          (object_id, (device_id, object_timeStamp)) //(id,(设备id, 时间戳))
        } catch {
          case _: Exception => {
            ("", ("", 0L))
          }
        }
      })
      .filter(_._1.nonEmpty)
      .groupByKey()
      .map(v => {
        val list = v._2.toList.distinct.sortBy(_._2)
        val map = list.groupBy(_._1).map(v => (v._1, v._2.map(_._2)))
        (v._1, (list, map))
      })
    //(粤Q17PA3-0,(List((440118626491017001,1621304472), (440118626491017001,1621308072), (440118626491017001,1621311672)),Map(440118626491017001 -> List(1621304472, 1621308072, 1621311672))))
    rdd
  }

  def loadData(ss: SparkSession,
               param: Param,
               sql: String)
  : RDD[((String, String), (Long, Long, String))]  = {
    val databaseType = param.keyMap("databaseType").toString
    val dataBase = DataBaseFactory(ss, new Properties(), databaseType)
    val minImpactCount = param.keyMap.getOrElse("minImpactCount", "1").toString.toInt
    logger.info(s"minImpactCount: $minImpactCount")
    //SELECT OBJECT_ID_A, OBJECT_ID_B, TIMESTAMP_A, TIMESTAMP_B, DEVICE_ID
    //[宁V4L2K9-0,粤GSJF16-0,1621315272,1621315272,440118626491017001]
    val rdd1 = dataBase.query(sql, new QueryBean())
      .rdd
      .map(r=> {
        try{
          val id1 = r.get(0).toString
          val id2 = r.get(1).toString
          val id1_timeStamp = r.get(2).toString.toLong
          val id2_timeStamp = r.get(3).toString.toLong
          val device_id = r.get(4).toString
          ((id1, id2), (id1_timeStamp, id2_timeStamp, device_id)) //((id1，id2),(id1时间戳，id2时间戳，设备id))
        }catch {
          case _:Exception =>
            (("", ""), (0L, 0L, ""))
        }
      })
        .filter(_._1._1.nonEmpty)
    val rdd2 = rdd1.collect()
    if(minImpactCount >= 2){
      val rdd2 = rdd1.map(v=>{(s"${v._1._1}_${v._1._2}_${Random.nextInt(5000)}", 1)})
        .reduceByKey(_+_)
        .map(v=> ((v._1.split("_")(0), v._1.split("_")(1)), v._2))
        .reduceByKey(_+_)
        .filter(_._2 >= minImpactCount)
      val rdd = rdd1.join(rdd2) //((id1，id2),(id1时间戳，id2时间戳，设备id))
        .map(v=> (v._1, v._2._1))
      return rdd.persist(StorageLevel.MEMORY_AND_DISK)
    }else{
      return rdd1
    }
  }
}
