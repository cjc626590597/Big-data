package com.suntek.algorithm.fusion

import com.alibaba.fastjson.JSONObject
import com.suntek.algorithm.common.bean.{QueryBean, SaveParamBean}
import com.suntek.algorithm.common.conf.{Constant, Param}
import com.suntek.algorithm.common.database.DataBaseFactory
import com.suntek.algorithm.common.util.{OtherDBTaleLoadUtil, SparkUtil}
import com.suntek.algorithm.evaluation.DataManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}
import java.text.SimpleDateFormat
import java.util.{Properties, TimeZone}

import scala.collection.mutable

/**
 *1.人员Id与车辆(主副驾驶)关联分析
 *2.人脸ID与车辆的关联分析
 * @author chenb
 * */
//noinspection SpellCheckingInspection

object PersonCarDriverHandler {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val CATEGORY1 = "category1"
  val CATEGORY2 = "category2"
  val NUMPARTITIONS = "numPartitions"
  val FACE_DETECT_LOAD_DAY = "face.detect.load.day"
  val VEHICLE_FRONT_LOAD_DAY = "vehicle.front.load.day"
  val PERSON_CAR_LOAD_ALL = "person.car.load.all"
  val DM_COMMON_STAT_DAY_TABLE_NAME = "DM_COMMON_STAT_DAY"
  val DM_COMMON_STAT_TABLE_NAME = "DM_COMMON_STAT"
  val DM_RELATION_TABLE_NAME = "DM_RELATION"
  val RELATION_MPPDB_TABLE_NAME = "FUSION_RELATION_INFO"

  val relationMap: mutable.Map[String, Object] = DataManager.loadRelation()

  @throws[Exception]
  def process(param: Param): Unit = {

    var category1 = param.releTypes(0)
    var category2 = param.releTypes(1)
    if (Constant.PRIORITY_LEVEL(category1) > Constant.PRIORITY_LEVEL(category2)) {
      category1 = param.releTypes(1)
      category2 = param.releTypes(0)
    }

    param.keyMap.put(CATEGORY1, category1)
    param.keyMap.put(CATEGORY2, category2)

    val batchId = param.keyMap("batchId").toString.substring(0, 8)
    val spark: SparkSession = SparkUtil.getSparkSession(param.master, s"PersonCarDriverHandler_$batchId", param)

    val numPartitions = param.keyMap.getOrElse(NUMPARTITIONS, "10").toString.toInt

    val sdf = new SimpleDateFormat("yyyyMMdd")
    sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    val runTimeStamp = sdf.parse(batchId).getTime - 60 * 60 * 1000L
    val startDate = sdf.format(runTimeStamp)
    val statDate = sdf.format(sdf.parse(batchId).getTime).substring(2)

    val runTimeStampStat = sdf.parse(batchId).getTime - 25 * 60 * 60 * 1000L
    val startDateStat = sdf.format(runTimeStampStat)

    val sql1 = param.keyMap(VEHICLE_FRONT_LOAD_DAY).toString
                    .replaceAll("@startDate@", startDate.substring(2))

    //加载主副驾驶数据
    val vehicleFrontData = loadVehicleFrontData(spark, param, sql1) //driverInfoId, (driverType, hphm, hpys),
    //driverType 1 主驾 2 副驾
    val sql2 = param.keyMap(FACE_DETECT_LOAD_DAY).toString
                    .replaceAll("@startDate@", startDate.substring(2))

    val facedetectData = loadFaceDetectData(spark, param, sql2) // info_id,(person_id,faceId,shot_time)
    // 按天处理数据
    val dataretDay = combineStatDayData(spark, param, vehicleFrontData, facedetectData, statDate, numPartitions)
    // 数据写入日表
    insertStatDayData(spark, dataretDay, category1, category2, DM_COMMON_STAT_DAY_TABLE_NAME, param, startDate)

    // 获取前一个批次汇总数据,并与当前批次汇总
    val sql3 = param.keyMap(PERSON_CAR_LOAD_ALL).toString
                    .replaceAll("@startDate@", startDateStat)
                    .replaceAll("@type@", s"$category1-$category2")
                    .replaceAll("@relationType@", s"${param.relationType}")

    val personCarStatData = loadPersonCarStatData(spark, param, sql3)
    // 汇总数据
    val dataret = combineStatData(dataretDay, personCarStatData)
    // 数据写入总表
    insertStatData(spark, category1, category2, dataret, DM_COMMON_STAT_TABLE_NAME, param, startDate)

    // 写入关系结果表
    //加载车档数据，生成personId与车档Id的关系数据
    val count = dataret.count()
    if (count > 0) {
      val carArchiveRdd = OtherDBTaleLoadUtil.loadCarArchiveData(spark, param)
      insertRelationData(spark, category1, category2, dataret, carArchiveRdd, DM_RELATION_TABLE_NAME, param, startDate)
    }
    spark.close()
  }

  @throws[Exception]
  def loadVehicleFrontData(spark: SparkSession,
                           param: Param,
                           sql: String)
  : RDD[(String, (Int, String, String))] = {
    try {
      val databaseType = param.keyMap.getOrElse("databaseType","hive").toString
      logger.info("loadVehicleFrontData: " + sql)
      val dataBase = DataBaseFactory(spark, new Properties(), databaseType)
      dataBase.query(sql, new QueryBean())
        .rdd
        //DRIVER_FACE_INFO_ID,CO_DRIVER_FACE_INFO_ID,PLATE_NO,PLATE_COLOR,PASS_TIME
        .flatMap { r =>
          try {
            val driverInfoId = r.get(0).toString
            val coDriverInfoId = r.get(1).toString
            val hphm = r.get(2).toString
            val hpys = r.get(3).toString
            val jgsk = r.get(4).toString
            //driverType 1 主驾 2 副驾
            List[(String, (Int, String, String))]((driverInfoId, (1, s"${hphm}-${hpys}", jgsk)),
              (coDriverInfoId, (2, s"${hphm}-${hpys}", jgsk)))
            // 1 主驾 2 副驾
          } catch {
            case _: Exception =>
              List[(String, (Int, String, String))]()
          }
        }
        .filter(_._1.nonEmpty)
    } catch {
      case ex: Exception =>
        logger.error(ex.getMessage, ex)
        throw new Exception(ex.getMessage)
    }
  }

  @throws[Exception]
  def loadFaceDetectData(spark: SparkSession,
                         param: Param,
                         sql: String)
  : RDD[(String, (String, Long))] = {

    try {
      val databaseType = param.keyMap.getOrElse("databaseType","hive").toString
      logger.info("faceDetectData: " + sql)
      val dataBase = DataBaseFactory(spark, new Properties(), databaseType)
      dataBase.query(sql, new QueryBean())
        .rdd
        //person_id,face_id,info_id,device_id,shot_time
        .map { r =>
          try {
            val personId = r.get(0).toString
            val infoId = r.get(1).toString
            //                  val deviceId = r.get(2).toString
            val shotTime = r.get(3).toString.toLong
            (infoId, (personId, shotTime))
          } catch {
            case _: Exception =>
              ("", ("", 0L))
          }
        }
        .filter(_._1.nonEmpty)
    } catch {
      case ex: Exception =>
        logger.error(ex.getMessage, ex)
        throw new Exception(ex.getMessage)
    }
  }

  @throws[Exception]
  def loadPersonCarStatData(spark: SparkSession,
                            param: Param,
                            sql: String)
  : RDD[((String, String, Int), (String, String, Int))] = {

    try {
      val databaseType = param.keyMap.getOrElse("databaseType","hive").toString
      logger.info("personCarStatData: " + sql)
      val dataBase = DataBaseFactory(spark, new Properties(), databaseType)
      dataBase.query(sql, new QueryBean())
        .rdd
        .map { r =>
          try {
            // OBJECT_ID_A,OBJECT_ID_B,CNT,REMARK,FIRST_TIME,LAST_TIME
            val personId = r.get(0).toString
            val hphm = r.get(1).toString
            val cnt = r.get(2).toString.toInt
            val driverType = r.get(3).toString.toInt
            val firstTime = r.get(4).toString
            val lastTime = r.get(5).toString
            ((personId, hphm, driverType), (firstTime, lastTime, cnt))
          } catch {
            case _: Exception =>
              (("", "", 0), ("", "", 0))
          }
        }
        .filter(_._1._1.nonEmpty)
    } catch {
      case ex: Exception =>
        logger.error(ex.getMessage, ex)
        throw new Exception(ex.getMessage)
    }
  }

  def combineStatDayData(spark: SparkSession,
                         param: Param,
                         vehicleFrontData: RDD[(String, (Int, String, String))],
                         facedetectData: RDD[(String, (String, Long))],
                         statDate: String,
                         numPartitions: Int
                        ): RDD[((String, String, Int), (String, String, Int))] = {

    // 人脸 - 车(主副驾驶) 数据写入 dm_relation
    val category1 = "car"
    val category2 = "face"
    val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
    sdf.setTimeZone(TimeZone.getTimeZone(param.zoneId))
    //关系类型
    //    val relationTypeBc = spark.sparkContext.broadcast(if (category1 != category2) 1 else 2)
    //    val relIdBc = spark.sparkContext.broadcast(relationMap.getOrElse(s"$category1-$category2", "03-0003-0006").toString)
    //    val category1Level = spark.sparkContext.broadcast(Constant.PRIORITY_LEVEL(category1))
    //    val category2Level = spark.sparkContext.broadcast(Constant.PRIORITY_LEVEL(category2))

    //关联得到viid_object_id和车牌的关系，先不入库，先让应用直接查就行了！
    //    val faceCarRow = vehicleFrontData
    //      .map { case (faceId, (driverType, hphm, jgsk)) =>
    //        val jsonObject = new JSONObject()
    //        jsonObject.put("DRIVER_TYPE", driverType) //驾驶类型 1 主驾 2 副驾
    //        // ID1,ID1_TYPE,ID1_ATTR,ID2,ID2_TYPE,ID2_ATTR
    //        Row.fromTuple(hphm, category1Level.value, "", faceId, category2Level.value, "",
    //          // REL_ID,REL_TYPE,REL_ATTR,REL_GEN_TYPE
    //          relIdBc.value, relationTypeBc.value, jsonObject.toJSONString, 2,
    //          // FIRST_TIME,LAST_TIME,OCCUR_NUM,SCORE,FIRST_DEVICE_ID,LAST_DEVICE_ID,STAT_DATE,INSERT_TIME
    //          jgsk.toLong, jgsk.toLong, 1, 100, "", "", statDate, sdf.format(new Date()))
    //      }
    //     // 数据写入睿帆雪球或者华为mppdb
    //    insertFaceCarRelationData(spark, faceCarRow.coalesce(numPartitions = numPartitions), relationMPPDBTableName, param, statDate, relIdBc.value)

    // 按日处理数据，将人脸整合表数据与主副驾驶数据根据infoId进行关联，
    // 得到personId和车牌之间的关系，再通过车档数据获取得到车档ID，最终得到personId与车档Id的数据

    val joinData = facedetectData.join(vehicleFrontData)

    joinData.map {
      case (_, ((personId, shotTime), (driverType, hphm, jgsk))) =>
        ((personId, hphm, driverType), jgsk)
    }
      .distinct()
      .groupByKey()
      .map { case ((personId, hphm, driverType), snapInfoIter) =>
        val snapInfoList = snapInfoIter.toList
        ((personId, hphm, driverType), (snapInfoList.min.toString, snapInfoList.max.toString, snapInfoList.length))
      }
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
  }

  def combineStatData(
                       dataretDay: RDD[((String, String, Int), (String, String, Int))],
                       personCarStatData: RDD[((String, String, Int), (String, String, Int))]
                     ): RDD[(String, String, String, Int, String, String)] = {

    dataretDay.cogroup(personCarStatData)
      .map { case ((personId, hphm, driverType), (snapInfoIter1, snapInfoIter2)) =>
        if (snapInfoIter1.isEmpty) {
          val (firstTime, lastTime, cnt) = snapInfoIter2.toList.head
          (personId, hphm, driverType.toString, cnt, firstTime, lastTime)
        } else if (snapInfoIter2.isEmpty) {
          val (firstTime, lastTime, cnt) = snapInfoIter1.toList.head
          (personId, hphm, driverType.toString, cnt, firstTime, lastTime)
        } else {
          val snapInfoList = snapInfoIter1.toList ::: snapInfoIter2.toList
          val (firstTime, lastTime, cnt) = (snapInfoList.map(_._1).min, snapInfoList.map(_._2).max, snapInfoList.map(_._3).sum)
          (personId, hphm, driverType.toString, cnt, firstTime, lastTime)
        }
      }
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
  }

  def insertFaceCarRelationData(
                                 spark: SparkSession,
                                 dataRow: RDD[Row],
                                 tableName: String,
                                 param: Param,
                                 statDate: String,
                                 relId: String,
                                 outputType: Int = Constant.OVERRIDE): Unit = {

    val params =
      """ID1,ID1_TYPE,ID1_ATTR,
                    ID2,ID2_TYPE,ID2_ATTR,
                    REL_ID,REL_TYPE,REL_ATTR,REL_GEN_TYPE,
                    FIRST_TIME,LAST_TIME,
                    OCCUR_NUM,SCORE,
                    FIRST_DEVICE_ID,LAST_DEVICE_ID,
                    STAT_DATE,INSERT_TIME
                 """
    val schema = StructType(List[StructField](
      StructField("ID1", StringType, nullable = true),
      StructField("ID1_TYPE", IntegerType, nullable = true),
      StructField("ID1_ATTR", StringType, nullable = true),

      StructField("ID2", StringType, nullable = true),
      StructField("ID2_TYPE", IntegerType, nullable = true),
      StructField("ID2_ATTR", StringType, nullable = true),

      StructField("REL_ID", StringType, nullable = true),
      StructField("REL_TYPE", IntegerType, nullable = true),
      StructField("REL_ATTR", StringType, nullable = true),
      StructField("REL_GEN_TYPE", IntegerType, nullable = true),

      StructField("FIRST_TIME", LongType, nullable = true),
      StructField("LAST_TIME", LongType, nullable = true),

      StructField("OCCUR_NUM", IntegerType, nullable = true),
      StructField("SCORE", IntegerType, nullable = true),

      StructField("FIRST_DEVICE_ID", StringType, nullable = true),
      StructField("LAST_DEVICE_ID", StringType, nullable = true),

      StructField("STAT_DATE", StringType, nullable = true),
      StructField("INSERT_TIME", StringType, nullable = true)
    ))

    var df = spark.createDataFrame(dataRow, schema)

    val landDataBase = param.keyMap.getOrElse("landDataBase", "pd_dts").toString
    val preSql = s"delete from $tableName where stat_date='$statDate' and rel_id='$relId'".toUpperCase
    val (databaseType, subProtocol, queryParam) =
      if (param.mppdbType == "snowballdb") { //数据落地于睿帆snowballdb
        df = df.drop("INSERT_TIME") // 删除INSERT_TIME字段
        (Constant.SNOWBALLDB, Constant.SNOWBALL_SUB_PROTOCOL, "socket_timeout=3000000")
      } else { //数据落地于华为mppdbc
        (Constant.GAUSSDB, Constant.GAUSSDB_SUB_PROTOCOL, "characterEncoding=UTF-8&autoReconnect=true&useSSL=false&zeroDateTimeBehavior=convertToNull&serverTimezone=UTC")
      }
    val properties = new Properties()

    val url = s"jdbc:$subProtocol://${param.mppdbIp}:${param.mppdbPort}/$landDataBase?$queryParam"
    logger.info(param.mppdbType)
    logger.info(url)
    logger.info(param.mppdbUsername)
    logger.info(param.mppdbPassword)
    properties.setProperty("jdbc.url", url)
    properties.setProperty("username", param.mppdbUsername)
    properties.setProperty("password", param.mppdbPassword)

    val database = DataBaseFactory(spark, properties, databaseType)
    val saveParamBean = new SaveParamBean(tableName, outputType, database.getDataBaseBean)
    saveParamBean.dataBaseBean = database.getDataBaseBean
    saveParamBean.params = params
    saveParamBean.preSql = preSql
    saveParamBean.tableName = tableName
    database.save(saveParamBean, df)
  }


  def insertStatDayData(spark: SparkSession,
                        dataret: RDD[((String, String, Int), (String, String, Int))],
                        category1: String,
                        category2: String,
                        tableName: String,
                        param: Param,
                        statDate: String,
                        outputType: Int = Constant.OVERRIDE_DYNAMIC)
  : Unit = {

    val category1Level = spark.sparkContext.broadcast(Constant.PRIORITY_LEVEL(category1))
    val category2Level = spark.sparkContext.broadcast(Constant.PRIORITY_LEVEL(category2))

    val databaseType = param.keyMap.getOrElse("databaseType","hive").toString
    val params = "OBJECT_ID_A,TYPE_A,OBJECT_ID_B,TYPE_B,CNT,REMARK,FIRST_TIME,LAST_TIME"
    val schema = StructType(List[StructField](
      StructField("OBJECT_ID_A", StringType, nullable = true),
      StructField("TYPE_A", IntegerType, nullable = true),
      StructField("OBJECT_ID_B", StringType, nullable = true),
      StructField("TYPE_B", IntegerType, nullable = true),
      StructField("CNT", IntegerType, nullable = true),
      StructField("REMARK", StringType, nullable = true),
      StructField("FIRST_TIME", StringType, nullable = true),
      StructField("LAST_TIME", StringType, nullable = true)
    ))

    val dataRow = dataret.map { case ((personId, hphm, driverType), (firstTime, lastTime, cnt)) =>
      Row.fromTuple(personId, category1Level.value, hphm, category2Level.value, cnt, driverType.toString, firstTime, lastTime)
    }
    val df = spark.createDataFrame(dataRow, schema)

    val database = DataBaseFactory(spark, new Properties(), databaseType)
    val saveParamBean = new SaveParamBean(tableName, outputType, database.getDataBaseBean)
    saveParamBean.dataBaseBean = database.getDataBaseBean
    saveParamBean.params = params

    saveParamBean.partitions = Array[(String, String)](
      ("STAT_DATE", statDate),
      ("TYPE", param.releTypeStr)
    )
    database.save(saveParamBean, df)
  }

  def insertStatData(
                      spark: SparkSession,
                      category1: String,
                      category2: String,
                      dataret: RDD[(String, String, String, Int, String, String)],
                      tableName: String,
                      param: Param,
                      statDate: String,
                      outputType: Int = Constant.OVERRIDE_DYNAMIC)
  : Unit = {

    val databaseType = param.keyMap.getOrElse("databaseType","hive").toString
    val params = "OBJECT_ID_A,TYPE_A,OBJECT_ID_B,TYPE_B,CNT,REMARK,FIRST_TIME,LAST_TIME"
    val schema = StructType(List[StructField](
      StructField("OBJECT_ID_A", StringType, nullable = true),
      StructField("TYPE_A", IntegerType, nullable = true),
      StructField("OBJECT_ID_B", StringType, nullable = true),
      StructField("TYPE_B", IntegerType, nullable = true),
      StructField("CNT", IntegerType, nullable = true),
      StructField("REMARK", StringType, nullable = true),
      StructField("FIRST_TIME", StringType, nullable = true),
      StructField("LAST_TIME", StringType, nullable = true)
    ))
    val category1Level = spark.sparkContext.broadcast(Constant.PRIORITY_LEVEL(category1))
    val category2Level = spark.sparkContext.broadcast(Constant.PRIORITY_LEVEL(category2))

    val dataRow = dataret.map {
      case (personId, hphm, driverType, cnt, firstTime, lastTime) =>
        Row.fromTuple(personId, category1Level.value, hphm, category2Level.value, cnt, driverType, firstTime, lastTime)
    }
    val df = spark.createDataFrame(dataRow, schema)

    val database = DataBaseFactory(spark, new Properties(), databaseType)
    val saveParamBean = new SaveParamBean(tableName, outputType, database.getDataBaseBean)
    saveParamBean.dataBaseBean = database.getDataBaseBean
    saveParamBean.params = params

    saveParamBean.partitions = Array[(String, String)](
      ("STAT_DATE", statDate),
      ("REL_TYPE", s"${param.relationType}"), //1 要素关联 2 实体关系
      ("TYPE", param.releTypeStr)
    )
    database.save(saveParamBean, df)
  }

  def insertRelationData(
                          spark: SparkSession,
                          category1: String,
                          category2: String,
                          dataret: RDD[(String, String, String, Int, String, String)],
                          carArchiveRdd: RDD[(String, String)],
                          tableName: String,
                          param: Param,
                          statDate: String,
                          outputType: Int = Constant.OVERRIDE_DYNAMIC)
  : Unit = {

    val databaseType = param.keyMap.getOrElse("databaseType","hive").toString
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

    val category1Level = spark.sparkContext.broadcast(Constant.PRIORITY_LEVEL(category1))
    val category2Level = spark.sparkContext.broadcast(Constant.PRIORITY_LEVEL(category2))

    val dataRow = dataret.map {
      case (personId, hphm, driverType, cnt, firstTime, lastTime) =>
        (hphm, (personId, driverType, cnt, firstTime, lastTime))
    }
      .leftOuterJoin(carArchiveRdd) //根据车牌关联获取车辆档案ID
      .map {
        case (hphm, ((personId, driverType, cnt, firstTime, lastTime), carAchiveId)) =>
          // 档案查不出车档,返回 -1
          val hphmValue = if (carAchiveId.nonEmpty) carAchiveId.get else "-1"
          val jsonObject = new JSONObject()
          jsonObject.put("DRIVER_TYPE", driverType) //驾驶类型 1 主驾 2 副驾
          Row.fromTuple(personId, category1Level.value.toString, "", hphmValue, category2Level.value.toString, "", 100,
            firstTime.toLong, "", lastTime.toLong, "", cnt, jsonObject.toJSONString)
      }

    val df = spark.createDataFrame(dataRow, schema)

    //    df.show()
    val database = DataBaseFactory(spark, new Properties(), databaseType)
    val saveParamBean = new SaveParamBean(tableName, outputType, database.getDataBaseBean)
    saveParamBean.dataBaseBean = database.getDataBaseBean
    saveParamBean.params = params

    saveParamBean.partitions = Array[(String, String)](
      ("STAT_DATE", statDate),
      ("REL_TYPE", s"${param.relationType}"), //1 要素关联 2 实体关系
      ("REL_ID", s"${relationMap.getOrElse(param.releTypeStr.toLowerCase(), "")}")
    )
    database.save(saveParamBean, df)
  }
}
