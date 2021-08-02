package com.suntek.algorithm.evaluation

import java.io.{FileInputStream, InputStreamReader}
import java.util.Properties
import com.suntek.algorithm.common.bean.QueryBean
import com.suntek.algorithm.common.conf.Param
import com.suntek.algorithm.common.database.DataBaseFactory
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.language.postfixOps
import scala.reflect.ClassTag

object DataManager {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  lazy val tableMap = Map(
    "lcss"->"dm_lcss",
    "fptree"->"dm_fptree",
    "dtw"->"dm_dtw",
    "entropy"->"dm_entropy_result",
    "fusion"->"dm_relation"
  )

  /**
   * 获取相似度的sql
   *
   * @param alg
   * @param a_type
   * @param statDate
   * @return
   */
  def getSimilarSql(alg: String, a_type: String, statDate: String, preTable: String): String = {
    var smapleSql: String = null
    val table = if (preTable.length > 0) preTable else tableMap((alg))
    //todo 可能有别的算法加入
    alg match {
      case "lcss" =>
        smapleSql =
          s"""
             |SELECT  OBJECT_ID_A, OBJECT_ID_B, CAST(SIM_SCORE as string) as SIM_SCORE
             |  FROM $table
             | WHERE TYPE = '$a_type' AND STAT_DATE = '$statDate'
       """.stripMargin
      case "fptree" =>
        smapleSql =
          s"""
             |SELECT OBJECT_ID_A, OBJECT_ID_B, cast(SCORE as string) AS SIM_SCORE
             |  FROM  $table
             | WHERE  TYPE = '$a_type' AND  STAT_DATE='$statDate'
       """.stripMargin
      case "dtw" =>
        smapleSql =
          s"""
             |SELECT OBJECT_ID_A, OBJECT_ID_B, cast(SCORE as string) as SIM_SCORE
             |  FROM $table
             | WHERE TYPE = '$a_type' AND  STAT_DATE='$statDate'
       """.stripMargin
      case "entropy" =>
        smapleSql =
          s"""
             |SELECT OBJECT_ID_A, OBJECT_ID_B, cast(SCORE as string) as SIM_SCORE
             |  FROM  $table
             | WHERE  TYPE = '$a_type' AND  STAT_DATE='$statDate' AND RESULT_TYPE= '${a_type.toLowerCase}-per-long-day'
       """.stripMargin
      case "fusion" =>
        val map = DataManager.loadRelation()
        val typecode = map(a_type.toLowerCase)
        smapleSql =
          s"""
             |SELECT OBJECT_ID_A, OBJECT_ID_B, cast(SCORE as string) as SIM_SCORE
             |  FROM  $table
             | WHERE   rel_id = '$typecode' AND  STAT_DATE='$statDate'
       """.stripMargin

      case _ =>
        smapleSql =
          s"""
             |SELECT OBJECT_ID_A, OBJECT_ID_B, cast(SCORE as string) as SIM_SCORE
             |  FROM  $table
             | WHERE  TYPE = '$a_type' AND  STAT_DATE='$statDate'
       """.stripMargin
    }
    logger.info(s"sql: $smapleSql")
    smapleSql
  }

  /**
   * 加载正样本和相似度数据，可以自行加入函数处理
   *
   * @param ss         SparkSession
   * @param param      参数列表
   * @param queryParam QueryBean
   * @param f          数据加工，可以根据 分类指标和排序指标进行特殊处理。
   *                   可以参考 `BinaryClassificationEvaluation.dataTransform`
   *                   或者`RankingEvaluation.dataTransform`
   * @tparam T 函数f 的返回参数
   * @return
   */
  def loadData[T: ClassTag](ss: SparkSession,
                            param: Param,
                            queryParam: QueryBean, f: (RDD[(String, String, Double)], Broadcast[Set[(String, String)]], Param, SparkSession) => T): T
  = {
    val alg = param.keyMap("algorithm").toString // lcss,fptree
    val a_type = param.keyMap("aType").toString //car-car ,car-imsi
    val date = param.keyMap("batchId").toString.substring(0, 8)
    val statDate = param.keyMap("statDate").toString.substring(0, 8)
    val posID = param.keyMap("posId").toString
    val preTable = param.keyMap.getOrElse("preTable", "").toString
    val databaseType = param.keyMap("databaseType").toString
    val properties = param.keyMap("properties").asInstanceOf[Properties]
    val dataBase = DataBaseFactory(ss, properties, databaseType)
    dataBase.query("set hive.exec.dynamic.partition.mode=nonstrict",queryParam)
   val presql = s"insert overwrite table  tb_acc_sample partition(type,bd) " +
     s"select '$posID' as id,  object_id_a as obja, object_id_b as objb, 1 as label ,type ,stat_date as bd " +
     "from (select object_id_a,object_id_b,type ,stat_date,row_number() over(partition by type order by sim_score desc ) as rn " +
     s"from dm_lcss where stat_date=$statDate  and type='$a_type' ) t where rn <=100"
    dataBase.query(presql,queryParam)

    val sql =
      s"""
         |select  id,obja,objb, label
         |from  tb_acc_sample
         |where   type = '$a_type' and  bd='$date' and id='$posID'
       """.stripMargin
    logger.info(sql)


    //获取正样本数据
    val posData = dataBase.query(sql, queryParam)
      .rdd.map {
      case Row(id: String, obja: String, objb: String, lable: String) =>
        (id, obja, objb, lable)
    }
    var posSet = posData.filter(_._4 == "1").map { case (_, obja, objb, _) =>
      (obja, objb)
    }
      .collect().toSet
    logger.info(s"数据库正样本数据量：${posSet.size}")
    val strs = a_type.split("-")
    if(strs(0).equals(strs(1))) {
      posSet = posSet.union(posSet.map(_ swap))
      logger.info(s"测试：类型一样")
    }


    val posSetBc = ss.sparkContext.broadcast(posSet)

    // 获取相似度的sql
    val ssql = getSimilarSql(alg, a_type, statDate, preTable)
    val simData = dataBase.query(ssql, queryParam)
      .rdd.map {
      case Row(objecta: String, objectb: String, simSore: String) =>
        (objecta, objectb, simSore.toDouble)
    }
    f(simData, posSetBc, param, ss)

  }


  /**
   * 加载多张表的数据
   *
   * @param ss         SparkSession
   * @param param      参数列表
   * @param queryParam QueryBean
   * @param f          生成sql
   * @return
   */
  def multiLoadData[T: ClassTag](ss: SparkSession,
                                 param: Param,
                                 queryParam: QueryBean, f: Param => Array[String], g: Row => T): Array[RDD[T]]
  = {
    val databaseType = param.keyMap("databaseType").toString
    val properties = param.keyMap("properties").asInstanceOf[Properties]
    val dataBase = DataBaseFactory(ss, properties, databaseType)
     f(param).map { sql =>
        dataBase.query(sql, queryParam)
          .rdd.map(g)
      }
  }


  def readConf(conf: java.io.File): mutable.Map[String, Object] = {
    val keyMap = mutable.Map[String, Object]()
    val properties = new Properties()
    val istr = new InputStreamReader(new FileInputStream(conf), "UTF-8")
    properties.load(istr)
    istr.close()

    val piterator = properties.entrySet().iterator()
    while (piterator.hasNext) {
      val p = piterator.next()
      val key = p.getKey.toString
      val value = p.getValue.toString
      keyMap += (key -> value)
    }
    keyMap
  }

  /** *
   * 加载relation文件，并存放到keyMap中
   */
  def loadRelation(): mutable.Map[String, Object] = {
    val path: String = System.getenv("ALG_DEPLOY_HOME")
    var relationFilePath = s"$path/conf/relation.conf"
    logger.info(s"[loadRelation]:${relationFilePath}")
    var conf = new java.io.File(relationFilePath)
    if (!conf.exists()) {
      relationFilePath = "/opt/data-algorithm/conf/relation.conf"
      //      relationFilePath = "/root/data-algorithm_liaojp/conf/relation.conf"
      conf = new java.io.File(relationFilePath)
    }
    readConf(conf)

  }


}