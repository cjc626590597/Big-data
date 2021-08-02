package com.suntek.algorithm.common.database.snowball

import java.sql.{Connection, Statement}
import java.util.Properties
import com.alibaba.druid.pool.{DruidDataSource, DruidPooledConnection}
import com.alibaba.druid.util.JdbcUtils
import com.suntek.algorithm.common.bean.{DataBaseBean, QueryBean, SaveParamBean}
import com.suntek.algorithm.common.conf.Constant
import com.suntek.algorithm.common.database.{DataBase, Druid}
import com.suntek.algorithm.common.util.{DruidUtil, SnowBallSaveUtil}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcOptionsInWrite}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import java.util
import collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class Snowball(ss: SparkSession ,dataBaseBean: DataBaseBean) extends DataBase{
  val logger = LoggerFactory.getLogger(this.getClass)
  val url = dataBaseBean.url
  val username = dataBaseBean.username
  val password = dataBaseBean.password
  val batchSize = 50000
  def getDataBaseBean: DataBaseBean = dataBaseBean

  def excute(sql: String, queryParam: QueryBean) = {
    val ds: DruidDataSource = Druid.getDruid(Constant.SNOWBALL_DRIVER, dataBaseBean.url, dataBaseBean.username, dataBaseBean.password)
    logger.info(sql)
    JdbcUtils.execute(ds, sql, new util.ArrayList[Object]())
  }

  def getTotal(sql: String,
               queryParam: QueryBean)
  : Int ={
    import com.alibaba.druid.util.JdbcUtils
    val ds: DruidDataSource = Druid.getDruid(Constant.SNOWBALL_DRIVER, url, username, password)
    val sqlTotal = s"select count(1) as nums from ( ${sql} )"
    logger.info(sqlTotal)

    val listRef = queryParam.parameters.map(l => l.asInstanceOf[AnyRef]).toList.asJava

    val retTotal = JdbcUtils.executeQuery(ds, sqlTotal, listRef)
    val total = retTotal.get(0).get("nums")

    Druid.remove(url)
    ds.close()

    Integer.parseInt(total.toString)
  }

  /**
   * 自定义分区方式
   */
  def getPredicates(total: Int,
                    batchSize: Int,
                    orderByParam: String)
  : Array[String] = {
    val pageSize = if(total % batchSize == 0) total/batchSize else total/batchSize + 1
    val array = ArrayBuffer[(Int, Int)]()
    for (i <- 0 until pageSize){
      val start = i * batchSize
      val end = batchSize
      array += start -> end
    }
    val predicates = array.map {
      case (start, end) => s"1=1 order by ${orderByParam} limit $start, $end"
    }
    predicates.toArray
  }

  def query(sql: String,
            queryParam: QueryBean
           ): DataFrame ={
    val prop = new Properties()
    prop.put("driver", dataBaseBean.driver)
    prop.put("url", dataBaseBean.url)
    prop.put("user", dataBaseBean.username)
    prop.put("password", dataBaseBean.password)
    prop.put("numPartitions", dataBaseBean.numPartitions+"")
//    prop.put("driver",Constant.SNOWBALL_DRIVER)

    logger.info(s"driver：${dataBaseBean.driver}")
    logger.info(s"url：${dataBaseBean.url}")
    logger.info(s"user：${dataBaseBean.username}")
    logger.info(s"password：${dataBaseBean.password}")
    logger.info(s"numPartitions：${dataBaseBean.numPartitions}")

    val total = getTotal(sql, queryParam)
    logger.info(s"数据总量：${total}")

    if(queryParam.isPage){
      queryParam.checkOrderByParam()
      // snowball 集群分页如果不按主键排序，可能会重复读取数据
      val predicates = getPredicates(total, queryParam.batchSize, queryParam.orderByParam)
      ss.read
        .jdbc(url, s"(${sql}) AS T", predicates, prop)
    }else{
      ss.read
        .jdbc(url, s"(${sql}) AS T", prop)
    }
  }

  def queryDruid(sql: String,
                 list: Array[Any],
                 dataBaseBean: DataBaseBean)
  : Array[mutable.Map[String, Any]] = {
    logger.info(s"Query sql:$sql")
    var ret = Array[mutable.Map[String, Any]]()
    try{
      val db = DruidUtil.getDruidDataSource(dataBaseBean)
      val listRef = list.map(l => l.asInstanceOf[AnyRef])
      val queryRet = if(list.length > 0) JdbcUtils.executeQuery(db, sql, listRef.toList.asJava)
      else JdbcUtils.executeQuery(db, sql)
      ret = queryRet.asScala.map(r=> {
        val map = mutable.Map[String, Any]()
        r.asScala.foreach(k=> map.put(k._1, k._2))
        map
      }).toArray

      Druid.remove(url)
      db.close()
    } catch {
      case ex: Exception => logger.error(ex.toString)
        throw ex
    }
    ret
  }


  /**
  * 插入数据
    */
  def saveForSpark(saveParamBean: SaveParamBean,
                   ret: DataFrame
                  ): Boolean = {
    val prop = new Properties()
    prop.setProperty("driver", Constant.SNOWBALL_DRIVER)
    prop.setProperty("url", dataBaseBean.url)
    prop.setProperty("user", dataBaseBean.username)
    prop.setProperty("password",  dataBaseBean.password)
    val db = DruidUtil.getDruidDataSource(saveParamBean.dataBaseBean)


    var conn: DruidPooledConnection = null
    val deleteStatement: Statement = null
    val tableName = saveParamBean.tableName
    val url = dataBaseBean.url
    try{
      conn = db.getConnection
      val saveMode = saveParamBean.outputType match {
        case Constant.OVERRIDE =>
          val sql = s"truncate table $tableName"
          val deleteStatement = conn.createStatement()
          deleteStatement.execute(sql)
          SaveMode.Append
        case Constant.APPEND =>
          val deleteSql = saveParamBean.preSql
          if(!deleteSql.isEmpty){
            val deleteStatement = conn.prepareStatement(deleteSql)
            deleteStatement.execute()
          }
          SaveMode.Append
        case _ => SaveMode.ErrorIfExists
      }
      conn.commit()
      //saveForJdbc(saveParamBean, ret)
      ret.write.mode(saveMode).jdbc(url, tableName, prop)
      logger.info(s"############# result save success to jdbcurl = $url tableName = $tableName " +
        s"tagName = ${saveParamBean.tagName} tagId = ${saveParamBean.tagId}  taskId = ${saveParamBean.taskId}")
      true
    }catch {
      case ex: Exception=>
        logger.error("############# result save faild to jdbcurl = " + url +" tableName = "+ tableName +
          s" taskId = ${saveParamBean.taskId} error : "+ex.getMessage)
        throw ex
    }finally {
      if(deleteStatement != null) deleteStatement.close()
      if(conn != null) conn.close()

      Druid.remove(url)
      db.close()
    }
  }

  def save(saveParamBean: SaveParamBean,
           data: DataFrame)
  : Boolean = {

    import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
    val tableName = saveParamBean.tableName

    val optionMap = Map(
      JDBCOptions.JDBC_URL -> url,
      JDBCOptions.JDBC_TABLE_NAME -> tableName,
      JDBCOptions.JDBC_DRIVER_CLASS ->  Constant.SNOWBALL_DRIVER,
      "user" -> dataBaseBean.username,
      "password" -> dataBaseBean.password,
      JDBCOptions.JDBC_NUM_PARTITIONS -> "10",
      JDBCOptions.JDBC_BATCH_INSERT_SIZE -> "5000"
    )
    val jdbcOptions = new JdbcOptionsInWrite(optionMap)
    val getConnection: () => Connection = JdbcUtils.createConnectionFactory(jdbcOptions)

    val deleteSql = saveParamBean.preSql

    var conn: Connection = null
    var deleteStatement: Statement = null
    try{
      conn = getConnection()
      // 删除分区
      if(!deleteSql.isEmpty){
        deleteStatement = conn.createStatement()
        logger.info("exec delete sql = " + deleteSql)
        deleteStatement.execute(deleteSql)
      }

    } catch {
      case ex: Exception => logger.error("delete  Error: "+ex)
        logger.error(s"############# delete faild to jdbcurl = ${saveParamBean.dataBaseBean.url} " +
          s"tableName = ${saveParamBean.tableName}  taskId = ${saveParamBean.taskId} error : ${ex.getMessage} deleteSql =${deleteSql} ")
        throw ex
    }finally {
      if(deleteStatement != null) deleteStatement.close()
      if(conn != null) conn.close()
    }

    SnowBallSaveUtil.saveTable(data, getConnection, jdbcOptions)

    true
  }

  def delete(sql: String,
             list: Array[Any],
             saveParamBean: SaveParamBean = null
            ): Boolean = {
    true
  }
}
