package com.suntek.algorithm.common.database.mysql

import java.io.ByteArrayInputStream
import java.sql.{Connection, PreparedStatement, Statement}
import java.util.Properties

import com.alibaba.druid.pool.{DruidDataSource, DruidPooledConnection}
import com.alibaba.druid.util.JdbcUtils
import com.suntek.algorithm.common.bean.{DataBaseBean, QueryBean, SaveParamBean}
import com.suntek.algorithm.common.conf.Constant
import com.suntek.algorithm.common.database.{DataBase, Druid}
import com.suntek.algorithm.common.util.DruidUtil
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import collection.JavaConverters._

class MySQL(ss: SparkSession,dataBaseBean: DataBaseBean) extends DataBase{
  val logger = LoggerFactory.getLogger(this.getClass)

  val batchSize = 5000

  def getDataBaseBean: DataBaseBean = dataBaseBean

  def getTotal(sql: String,
               queryParam: QueryBean)
  : Int = {
    val ds: DruidDataSource = Druid.getDruid(Constant.MYSQL_DRIVER, dataBaseBean.url, dataBaseBean.username, dataBaseBean.password)
    val sqlTotal = s"select count(*) as nums from ( ${sql} ) a"
    logger.info(sqlTotal)
    val retTotal = JdbcUtils.executeQuery(ds, sqlTotal)
    val total = retTotal.get(0).get("nums")
    Integer.parseInt(total.toString)
  }
  /**
   * 自定义分区方式
   */
  def getPredicates(total: Int,
                    batchSize: Int)
  : Array[String] = {
    val pageSize = if(total % batchSize == 0) total/batchSize else total/batchSize + 1
    val array = ArrayBuffer[(Int, Int)]()
    for (i <- 0 until pageSize){
      val start = i * batchSize
      val end = batchSize
      array += start -> end
    }
    val predicates = array.map {
      case (start, end) => s"1=1 limit $start, $end"
    }
    predicates.toArray
  }

  def query(sql: String,
            queryParam: QueryBean
           )
  : DataFrame = {
    val prop = new Properties()
    logger.info(dataBaseBean.driver)
    logger.info(dataBaseBean.url)
    logger.info(dataBaseBean.username)
    logger.info(dataBaseBean.password)
    prop.setProperty("driver", dataBaseBean.driver)
    prop.setProperty("url", dataBaseBean.url)
    prop.setProperty("user", dataBaseBean.username)
    prop.setProperty("password", dataBaseBean.password)
    prop.setProperty("numPartitions", dataBaseBean.numPartitions+"")

    if(queryParam.isPage){
      val total = getTotal(sql, queryParam)
      logger.info(s"数据总量：${total}")
      val predicates = getPredicates(total, queryParam.batchSize)
      ss.read
        .jdbc(dataBaseBean.url, s"(${sql}) AS T", predicates, prop)
    }else{
      ss.read
        .jdbc(dataBaseBean.url, s"(${sql}) AS T", prop)
    }
  }

  /**
    * makeData
    */
  def makeData(stmt: com.mysql.jdbc.PreparedStatement,
               sb: StringBuilder)
  : Unit = {
    val is = new ByteArrayInputStream(sb.toString.getBytes)
    stmt.setLocalInfileInputStream(is)
    stmt.executeUpdate()
  }

  /**
    * 保存数据
    *
    */
  def save(saveParamBean: SaveParamBean, data: DataFrame): Boolean = {
    val insertSql = saveParamBean.insertSql
    val deleteSql = saveParamBean.preSql
    val list = saveParamBean.preParams
    val linesterminated = saveParamBean.linesterminated
    val db = DruidUtil.getDruidDataSource(saveParamBean.dataBaseBean)
    var conn: DruidPooledConnection = null
    var statement: PreparedStatement = null
    var deleteStatement: PreparedStatement = null

    try {
      conn = db.getConnection
      conn.setAutoCommit(false)
      logger.info("删除: " + deleteSql)

      deleteStatement = conn.prepareStatement(deleteSql)
      setParameters(deleteStatement, list)
      deleteStatement.execute()

      logger.info("新增: " + insertSql)
      statement = conn.prepareStatement(insertSql)
      val stmt = statement.unwrap(classOf[com.mysql.jdbc.PreparedStatement])
      var sb = new StringBuilder()
      var k = 0
      data.asInstanceOf[Array[Any]].foreach(v => {
        sb.append(v.toString)
        sb.append(linesterminated)
        k = k + 1
        if (k % batchSize == 0) {
          makeData(stmt, sb)
          sb = new StringBuilder()
          k = 0
        }
      })
      if (k > 0) {
        makeData(stmt, sb)
      }
      conn.commit()
      logger.info(s"############# result save success to jdbcurl = ${saveParamBean.dataBaseBean.url} " +
        s"tableName = ${saveParamBean.tableName} tagName = ${saveParamBean.tagName} tagId = ${saveParamBean.tagId} " +
        s" taskId = ${saveParamBean.taskId}")
      true
    } catch {
      case ex: Exception => logger.error("SaveForDruid Error: " + ex)
        logger.error(s"############# result save faild to jdbcurl = ${saveParamBean.dataBaseBean.url} " +
          s"tableName = ${saveParamBean.tableName}  taskId = ${saveParamBean.taskId} error : ${ex.getMessage}")
        throw ex
    } finally {
      if (deleteStatement != null) deleteStatement.close()
      if (statement != null) statement.close()
      if (conn != null) conn.close()
    }
  }

  /**
    * mysql 数据删除
    */
  def delete(sql: String,
             list: Array[Any],
             saveParamBean: SaveParamBean = null): Boolean = {
    execute(sql, list)
  }

  /**
    * 執行sql
    */
  def execute(sql: String,
              list: Array[Any]): Boolean = {

    val conn = DruidUtil.getDruidDataSource(dataBaseBean).getConnection
    val stat = conn.prepareStatement(sql)
    logger.info(sql)
    try {
      for (i <- list.indices) {
        logger.info(s"param $i = ${list(i)}")
        stat.setObject(i + 1, list(i))
      }

      stat.executeUpdate()

      true
    } catch {
      case ex: Exception => logger.error(ex.toString)
        throw ex
    } finally {
      stat.close()
      conn.close()
    }
  }

  def setParameters(statement: PreparedStatement, parameters: Array[Any]): Unit = {
    if (parameters.length > 0) {
      val listRef = parameters.map(l => l.asInstanceOf[AnyRef]).toList.asJava
      var i = 0
      val size = listRef.size
      while (i < size) {
        val param = listRef.get(i)
        statement.setObject(i + 1, param)
        i += 1
      }
    }
  }
}
