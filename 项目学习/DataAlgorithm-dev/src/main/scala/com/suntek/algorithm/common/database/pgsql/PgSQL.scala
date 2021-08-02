package com.suntek.algorithm.common.database.pgsql

import java.io.ByteArrayInputStream
import java.util.Properties
import java.sql.{PreparedStatement => JavaPreparedStatement}

import com.alibaba.druid.pool.{DruidDataSource, DruidPooledConnection}
import com.alibaba.druid.util.JdbcUtils
import com.suntek.algorithm.common.bean.{DataBaseBean, QueryBean, SaveParamBean}
import com.suntek.algorithm.common.conf.Constant
import com.suntek.algorithm.common.database.{DataBase, Druid}
import com.suntek.algorithm.common.util.DruidUtil
import com.mysql.jdbc.{PreparedStatement => MysqlPreparedStatement}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import collection.JavaConverters._

class PgSQL(ss: SparkSession ,dataBaseBean: DataBaseBean) extends DataBase {

  val logger = LoggerFactory.getLogger(this.getClass)
  val batchSize = 5000

  def getDataBaseBean: DataBaseBean = dataBaseBean

  def getTotal(sql: String,
               queryParam: QueryBean)
  : Int ={
    val ds: DruidDataSource = Druid.getDruid(Constant.PGSQL_DRIVER, dataBaseBean.url, dataBaseBean.username, dataBaseBean.password)
    val sqlTotal = s"select count(1) as nums from ( ${sql} )"
    logger.info(sqlTotal)
    val retTotal = JdbcUtils.executeQuery(ds, sqlTotal, queryParam.parameters.asJava)
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
      case (start, end) => s"1=1 offset $start limit $end"
    }
    logger.info(s"分页数量:${predicates.size}")
    predicates.toArray
  }

  def query(sql: String,
            queryParam: QueryBean
           )
  : DataFrame = {

    val prop = new Properties()
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
  def makeData(stmt: MysqlPreparedStatement,
               sb: StringBuilder): Unit = {
    val is = new ByteArrayInputStream(sb.toString.getBytes)
    stmt.setLocalInfileInputStream(is)
    stmt.executeUpdate()
  }

  /**
    *保存数据
    *
    */
  def save(saveParamBean: SaveParamBean, data: DataFrame): Boolean = {
    if(!saveParamBean.checkSnowBall()){
      throw new Exception("配置有误")
    }
    val deleteSql = saveParamBean.preSql
    val tableName = saveParamBean.tableName
    val db = DruidUtil.getDruidDataSource(saveParamBean.dataBaseBean)

    var conn: DruidPooledConnection = null
    var statement: JavaPreparedStatement = null
    var deleteStatement: JavaPreparedStatement = null
    try{
      conn = db.getConnection
      conn.setAutoCommit(false)
      // 删除数据
      if(!deleteSql.isEmpty){
        deleteStatement = conn.prepareStatement(deleteSql)
        deleteStatement.execute()
      }
      // 插入数据
      var k = 0
      val valueList = for(_ <- saveParamBean.params.split(",")) yield "?"
      val insertSql = s"insert into $tableName( ${saveParamBean.params}) values (${valueList.mkString(",")})"
      statement = conn.prepareStatement(insertSql)
      data
        .rdd
        .map { r=>
          val row = ArrayBuffer[Any]()
          for(i<- valueList.indices) row.append(r.get(i))
          row.toList
        }
        .collect()
        .foreach { r=>
          for(i <- r.indices){
            statement.setObject(i + 1, r(i).asInstanceOf[Object])
          }
          statement.addBatch()
          k = k + 1
          if (k % batchSize == 0) {
            statement.executeBatch()
            k = 0
          }
        }
      statement.executeBatch()
      conn.commit()
      logger.info(s"############# result save success to jdbcurl = ${saveParamBean.dataBaseBean.url} " +
        s"tableName = $tableName tagName = ${saveParamBean.tagName} tagId = ${saveParamBean.tagId} taskId = ${saveParamBean.taskId}")
      true
    } catch {
      case ex: Exception => logger.error("SaveForDruid Error: "+ex)
        logger.error(s"############# result save faild to jdbcurl = ${saveParamBean.dataBaseBean.url} " +
          s"tableName = $tableName  taskId = ${saveParamBean.taskId} error : ${ex.getMessage}")
        throw ex
    }finally {
      if(deleteStatement != null) {
        deleteStatement.close()
      }
      if(statement != null) {
        statement.close()
      }
      if(conn != null) {
        conn.close()
      }
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
      case ex: Exception =>
        logger.error(ex.toString)
        throw ex
    } finally {
      stat.close()
      conn.close()
    }
  }

  def save2(saveParamBean: SaveParamBean,
            dataForDataFrame: RDD[Row]): Boolean={
    logger.info(s"pgsql 不支持")
    false
  }
}
