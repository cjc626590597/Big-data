package com.suntek.algorithm.common.database.mpp

import com.alibaba.druid.pool.{DruidDataSource, DruidPooledConnection}
import com.alibaba.druid.util.JdbcUtils
import com.suntek.algorithm.common.bean.{DataBaseBean, QueryBean, SaveParamBean}
import com.suntek.algorithm.common.conf.Constant
import com.suntek.algorithm.common.database.{DataBase, Druid}
import com.suntek.algorithm.common.util.DruidUtil
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.postgresql.util.PSQLException
import org.slf4j.{Logger, LoggerFactory}

import java.sql.Statement
import java.util
import java.util.Properties
import scala.collection.mutable.ArrayBuffer

@SerialVersionUID(2L)
class Mpp(ss: SparkSession, dataBaseBean: DataBaseBean) extends DataBase with Serializable {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val batchSize = 5000
  val driver: String = Constant.MPP_DRIVER

  override def getDataBaseBean: DataBaseBean = dataBaseBean


  def getTotal(sql: String,
               queryParam: QueryBean)
  : Int = {
    val ds: DruidDataSource = Druid.getDruid(Constant.MPP_DRIVER, dataBaseBean.url, dataBaseBean.username, dataBaseBean.password)
    val sqlTotal = s"select count(1) as nums from ( ${sql} )"
    logger.info(sqlTotal)
    val retTotal = JdbcUtils.executeQuery(ds, sqlTotal, new util.ArrayList[Object]())
    val total = retTotal.get(0).get("nums")
    Integer.parseInt(total.toString)
  }

  def excute(sql: String, queryParam: QueryBean) = {
    val ds: DruidDataSource = Druid.getDruid(Constant.MPP_DRIVER, dataBaseBean.url, dataBaseBean.username, dataBaseBean.password)
    logger.info(sql)
    JdbcUtils.execute(ds, sql, new util.ArrayList[Object]())
  }


  /**
   * 自定义分区方式
   */
  def getPredicates(total: Int,
                    batchSize: Int)
  : Array[String] = {
    val pageSize = if (total % batchSize == 0) total / batchSize else total / batchSize + 1
    val array = ArrayBuffer[(Int, Int)]()
    for (i <- 0 until pageSize) {
      val start = i * batchSize
      val end = batchSize
      array += start -> end
    }
    val predicates = array.map {
      case (start, end) => s"1=1 offset $start limit $end"
    }
    logger.info(s"${predicates.take(10).toList}")
    logger.info(s"分页数量:${predicates.size}")
    predicates.toArray
  }

  override def query(sql: String, queryParam: QueryBean): DataFrame = {

    val prop = new Properties()
    logger.info("9999999999999999999")
    logger.info(dataBaseBean.driver)
    logger.info(dataBaseBean.url)
    logger.info(dataBaseBean.username)
    logger.info(dataBaseBean.password)
    logger.info(sql)
    prop.setProperty("driver", dataBaseBean.driver)
    prop.setProperty("url", dataBaseBean.url)
    prop.setProperty("user", dataBaseBean.username)
    prop.setProperty("password", dataBaseBean.password)
    prop.setProperty("numPartitions", dataBaseBean.numPartitions + "")


    if (queryParam.isPage) {
      val total = getTotal(sql, queryParam)
      logger.info(s"数据总量：${total}")
      val predicates = getPredicates(total, queryParam.batchSize)
      ss.read
        .jdbc(dataBaseBean.url, s"(${sql}) AS T", predicates, prop)
    } else {
      ss.read
        .jdbc(dataBaseBean.url, s"(${sql}) AS T", prop)
    }
  }

  /**
   * 保存数据
   *
   */
  override def save(saveParamBean: SaveParamBean, data: DataFrame): Boolean = {
    // mpp增加分区
    try {
      if (saveParamBean.isCreateMppdbPartition){
        createPartition(saveParamBean)
      }
    }catch {
      case e:PSQLException => logger.error("分区已存在无需创建", e)
      case e:Exception => logger.error("未知异常", e)
    }
    if (!saveParamBean.checkMppJdbc()) {
      throw new Exception("配置有误")
    }
    saveForJdbc(saveParamBean, data)
  }

  def createPartition(saveParamBean: SaveParamBean): Unit ={

    var conn: DruidPooledConnection = null
    var deleteStatement: Statement = null
    try {
      val sql = s"alter table ${saveParamBean.tableName} add partition ${saveParamBean.mppdbPartition} VALUES LESS THAN(${saveParamBean.mppdbDate})"
      logger.info(s"create partition sql: $sql")
      val db = DruidUtil.getDruidDataSource(saveParamBean.dataBaseBean)
      conn = db.getConnection
      deleteStatement = conn.createStatement()
      deleteStatement.execute(sql)
    } catch {
      case ex: Exception => logger.error("create partition error: " + ex)
    } finally {
      if (deleteStatement != null) deleteStatement.close()
      if (conn != null) conn.close()
    }
  }


  def saveForJdbc(saveParamBean: SaveParamBean, data: DataFrame): Boolean = {
    if (!saveParamBean.checkMppJdbc()) {
      throw new Exception("配置有误")
    }
    val deleteSql = saveParamBean.preSql
    val tableName = saveParamBean.tableName
    try {
      // 删除数据
      if (deleteSql.nonEmpty) {
        delete(deleteSql, saveParamBean = saveParamBean)
      }
      // 插入数据
      var k = 0
      val valueList = for (_ <- saveParamBean.params.split(",")) yield "?"
      val insertSql = s"insert into $tableName( ${saveParamBean.params}) values (${valueList.mkString(",")})"

      data.foreachPartition { iter =>
        val db = DruidUtil.getDruidDataSource(saveParamBean.dataBaseBean, isJudgeExistDs = false)
        val conn = db.getConnection
        conn.setAutoCommit(false)
        val statement = conn.prepareStatement(insertSql)
        try {
          while (iter.hasNext) {
            val row = iter.next()
            for (i <- valueList.indices) {
              statement.setObject(i + 1, row.get(i).asInstanceOf[Object])
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
        } catch {
          case ex: Exception => logger.error("SaveForDruid Error: " + ex)
            logger.error(s"############# result save faild to jdbcurl = ${saveParamBean.dataBaseBean.url} " +
              s"tableName = $tableName  taskId = ${saveParamBean.taskId} error : ${ex.getMessage}")
            throw ex
        } finally {
          if (statement != null) {
            statement.close()
          }
          if (conn != null) {
            conn.close()
          }
        }
      }
      logger.info(s"############# result save success to jdbcurl = ${saveParamBean.dataBaseBean.url} " +
        s"tableName = $tableName tagName = ${saveParamBean.tagName} tagId = ${saveParamBean.tagId} taskId = ${saveParamBean.taskId}")
      true
    } catch {
      case ex: Exception => logger.error("SaveForDruid Error: " + ex)
        logger.error(s"############# result save faild to jdbcurl = ${saveParamBean.dataBaseBean.url} " +
          s"tableName = $tableName  taskId = ${saveParamBean.taskId} error : ${ex.getMessage}")
        throw ex
    }
  }

  override def delete(sql: String, list: Array[Any] = Array[Any](), saveParamBean: SaveParamBean): Boolean = {
      var conn: DruidPooledConnection = null
      var deleteStatement: Statement = null
      try {
        logger.info(s"delete sql: $sql")
        val db = DruidUtil.getDruidDataSource(saveParamBean.dataBaseBean)
        conn = db.getConnection
        deleteStatement = conn.createStatement()
        deleteStatement.execute(sql)
      } catch {
        case ex: Exception => logger.error("delete data error: " + ex)
          throw ex
      } finally {
        if (deleteStatement != null) deleteStatement.close()
        if (conn != null) conn.close()
      }
      true
    }
}
