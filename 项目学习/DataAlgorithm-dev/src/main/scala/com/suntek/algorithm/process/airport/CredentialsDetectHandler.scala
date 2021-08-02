package com.suntek.algorithm.process.airport

import com.alibaba.druid.pool.{DruidDataSource, DruidPooledConnection}
import com.suntek.algorithm.common.conf.Param
import com.suntek.algorithm.common.util.DruidUtil.{dsMap, logger}
import org.apache.commons.lang3.time.FastDateFormat

import java.sql.{PreparedStatement, ResultSet, Statement}
import java.util.Date
import scala.collection.mutable.ListBuffer


/**
 * @author cyl
 * @date 2021-05-20 16:13
 */
object CredentialsDetectHandler {
  val CREDENTIALS_DETECT_STAT = "airport.credentials.detect.stat"
  val CREDENTIALS_DETECT_INSERT = "airport.credentials.detect.insert"
  val CREDENTIALS_DETECT_UPDATE_TIME = "airport.credentials.detect.update_time"

  def process(param: Param): Unit = {
    //统计结果
    val res = statPersonResult(param)
    //插入数据
    saveRes(res,param)

    logger.info("机场一人多证已统计完成")
  }

  def createDs(url: String, userName: String, password: String, driver: String): DruidDataSource = {
    var ds: DruidDataSource = null
    try {
      logger.info("开启一个<{}> Druid Connection", url)
      ds = new DruidDataSource()
      ds.setDriverClassName(driver)
      ds.setUrl(url)
      ds.setUsername(userName)
      ds.setPassword(password)
      ds.setInitialSize(2)
      ds.setMaxActive(10)
      ds.setMinIdle(3)
      ds.setMaxWait(3000)
      ds.setValidationQuery("SELECT 1")
      ds.setTestWhileIdle(true)
      ds.setTestOnBorrow(false)
      ds.setTestOnReturn(false)
    } catch {
      case error: Exception =>
        logger.error("Error Create <" + url + "> Druid Connection", error)
    }
    dsMap += (url -> ds)
    ds

  }

  def statPersonResult(param: Param): ListBuffer[(String, String, String, Int, String, String)] = {
    val sourceDataBase = param.keyMap.getOrElse("sourceDataBase", "pd_dts").toString
//    val db = createDs("jdbc:snowball://172.25.21.16:8123/pd_dts?socket_timeout=30000000", "default", "", "com.inforefiner.snowball.SnowballDriver")
    val db = createDs(s"jdbc:snowball://${param.mppdbIp}:${param.mppdbPort}/${sourceDataBase}?socket_timeout=30000000", param.mppdbUsername, param.mppdbPassword, "com.inforefiner.snowball.SnowballDriver")
    logger.info("mpp链接创建完成")
    val sql =  param.keyMap(CREDENTIALS_DETECT_STAT).toString

    var conn: DruidPooledConnection = null
    var statement: Statement = null
    var rs: ResultSet = null
    val resList = ListBuffer[(String, String, String, Int, String, String)]()
    try {
      conn = db.getConnection
      statement = conn.createStatement()
      rs = statement.executeQuery(sql)
      while (rs.next()) {
        resList.append((rs.getString(1), rs.getString(2), rs.getString(3), rs.getInt(4), rs.getString(5), rs.getString(6)))
      }
    } catch {
      case ex: Exception =>
        throw ex
    } finally {
      if (rs != null) rs.close()
      if (statement != null) statement.close()
      if (conn != null) conn.close()
    }
    logger.info(s"一人多证统计完成，返回结果：${resList.size}")
    resList
  }

  def saveRes(res: ListBuffer[(String, String, String, Int, String, String)],param: Param): Unit = {
    val landDataBase = param.keyMap.getOrElse("landDataBase", "airport_custom_webapp").toString

    val df = FastDateFormat.getInstance("yyyyMMdd")
    val stat_date = df.format(new Date())
    val db = createDs(s"jdbc:mysql://${param.mysqlIp}/${landDataBase}?characterEncoding=UTF-8&autoReconnect=true&useSSL=false", param.mysqlUserName, param.mysqlPassWord, "com.mysql.jdbc.Driver")
    logger.info("mysql链接创建完成")
    val sql = param.keyMap(CREDENTIALS_DETECT_INSERT).toString

    var conn: DruidPooledConnection = null
    var pstm: PreparedStatement = null
    var pstmU: PreparedStatement = null
    try {
      conn = db.getConnection
      conn.setAutoCommit(false)
      pstm = conn.prepareStatement(sql)
      var rowCount = 1
      res.foreach(r => {
        pstm.setString(1, r._1)
        pstm.setString(2, r._2)
        pstm.setString(3, r._3)
        pstm.setInt(4, r._4)
        pstm.setString(5, r._5)
        pstm.setString(6, stat_date)
        pstm.setString(7, r._6)
        pstm.addBatch()
        rowCount += 1

        if (rowCount % 2000 == 0) {
          pstm.executeBatch()
          logger.info(s"批量提交完成，当前进度：$rowCount")
        }
      })

      pstm.executeBatch()
      conn.commit()

      val updateSql = param.keyMap(CREDENTIALS_DETECT_UPDATE_TIME).toString
      pstmU = conn.prepareStatement(updateSql)
      pstmU.setString(1, stat_date)
      pstmU.setString(2, stat_date)
      pstmU.execute()
      conn.commit()
    }catch {
      case ex: Exception =>
        throw ex
    }
    finally {
      if (pstm != null) pstm.close()
      if (pstmU != null) pstmU.close()
      if (conn != null) conn.close()
    }
    logger.info("结果插入mysql完成!")
  }
}
