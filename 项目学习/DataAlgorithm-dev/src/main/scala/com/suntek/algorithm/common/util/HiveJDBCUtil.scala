package com.suntek.algorithm.common.util
import java.sql.{Connection, DriverManager, Statement}
/**
  * Created with IntelliJ IDEA.
  * Author: yuanweilin
  * Date: 2020-12-17 15:09
  * Description:
  */
object HiveJDBCUtil {

  val HIVE_DRIVER_CLASS_NAME="org.apache.hive.jdbc.HiveDriver"
  var conn:Connection = null

  def getConnnection(url:String): Connection ={
    Class.forName(HIVE_DRIVER_CLASS_NAME)
    conn = DriverManager.getConnection(url, "", "")
    conn
  }

  def close(conn:Connection, stmt:Statement): Unit ={

    if(stmt != null && !stmt.isClosed){
      stmt.close()
    }

    if(conn != null && !conn.isClosed){
      conn.close()
    }

  }

}
