package com.atguigu.bigdata.spark.streaming

import com.atguigu.bigdata.spark.util.JDBCUtils

import scala.collection.mutable.ListBuffer

object test {
  def main(args: Array[String]): Unit = {
    val listBuffer = ListBuffer[String]()
    val conn = JDBCUtils.getConnection
    val pstmt = conn.prepareStatement("select userid from black_list")
    val rs = pstmt.executeQuery()
    while (rs.next()) {
      listBuffer.append(rs.getString(1))
    }
    listBuffer.foreach(print)
    rs.close()
    pstmt.close()
    conn.close()
  }
}
