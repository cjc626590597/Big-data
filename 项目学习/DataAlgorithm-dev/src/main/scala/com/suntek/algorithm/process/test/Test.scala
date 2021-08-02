package com.suntek.algorithm.process.test

import java.util.Properties

import com.suntek.algorithm.common.bean.QueryBean
import com.suntek.algorithm.common.conf.Param
import com.suntek.algorithm.common.database.DataBaseFactory
import com.suntek.algorithm.common.util.SparkUtil
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

/**
  * @author zhy
  * 部署验证
  * @date 2020-12-21 14:22
  */
object Test {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val jsonStr: String = args.mkString("")
    val param = new Param(jsonStr)
    process(param)
  }

  def process(param: Param): Unit = {
    val ss: SparkSession = SparkUtil.getSparkSession(param.master, "Test", param)
    val sql =
      s"""
         |SELECT * FROM TB_DEVICE_ROUND
       """.stripMargin
    val queryParam = new QueryBean()
    val databaseType = param.keyMap("databaseType").toString
    val dataBase = DataBaseFactory(ss, new Properties(), databaseType)
    val rdd = dataBase.query(sql, queryParam)
    val count = rdd.count()
    logger.info(s"TB_DEVICE_ROUND 的总数据量 为 ${count}")
  }

}
