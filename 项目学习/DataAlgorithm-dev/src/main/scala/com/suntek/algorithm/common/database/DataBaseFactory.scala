package com.suntek.algorithm.common.database

import com.suntek.algorithm.common.bean.DataBaseBean
import com.suntek.algorithm.common.conf.Constant
import com.suntek.algorithm.common.database.hive.Hive
import com.suntek.algorithm.common.database.mpp.Mpp
import com.suntek.algorithm.common.database.mysql.MySQL
import com.suntek.algorithm.common.database.pgsql.PgSQL
import com.suntek.algorithm.common.database.snowball.Snowball
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import java.util.Properties

object DataBaseFactory {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def apply(ss: SparkSession,
            properties: Properties,
            databaseType: String): DataBase ={

    var numPartitions = 10
    if(StringUtils.isNotBlank(properties.getProperty("numPartitions")))
      numPartitions = Integer.parseInt(properties.getProperty("numPartitions"))

    val dataBaseBean: DataBaseBean = new DataBaseBean(properties.getProperty("jdbc.url"), properties.getProperty("username"), properties.getProperty("password"), numPartitions )

    databaseType match {
      case Constant.MYSQL =>
        properties.put("driver", Constant.MYSQL_DRIVER)
        dataBaseBean.driver = Constant.MYSQL_DRIVER
        new MySQL(ss, dataBaseBean)

      case Constant.SNOWBALL | Constant.SNOWBALLDB =>
        properties.put("driver", Constant.SNOWBALL_DRIVER)
        dataBaseBean.driver = Constant.SNOWBALL_DRIVER
        new Snowball(ss, dataBaseBean)

      case Constant.PGSQL =>
        properties.put("driver", Constant.PGSQL_DRIVER)
        dataBaseBean.driver = Constant.PGSQL_DRIVER
        new PgSQL(ss, dataBaseBean)

      case Constant.HIVE =>
        new Hive(ss, dataBaseBean)
      case Constant.MPP | Constant.MPPDB | Constant.GAUSSDB =>
        properties.put("driver", Constant.MPP_DRIVER)
        dataBaseBean.driver = Constant.MPP_DRIVER
        new Mpp(ss,dataBaseBean)
      case _ =>
        throw new Exception(s"不支持数据类型 ${databaseType}")
    }
  }
}
