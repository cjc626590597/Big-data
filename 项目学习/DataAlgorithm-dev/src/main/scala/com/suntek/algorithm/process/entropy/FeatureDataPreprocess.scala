package com.suntek.algorithm.process.entropy

import com.suntek.algorithm.common.bean.{QueryBean, TableBean}
import com.suntek.algorithm.common.conf.Param
import com.suntek.algorithm.common.database.DataBaseFactory
import com.suntek.algorithm.common.util.{PropertiesUtil, SparkUtil}
import com.suntek.algorithm.process.util.ReplaceSqlUtil
import org.apache.spark.sql.hive.HiveContext

/**
  * Created with IntelliJ IDEA.
  * Author: yuanweilin
  * Date: 2020-11-20 2:12
  * Description:通过sql准备各类特征数据，并落表（因为特征数据多种多样，因此目前想到的就是通过sql来适配各种数据要求）
  */
object FeatureDataPreprocess {

  def process(param: Param): Unit = {

    val batchId = param.keyMap.get("batchId").get.toString

    val confKey = param.keyMap("featurePreprocKey").toString

    val execSqlsStr = ReplaceSqlUtil.replaceSqlParam(param.keyMap(confKey).toString, param)

    val execSqls: Array[String] = execSqlsStr.split(";")

    if (execSqls.length > 0) {
      val master = param.master
      val sparkSession = SparkUtil.getSparkSession(master, s"FEATURE DATA PREPROCESS  ${param.releTypeStr} ${batchId}  Job", param)

      val tableDetail = new TableBean("", "hive",
        "", "default", "", "", "default")

      val properties = PropertiesUtil.genJDBCProperties(param)
      val database = DataBaseFactory(sparkSession, properties, tableDetail.databaseType)

      val queryParamBean = new QueryBean()
      queryParamBean.isPage = false
//      val hiveContext = new HiveContext(sparkSession.sparkContext)
//      hiveContext.sql("show tables").show()
//
//      sparkSession.sql("show tables").show()
      for (execSql <- execSqls) {

        database.query(ReplaceSqlUtil.replaceSqlParam(execSql, param), queryParamBean)

      }

      sparkSession.close()
    }


  }

}
