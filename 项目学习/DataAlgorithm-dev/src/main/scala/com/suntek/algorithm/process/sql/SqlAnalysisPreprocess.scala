package com.suntek.algorithm.process.sql

import java.text.SimpleDateFormat

import com.suntek.algorithm.common.bean.{QueryBean, TableBean}
import com.suntek.algorithm.common.conf.Param
import com.suntek.algorithm.common.database.DataBaseFactory
import com.suntek.algorithm.common.util.{PropertiesUtil, SparkUtil}
import com.suntek.algorithm.process.distribution.Distribution.registerUDF
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source

/**
 * Created with IntelliJ IDEA.
 * Author: liaojp
 * Date: 2020-11-20 2:12
 * Description:执行sql文件，并落表。sql文件里的sql和注释必须用分号分开例如：
 * 自动填充 @BATCHID@
 * --注释;
 * SELECT * FROM  TABLE  WHERE STAT_DATE='@BATCHID@';
 *
 *
 */
object SqlAnalysisPreprocess {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * 必须配置 系统参数：SQL_TEMPLATE_DIR，
   * subTask子任务，为文件名，文件后缀必须为。sql
   * @param param
   */
  def process(param: Param): Unit = {

    val dir = System.getProperty("SQL_TEMPLATE_DIR")
    //todo
    logger.info(s"[load Sql File Dir]:$dir")
    val master = param.master
    val batchIdDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val batchId = param.keyMap("batchId").toString
    var startTimeStamp: Long = batchIdDateFormat.parse(batchId).getTime - 24 * 60 * 60 * 1000L
    val sparkSession = SparkUtil.getSparkSession(master, s"SQL ANALYSIS PREPROCESS  $batchId  Job", param)
    registerUDF(sparkSession, "isValidEntity", "com.suntek.algorithm.udf.EntityCheckUDF" )
     def perTask(filePath:String): Unit ={
       val sourse = Source.fromFile(filePath)
       val execSqlsStr = sourse.getLines()
       val execSqls: Array[String] = execSqlsStr.mkString(" ")
         .replace("@BATCHID@",batchIdDateFormat.format(startTimeStamp)).split(";").map(_.trim)
         .filter(x=>x.nonEmpty && !x.startsWith("--"))
       sourse.close()
       logger.info(s"load Sql File completed!")
       if (execSqls.length > 0) {

         val tableDetail = new TableBean("", "hive",
           "", "default", "", "", "default")
         val properties = PropertiesUtil.genJDBCProperties(param)
         sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
         val database = DataBaseFactory(sparkSession, properties, tableDetail.databaseType)
         val queryParamBean = new QueryBean()
         queryParamBean.isPage = false
         for (execSql <- execSqls) {
           database.query(execSql, queryParamBean)
         }
       }
     }
    param.keyMap("subTask").toString.split(",").foreach(name=> {
      val filePath= s"$dir/$name.sql"
      logger.info(s"[load Sql File ]:$filePath")
      perTask(filePath)
    })
    sparkSession.close()
  }




}
