package com.suntek.algorithm.process.lcss

import com.suntek.algorithm.common.bean.{SaveParamBean, TableBean}
import com.suntek.algorithm.common.conf.Param
import com.suntek.algorithm.common.database.DataBaseFactory
import com.suntek.algorithm.common.util.PropertiesUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created with IntelliJ IDEA.
  * Author: yuanweilin
  * Date: 2020-11-19 20:12
  * Description:合并保存每个对象的每小时子序列
  */
object ObjectHourHandler {

  /**
    *合并对象的子序列
    * @param sparkSession SparkSession
    * @param param param
    * @param isDiffCategory 是否同类型
    * @param objectARdd 对象ARDD
    * @param objectBRdd 对象BRDD
    * @return （对象ID, 序列长度，序列）
    */
  def combineObject(sparkSession: SparkSession, param:Param, isDiffCategory: Boolean,
                    objectARdd:  RDD[(String, (String, Long, String))],
                    objectBRdd:  RDD[(String, (String, Long, String))])
  :Boolean ={

    var objectRdd = sparkSession.sparkContext.emptyRDD[(String, String)]

    if(!isDiffCategory){
      objectRdd = objectARdd
          .map(m => (m._2._1, m._1))
    }else{
      objectRdd = objectARdd
        .map(m => (m._2._1, m._1))
          .union(objectBRdd.map(m => (m._2._1, m._1)))
    }

    val objectResult = objectRdd
//      .map(m => (m._2._1, m._1._1  + "_" + m._1._2))
      .reduceByKey((x, y ) => x.concat(param.seqSeparator).concat(y))
      .map { m =>
        val seq_length = m._2.split(param.seqSeparator).length
        Row.fromTuple(m._1, seq_length, m._2)
      }

    insertObjectSeqData(sparkSession, param, objectResult)

  }

  def insertObjectSeqData(sparkSession: SparkSession, param: Param, rowRdd: RDD[Row]): Boolean = {

    val params = "OBJECT_ID,SEQ_LENGTH, SEQ_STR"
    val schema = StructType(List(
      StructField("OBJECT_ID", StringType, nullable = true),
      StructField("SEQ_LENGTH", IntegerType, nullable = true),
      StructField("SEQ_STR", StringType, nullable = true)
    ))
    val df = sparkSession.createDataFrame(rowRdd, schema)

    val outTable = new TableBean("DM_LCSS_OBJECT_HOUR","hive",
      "","default","","","default")

    val properties = PropertiesUtil.genJDBCProperties(outTable)

    val database = DataBaseFactory(sparkSession, properties, outTable.databaseType)
    val dataBaseBean = database.getDataBaseBean
    dataBaseBean.dataBaseName = outTable.databaseName
    val saveParamBean = new SaveParamBean(outTable.tableName, outTable.outputType, dataBaseBean)
    saveParamBean.dataBaseBean = dataBaseBean
    saveParamBean.params = params
    saveParamBean.partitions = Array[(String, String)](("STAT_DATE", s"${ param.keyMap("statDate").toString}"), ("TYPE", s"${param.releTypeStr}") )
    // saveParamBean.partitions = Array[(String, String)](("STAT_DATE", s"${param.startTime.substring(0, 10)}"), ("TYPE", s"${param.releTypeStr}") )
    database.save(saveParamBean, df)
  }
}