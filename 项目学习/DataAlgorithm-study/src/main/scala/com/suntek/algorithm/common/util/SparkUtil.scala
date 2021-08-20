package com.suntek.algorithm.common.util

import java.util.UUID

import com.suntek.algorithm.common.conf.{Constant, Param}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object SparkUtil {
//  val logger = LoggerFactory.getLogger(this.getClass)

  def getSparkSesstion(master:String,
                       jobName:String,
                       param:Param,
                       enableHiveSupport: Boolean = false): SparkSession ={
    var ss: SparkSession = null
    master match {
      case Constant.LOCAL =>
        val spark = SparkSession
          .builder
          .master("local[*]")
          .appName(jobName)
          .config("spark.serializer", Constant.SPARK_SERIALIZER)
          .config("spark.sql.warehouse.dir","file:///")
//          .config("spark.sql.shuffle.partitions", partitions)
//          .config("spark.shuffle.file.buffer","64K")
//          .config("spark.reducer.maxSizeInFlight","96M")
//          .config("spark.executor.heartbeatInterval","30000ms")
//          .config("hive.metastore.uris","thrift://172.25.21.2:9083")
//          .config("spark.local.dir","E:\\tmp")
//          .config("spark.memory.offHeap.enabled",true)
//          .config("spark.memory.offHeap.size","8g")
//          .config("spark.driver.maxResultSize", "8g")
        ss = if(enableHiveSupport) spark.enableHiveSupport().getOrCreate() else spark.getOrCreate()
      case Constant.YARN =>
        ss = SparkSession
          .builder
          .master("yarn")
          .appName("defaultJob")
          .config("spark.serializer", Constant.SPARK_SERIALIZER)
          .getOrCreate()
      case _ =>
        ss = SparkSession
          .builder
          .master("local[*]")
          .appName("defaultJob")
          .config("spark.serializer", Constant.SPARK_SERIALIZER)
          .getOrCreate()
      }
    ss
  }
}
