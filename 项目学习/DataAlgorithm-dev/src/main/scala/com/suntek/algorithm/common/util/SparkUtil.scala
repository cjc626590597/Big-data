package com.suntek.algorithm.common.util

import java.util.UUID

import com.suntek.algorithm.common.conf.{Constant, Param}
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

object SparkUtil {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def getSparkSession(mode: String,
                      jobName: String,
                      param: Param,
                      enableHiveSupport: Boolean = true): SparkSession = {
    var ss: SparkSession = null
    mode match {
      case Constant.LOCAL =>
        val spark = SparkSession
          .builder
          .master("local[12]")
          .appName(s"${jobName}_${UUID.randomUUID()}")
//          .config("spark.sql.shuffle.partitions", partitions)
          .config("spark.serializer", Constant.SPARK_SERIALIZER)
          .config("spark.sql.warehouse.dir","file:///")
          //          .config("spark.default.parallelism", 36)
          .config("spark.shuffle.file.buffer","96K")
          .config("spark.reducer.maxSizeInFlight","96M")
          .config("spark.executor.heartbeatInterval","30000ms")
          .config("spark.driver.memory", "4G")
          .config("spark.executor.memory", "8G")
//          .config("spark.local.dir","D:\\tmp")
//            .config("spark.memory.offHeap.enabled",true)
//            .config("spark.memory.offHeap.size","10g")
//          .config("spark.driver.maxResultSize", "10g")

        ss = if (enableHiveSupport) spark.enableHiveSupport().getOrCreate() else spark.getOrCreate()
//          .config("hive.metastore.uris","thrift://172.25.21.2:9083")
      case Constant.YARN =>
        val spark_executor_memory = param.keyMap.getOrElse("spark.executor.memory", "10g").toString
        val spark_port_maxRetries = param.keyMap.getOrElse("spark.port.maxRetries", "500").toString
        val spark_default_parallelism = param.keyMap.getOrElse("spark.default.parallelism", "100").toString
        val spark_executor_cores = param.keyMap.getOrElse("spark.executor.cores", "6").toString
        val inferenceMode = param.keyMap.getOrElse("inference.mode", "NEVER_INFER").toString
        val parallelPartitionsDiscovery = param.keyMap.getOrElse("parallel.partitions.discovery", "3000").toString

        ss = SparkSession
          .builder
          .appName(s"${jobName}_${UUID.randomUUID()}")
          .config("spark.serializer", Constant.SPARK_SERIALIZER)
          .config("spark.shuffle.file.buffer","64K")
          .config("spark.executor.memory", spark_executor_memory)
          .config("spark.port.maxRetries", spark_port_maxRetries)
          .config("spark.default.parallelism", spark_default_parallelism)
          .config("spark.sql.hive.caseSensitiveInferenceMode", inferenceMode)
          .config("spark.sql.sources.parallelPartitionsDiscovery.threshold", parallelPartitionsDiscovery)
          // .config("spark.driver.memory", spark_driver_memory)
          .config("spark.executor.cores", spark_executor_cores)
          .enableHiveSupport()
          .getOrCreate()
        ss.sparkContext.hadoopConfiguration.set("dfs.socket.timeout", "3600000")
      case "test" =>
        val spark = SparkSession
          .builder
          .master("local[*]")
          .appName(s"${jobName}_${UUID.randomUUID()}")
          .config("spark.serializer", Constant.SPARK_SERIALIZER)
          .config("spark.sql.warehouse.dir","file:///")
          .config("spark.shuffle.file.buffer","64K")
          .config("spark.reducer.maxSizeInFlight","96M")
          .config("spark.executor.heartbeatInterval","30000ms")
          .config("hive.metastore.uris","thrift://172.25.21.2:9083")
//          .config("spark.local.dir","E:\\tmp")
          .config("spark.memory.offHeap.enabled",true)
          .config("spark.memory.offHeap.size","8g")
          .config("spark.driver.maxResultSize", "8g")
        ss =  spark.enableHiveSupport().getOrCreate()
      case _ =>
        ss = SparkSession
          .builder
          .master("local[*]")
          .appName(s"${jobName}_${UUID.randomUUID()}")
          .config("spark.serializer", Constant.SPARK_SERIALIZER)
          .getOrCreate()
    }
    ss
  }
}
