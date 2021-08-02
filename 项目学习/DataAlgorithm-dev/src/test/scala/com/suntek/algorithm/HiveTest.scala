package com.suntek.algorithm

import java.util.{Properties, UUID}

import com.suntek.algorithm.common.bean.{SaveParamBean, TableBean}
import com.suntek.algorithm.common.conf.Constant
import com.suntek.algorithm.common.database.DataBaseFactory
import org.apache.spark.sql.SparkSession

/**
  * Created with IntelliJ IDEA.
  * Author: yuanweilin
  * Date: 2020-10-15 9:57
  * Description:
  */
object HiveTest {

  def main(args: Array[String]): Unit = {
    val ss = SparkSession
      .builder
        .master("local[12]")
      .appName("HiveTest")
      .config("spark.serializer", Constant.SPARK_SERIALIZER)
      .config("spark.shuffle.file.buffer","64K")
      .config("spark.reducer.maxSizeInFlight","96M")
      .config("hive.metastore.uris","thrift://172.25.21.2:9083")
      .enableHiveSupport()
      .getOrCreate()


    val df = ss.sql("select * from MI_ACCOMPANY_DAY_CAR where EVENT_DATE='2020-09-01' and STAT_DATE='2020-10-14'")

    df.show()

    val outTable = new TableBean("MI_ACCOMPANY_DAY_CAR","hive",
      "","default","","","default")

    //    val outTable = new TableBean()
    val properties = new Properties()
    properties.put("numPartitions", "10")
    properties.put("jdbc.url", outTable.url)
    properties.put("username", outTable.userName)
    properties.put("password", outTable.password)

    val params = "OBJECT_ID_A,OBJECT_ID_B,SIM_SCORE,TIME_SUM,SEQ_SAME_LENGTH, SIM_SEQ_STR, SEQ_LENGTH_A,SEQ_STR_A,SEQ_LENGTH_B,SEQ_STR_B,FIRST_TIME, FIRST_DEVICE, LAST_TIME,LAST_DEVICE, DATA_SRC"


    val database = DataBaseFactory(ss, properties, outTable.databaseType)
    val dataBaseBean = database.getDataBaseBean
    dataBaseBean.dataBaseName = outTable.databaseName
    val saveParamBean = new SaveParamBean(outTable.tableName, outTable.outputType, dataBaseBean)
    saveParamBean.dataBaseBean = dataBaseBean
    saveParamBean.params = params
    saveParamBean.partitions = Array(("EVENT_DATE",s"test"),("STAT_DATE",s"test"))
    database.save(saveParamBean, df)

  }

}
