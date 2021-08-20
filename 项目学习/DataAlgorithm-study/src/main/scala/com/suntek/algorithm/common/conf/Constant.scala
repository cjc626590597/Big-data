package com.suntek.algorithm.common.conf

object Constant {
  val LOCAL: String = "local"
  val YARN: String = "yarn"
  val MYSQL: String = "mysql"
  val MYSQL_DRIVER = "com.mysql.jdbc.Driver"
  val HIVE : String = "hive"
  val SPARK_SERIALIZER = "org.apache.spark.serializer.KryoSerializer"
  val SPARK: String = "spark"
  val JDBC: String = "jdbc"
  val DRUID: String = "druid"

  val PRIORITY_LEVEL: Map[String, Int] = Map[String, Int](
    "person"-> 1,"car"-> 2, "phone"-> 3, "imei"-> 4, "imsi"-> 5, "mac"-> 6,
    "face" -> 7, "personbody" -> 8, "idcard" -> 9)
}
