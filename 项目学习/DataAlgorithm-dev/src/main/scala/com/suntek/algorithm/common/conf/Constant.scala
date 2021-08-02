package com.suntek.algorithm.common.conf

object Constant {
  val LOCAL: String = "local"
  val YARN: String = "yarn"
  val MYSQL: String = "mysql"
  val MYSQL_DRIVER: String = "com.mysql.jdbc.Driver"
  val SNOWBALL: String = "snowball"
  val SNOWBALLDB: String = "snowballdb"
  val SNOWBALL_DRIVER: String = "com.inforefiner.snowball.SnowballDriver"
  val PGSQL : String = "pgsql"
  val PGSQL_DRIVER: String = "org.postgresql.Driver"
  val HIVE : String = "hive"
  val SPARK_SERIALIZER = "org.apache.spark.serializer.KryoSerializer"
  val SPARK: String = "spark"
  val JDBC: String = "jdbc"
  val DRUID: String = "druid"

  val MPP = "mpp"
  val MPPDB = "mppdb"
  val GAUSSDB = "gaussdb"
  val MPP_DRIVER = "org.postgresql.Driver"

  val SNOWBALL_SUB_PROTOCOL: String = "snowball"
  val GAUSSDB_SUB_PROTOCOL: String = "postgresql"


  val FRE_ITEMS_BEAN: String = "FreItemsBean"

  val FRE_A_ITEM_BEAN: String = "FreAItemBean"

  val FRE_ITEMS_DECODE_BEAN: String = "FreItemsBean"

  val FRE_A_ITEM_DECODE_BEAN: String = "FreAItemBean"

  val OVERRIDE: Int = 1 //全量
  val APPEND: Int = 2 //增量
  val OVERRIDE_DYNAMIC: Int = 11 //全量且动态分区方式
  val OVERRIDE_APPEND: Int = 12 //增量且动态分区方式

  val PRIORITY_LEVEL: Map[String, Int] = Map[String, Int](
    "person"-> 1,"car"-> 2, "phone"-> 3, "imei"-> 4, "imsi"-> 5, "mac"-> 6,
            "face" -> 7, "personbody" -> 8, "idcard" -> 9)


  val HIVE_PATH = s"/tmp/dm"

  //参数分隔符：用于将spark-submit参数重新组装避免空格带来的问题
  val SEPARATOR = "#--#"
}
