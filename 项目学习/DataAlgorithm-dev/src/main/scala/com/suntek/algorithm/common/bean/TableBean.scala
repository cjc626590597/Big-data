package com.suntek.algorithm.common.bean

@SerialVersionUID(2L)
class TableBean(t_tableName: String = "",
                t_databaseType: String = "hive",
                t_url: String = "",
                t_userName: String = "",
                t_password: String = "",
                t_datasetId: String = "",
                t_databaseName: String = "",
                t_host: String = "",
                t_alias: String = "",
                t_outputType: Int = 11,
                t_partitions: Array[(String, String)] = Array[(String, String)]()) extends Serializable {
  var tableName: String = t_tableName
  var databaseType: String = t_databaseType
  var url: String = t_url
  var userName: String = t_userName
  var password: String = t_password
  var datasetId: String = t_datasetId
  var databaseName: String = t_databaseName
  var host: String = t_host
  var alias: String = t_alias
  var outputType: Int = t_outputType
  var partitions: Array[(String, String)] = t_partitions

  def setUrl(url: String): Unit = {
    this.url = url
  }

  override def toString: String = {
    s"$tableName,$databaseType,$url,$userName,$password,$datasetId,$alias,$databaseName,$outputType"
  }
}