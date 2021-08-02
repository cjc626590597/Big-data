package com.suntek.algorithm.common.bean

/**
  * json参数解析
  *
  * @param r_modelRecord
  * @param t_tableRecord
  * @param t_tableName
  * @param t_datasetId
  * @param t_tableDetail
  */
//noinspection ScalaDocMissingParameterDescription
@SerialVersionUID(2L)
class RecordBean(r_modelRecord: String = "",
                 t_tableRecord: String = "",
                 t_tableName: String = "",
                 t_datasetId: String = "",
                 t_tableDetail: TableBean = new TableBean()) extends Serializable{
  var modelRecord: String = r_modelRecord
  var tableRecord: String = t_tableRecord
  var datasetId: String = t_datasetId
  var tableName: String = t_tableName
  var tableDetail: TableBean = t_tableDetail

  override def toString: String = {
    s"$modelRecord,$tableRecord,$datasetId,$tableName,${tableDetail.toString}"
  }
}