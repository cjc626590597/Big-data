package com.suntek.algorithm.common.database

import com.suntek.algorithm.common.bean.{DataBaseBean, QueryBean, SaveParamBean}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

trait DataBase {

  def getDataBaseBean: DataBaseBean

  def query(sql: String, queryParam: QueryBean): DataFrame

  def save(saveParamBean: SaveParamBean,
           data: DataFrame
          ): Boolean

  def delete(sql: String,
             list: Array[Any] = Array[Any](),
             saveParamBean: SaveParamBean = null
            ): Boolean
}
