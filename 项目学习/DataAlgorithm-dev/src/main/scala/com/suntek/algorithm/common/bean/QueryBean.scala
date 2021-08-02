package com.suntek.algorithm.common.bean

import org.apache.commons.lang.StringUtils
import org.slf4j.LoggerFactory

class QueryBean {
  val logger = LoggerFactory.getLogger(this.getClass)
  var batchSize: Int = 50000 // 每页数量
  var orderByParam: String = "" // 排序字段
  var isPage: Boolean = false // 是否分页
  var parameters:List[Any] = List[Object]()

  def checkOrderByParam(): Unit ={
    if(StringUtils.isEmpty(orderByParam)){
      throw new Exception("查询参数中，排序字段不能为空")
    }
  }
}
