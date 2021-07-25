package com.atguigu.bigdata.spark.core.framework.controller

import com.atguigu.bigdata.spark.core.framework.common.TController
import com.atguigu.bigdata.spark.core.framework.service.WordCountService

class WordCountcontroller extends TController{

  private val wordCountService = new WordCountService()

  override def dispatch(): Unit = {
    val value5 = wordCountService.dataAnalysis()
    value5.foreach(println)
  }
}
