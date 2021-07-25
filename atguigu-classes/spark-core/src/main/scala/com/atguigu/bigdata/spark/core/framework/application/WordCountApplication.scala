package com.atguigu.bigdata.spark.core.framework.application

import com.atguigu.bigdata.spark.core.framework.common.TApplication
import com.atguigu.bigdata.spark.core.framework.controller.WordCountcontroller

object WordCountApplication extends App with TApplication{

  start(){
    val countcontroller = new WordCountcontroller()

    countcontroller.dispatch()
  }
}
