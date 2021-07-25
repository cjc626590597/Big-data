package com.atguigu.bigdata.spark.core.framework.common

import com.atguigu.bigdata.spark.core.framework.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

trait TApplication {
  def start(thread:String = "local[*]", name:String = "WordCount")(op: => Unit):Unit ={
    val sparkConf = new SparkConf().setMaster(thread).setAppName(name)
    val sc = new SparkContext(sparkConf)
    EnvUtil.put(sc)

    try{
      op
    }catch {
      case ex => ex.printStackTrace()
    }

    sc.stop()
    EnvUtil.remove()
  }
}
