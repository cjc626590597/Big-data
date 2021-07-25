package com.atguigu.bigdata.spark.core.framework.common

import com.atguigu.bigdata.spark.core.framework.util.EnvUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

trait TDao {
  def readFile(path: String):RDD[String] = {
    val sc: SparkContext = EnvUtil.get()
    val value1: RDD[String] = sc.textFile(path)
    value1
  }
}
