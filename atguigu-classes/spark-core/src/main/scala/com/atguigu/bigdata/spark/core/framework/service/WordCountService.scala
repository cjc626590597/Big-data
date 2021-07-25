package com.atguigu.bigdata.spark.core.framework.service

import com.atguigu.bigdata.spark.core.framework.dao.WordCountDao
import com.atguigu.bigdata.spark.core.framework.common.TService
import org.apache.spark.rdd.RDD

class WordCountService extends TService{

  private val wordCountDao = new WordCountDao()

  override def dataAnalysis(): Array[(String, Int)] ={

    val value1 = wordCountDao.readFile("data/1.txt")
    val value2: RDD[String] = value1.flatMap(_.split(" "))
    val value3: RDD[(String, Iterable[String])] = value2.groupBy( word => word)
    val value4: RDD[(String, Int)] = value3.map {
      case (word, list) => {
        (word, list.size)
      }
    }
    val value5: Array[(String, Int)] = value4.collect()
    value5
  }
}
