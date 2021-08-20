package com.suntek.algorithm.common.conf

import java.time.ZoneId

@SerialVersionUID(2L)
class Param(jsonStr:String) extends Serializable {

  val seqSeparator = "#"
  val mf = 1.0
  val secondsSeriesThreshold = 60
  val zoneId = ZoneId.of("Asia/Shanghai")
  val numPartitions = "10"

  loadModelSql()
  loadConf()

  def loadModelSql(): Unit ={

  }

  def loadConf(): Unit ={

  }

}
