package com.suntek.algorithm.common.conf

import java.time.ZoneId

/**
  * Created with IntelliJ IDEA.
  * Author: yuanweilin
  * Date: 2020-10-13 14:12
  * Description:伴随算法所需要的变量, 减少内存开销
  */

@SerialVersionUID(2L)
class AccompanyParam(param: Param) extends Serializable {
  var sliceTime = 60
  var mf = 1
  var deviceThreshold = 3
  var separator = "#"
  //每个卡口时间段划分的时间间隔，单位秒
  var interval = 10 * 60

  var event_time_format_str: String = null

  var eventDate: String = null

  var simScoreThreshold = 0.005

  //数据源
  var dataSource = ""

  var now = ""

  val zoneId: ZoneId = ZoneId.of("Asia/Shanghai")

  //结果表中时间格式
//  val resutlTimeFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")


  sliceTime = (Integer.parseInt(param.keyMap.getOrElse("SLICE_TIME", "60").toString))

  //相似度计算结果的放大因子
  mf = (Integer.parseInt(param.keyMap.getOrElse("MF", "1").toString))

  //经过最小卡口/设备数量阈值
  deviceThreshold = (Integer.parseInt(param.keyMap.getOrElse("DEVICE_THRESHOLD", "3").toString))

  interval = (Integer.parseInt(param.keyMap.getOrElse("TIME_INTERVAL", "600").toString))

  //过滤相似度低的数据
  simScoreThreshold = (java.lang.Double.parseDouble(param.keyMap.getOrElse("SIM_SORCE_THRESHOLD", "0").toString))

  //分隔符
  separator = param.keyMap.getOrElse("SEPARATOR", "#").toString

  //事件发生时间格式
  event_time_format_str = param.keyMap.getOrElse("EVENT_TIME_FORMAT", "yyyy-MM-dd HH:mm:ss").toString

  eventDate = param.dateFormat.format(param.startTimeStamp)

  dataSource = param.keyMap.getOrElse("DATA_SOURCE", "").toString

  now = param.now

  val accompanyType = param.keyMap.getOrElse("ACCOMPANY_TYPE", "").toString

  val accompanyStatType = param.keyMap.getOrElse("ACCOMPANY_STAT_TYPE", "").toString

}
