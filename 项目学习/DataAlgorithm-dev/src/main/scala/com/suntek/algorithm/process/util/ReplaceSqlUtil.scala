package com.suntek.algorithm.process.util

import java.text.SimpleDateFormat
import java.util.TimeZone

import com.suntek.algorithm.common.conf.Param

object ReplaceSqlUtil {

  /***
   *
   * @param sql  sql
   * @param param param
   * @return 返回经过变量替换的sql
   */
  def replaceSqlParam(sql: String, param: Param): String = {
    //事件发生日期格式
    val eventDateFormatStr = param.keyMap.getOrElse("EVENT_DATE_FORMAT", "yyMMdd").toString
    val eventHourFormatStr = param.keyMap.getOrElse("EVENT_HOUR_FORMAT", "HH").toString
    val statDateFormatStr = param.keyMap.getOrElse("STAT_DATE_FORMAT", "yyyyMMdd").toString
    val statDayFormatStr = param.keyMap.getOrElse("STAT_DAY_FORMAT", "yyMMdd").toString

    val eventDateFormat = new SimpleDateFormat(eventDateFormatStr)
    val eventHourFormat = new SimpleDateFormat(eventHourFormatStr)
    val statDateFormat = new SimpleDateFormat(statDateFormatStr)
    val statDayFormat = new SimpleDateFormat(statDayFormatStr)

    eventDateFormat.setTimeZone(TimeZone.getTimeZone(param.zoneId))
    eventHourFormat.setTimeZone(TimeZone.getTimeZone(param.zoneId))
    statDateFormat.setTimeZone(TimeZone.getTimeZone(param.zoneId))
    statDayFormat.setTimeZone(TimeZone.getTimeZone(param.zoneId))

    val startDateTime = eventDateFormat.format(param.startTimeStamp)
    val endDateTime = eventDateFormat.format(param.endTimeStamp)
    var statDateTime = statDateFormat.format(param.startTimeStamp)
    statDateTime = "20210518"

    val startDayTime = statDayFormat.format(param.startTimeStamp)
    val endDayTime = statDayFormat.format(param.endTimeStamp)

    val startHour = eventHourFormat.format(param.startTimeStamp)
    val endHour = eventHourFormat.format(param.endTimeStamp)

    //替换@startDateTime@ @endDateTime@ @startHour@ @endHour@
    var resultSql = sql
      .replaceAll(param.startDateTimeParam, startDateTime)
      .replaceAll(param.endDateTimeParam, endDateTime)
      .replaceAll(param.startHourParam, startHour)
      .replaceAll(param.endHourParam, endHour)
      .replaceAll(param.nowDateParam, param.now)
      .replaceAll(param.startDateParam, s"$startDateTime")
      .replaceAll(param.endDateParam, s"$endDateTime")
      .replaceAll(param.statDateParam, s"$statDateTime")
      .replaceAll(param.startStatDayParam, s"$startDayTime")
      .replaceAll(param.endStatDayParam, s"$endDayTime")
      .replaceAll("@type_a@", param.category1)
      .replaceAll("@type_b@", param.category2)
      .replaceAll("@rel_type@", s"${param.relationType}")

    //替换其他配置参数
    param.keyMap.foreach(p => resultSql = resultSql.replace(s"@${p._1}@", p._2.toString))
    resultSql
  }
}
