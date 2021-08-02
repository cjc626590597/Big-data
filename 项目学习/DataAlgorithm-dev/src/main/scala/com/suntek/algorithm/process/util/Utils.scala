package com.suntek.algorithm.process.util

import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}
import java.util.regex.Pattern

object Utils {

  val hphmRegex: String = "^[京津沪渝冀豫云辽黑湘皖鲁新苏浙赣鄂桂甘晋蒙陕吉闽贵粤青藏川宁琼使领A-Z]{1}" +
    "[A-Z]{1}" +
    "[京津沪渝冀豫云辽黑湘皖鲁新苏浙赣鄂桂甘晋蒙陕吉闽贵粤青藏川宁琼]{0,1}" +
    "[A-Z0-9]{4,5}" +
    "[A-Z0-9挂学警港澳]{1}$"

  //为了兼容旧时军牌标准
  val oldHphmRegex = "^[\\u4e00-\\u9fa5]{1}[a-zA-Z]{1}[a-zA-Z_0-9]{4}[a-zA-Z_0-9_\\u4e00-\\u9fa5]$|^[a-zA-Z]{2}\\d{7}$"


  lazy val hphmPattern: Pattern = Pattern.compile(hphmRegex)
  lazy val oldHphmPattern: Pattern = Pattern.compile(oldHphmRegex)

  /**
   * 判断车牌号码是否有效
   *
   * @param hphm 号牌号码
   * @return true 为有效车牌
   */
  def isValidHphm(hphm: String): Boolean = {
    var ret = hphmPattern.matcher(hphm).matches()
    if (!ret) {
      ret = oldHphmPattern.matcher(hphm).matches()
    }
    ret
  }

  /**
   * 之前的正则不能全面覆盖11位的手机号码，现在这里放宽了其11位的手机号码的规则，13，14，15，17，18, 16,19开头的后面的数字全部放开，
   * 并且加上国外手机号的规则用以备用，13位手机号只判断第一位是否为1，00开头的不管位数第三位不为0后面的为0到1
   *
   * @param number 手机号码
   * @return
   */
  def isMobileNumber(number: String): Boolean = {
    if (number.length == 11) {
      val mobileRegex = "^1[3|4|5|7|8|6|9][0-9]{9}$"
      number.matches(mobileRegex)
    } else if (number.length == 13) {
      val mobileRegex = "^1[0-9]{12}$"
      number.matches(mobileRegex)
    } else {
      val mobileRegex = "^00[1-9]([0-9]+)$"
      number.matches(mobileRegex)
    }
  }

  // TODO 简单校验,后续补充校验
  def isValidImsi(imsi: Object): Boolean = {
    try {
      imsi != null && imsi.toString.length() >= 15 && imsi.toString.toLong > 0
    } catch {
      case _: Exception =>
        false
    }
  }

  // TODO 简单校验,后续补充校验
  def isValidImei(imei: Object): Boolean = {
    try {
      imei != null && imei.toString.length() >= 14 && imei.toString.toLong > 0
    } catch {
      case _: Exception =>
        imei != null && imei.toString.length() >= 14
    }
  }

  // TODO 简单校验,后续补充校验
  def isValidMac(mac: Object): Boolean = {
    mac != null && mac.toString.length() >= 12
  }

  def getTimeList(batchId: String, days: String): (List[String], List[String]) = {

    val date = batchId.substring(0, 8)
    val sdf = new SimpleDateFormat("yyyyMMdd")
    sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    val runTimeStamp = sdf.parse(date).getTime - 60 * 60 * 1000L
    val endDate = sdf.format(runTimeStamp)
    val startDate = sdf.format(sdf.parse(endDate).getTime - days.toInt * 24 * 3600 * 1000L)

    val date0 = sdf.parse(startDate)
    val date1 = sdf.parse(endDate)
    val cal = Calendar.getInstance
    cal.setTime(date0)
    var dateList = List[String]()
    while (cal.getTime.compareTo(date1) < 0) {
      cal.add(Calendar.DAY_OF_MONTH, 1)
      dateList :+= sdf.format(cal.getTime)
    }
      val hourList = (0 to 23).flatMap(r => dateList.map(v => v + s"${if(r > 9) s"$r" else s"0$r"}" + "0000"))
                              .toList :::
                      List[String](s"${date}000000", s"${date}010000")

      val dateResult = (dateList ::: List[String](s"$date")).sortWith(_ < _).drop(1).map(r => s"${r}000000")
      val hourResult = hourList.sortWith(_ < _).drop(2)
      (dateResult, hourResult)
  }
}