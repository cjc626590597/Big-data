package com.suntek.algorithm.process.distribution

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime}
import java.util.Date

import com.suntek.algorithm.common.conf.{Constant, Param}
import com.suntek.algorithm.common.util.SparkUtil
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer


object distributionMain {
  def main(args: Array[String]): Unit = {
    val param = new Param("")
    val spark = SparkUtil.getSparkSesstion("local", "distributeJob", param, false)
    val distributionRet1 = spark.sparkContext.textFile("output/DistributionRet1")
    val retDeviceRoundBC = spark.sparkContext.broadcast(new DeviceGroupS())
    val ret1 = str2ret1(spark, retDeviceRoundBC, param, distributionRet1)

//    val retDetail = genDetectionDis(ss, ret1, ret2, param, isDiffCategory, retDeviceRoundBC) //设备，对象A，对象A类型，对象B，对象B类型, 对象A时间戳, 对象B时间戳, 时间差, 入库时间

    val rdd = ret1.collect()
    println(1)
  }

//  def genDetectionDis(ss: SparkSession,
//                      ret1: RDD[(String, (String, Long, String))],
//                      ret2: RDD[(String, (String, Long, String))],
//                      param: Param,
//                      isDiffCategory: Boolean,
//                      retDeviceRoundBC: Broadcast[DeviceGroupS]
//                     )
//  : RDD[(String, String, String, String, String, Long, Long, Long, String, String, String)] = {
//    // ret1：(关联设备, (id1, 时间戳，设备))
//    // ret2：(关联设备, (id2, 时间戳，设备))
//    val secondsSeriesThreshold = param.keyMap.getOrElse(SECONDSSERIESTHRESHOLD, "60").toString.toLong
//    val secondsSeriesThresholdBC = ss.sparkContext.broadcast(secondsSeriesThreshold)
//    val category1 = param.keyMap(CATEGORY1).toString
//    val category2 = param.keyMap(CATEGORY2).toString
//
//    val category1Level = ss.sparkContext.broadcast(Constant.PRIORITY_LEVEL(category1))
//    val category2Level = ss.sparkContext.broadcast(Constant.PRIORITY_LEVEL(category2))
//    val dateFormat = new SimpleDateFormat(param.keyMap("dateFormat").toString)
//    val nowBC = ss.sparkContext.broadcast(dateFormat.format(new Date()))
//    val ret = if(isDiffCategory){
//      ret1.join(ret2)
//        .map{ v=>
//          try{
//            val idTimeStamp2 = v._2._2._2
//            val idTimeStamp1 = v._2._1._2
//            //  infoId
//            val infoIdA = v._2._1._3
//            val infoIdB = v._2._2._3
//            if(idTimeStamp1 - idTimeStamp2 < secondsSeriesThresholdBC.value
//              && idTimeStamp1 - idTimeStamp2 > secondsSeriesThresholdBC.value * -1){
//              //(设备，id1，id2, 时间戳)
//              (( v._1, v._2._1._1, v._2._2._1,  idTimeStamp2), ((idTimeStamp1, 1), (infoIdA, infoIdB)))
//            }else{
//              (("", "", "", 0L), ((0L, 0), ("", "")))
//            }
//          }catch{
//            case _: Exception=> (("", "", "", 0L), ((0L, 0), ("", "")))
//          }
//        }
//    }else{
//      ret1.join(ret2)
//        .filter(r => r._2._1._1 != r._2._2._1) // 过滤id1 = id2 数据
//        .map(v=> {
//          try{
//            val idTimeStamp2 = v._2._2._2
//            val idTimeStamp1 = v._2._1._2
//            //  (设备Id, infoId)
//            val infoIdA = v._2._1._3
//            val infoIdB = v._2._2._3
//            if(idTimeStamp1 - idTimeStamp2 < secondsSeriesThresholdBC.value
//              && idTimeStamp1 - idTimeStamp2 > secondsSeriesThresholdBC.value * -1){
//              //(设备，id1，id2, 时间戳)
//              val (d1, d2, t1, t2) = neaten(v._2._1._1, v._2._2._1, idTimeStamp1, idTimeStamp2)
//              if(v._2._2._1 < v._2._1._1) {
//                ((v._1, d1, d2,  t2), ((t1, 1), (infoIdB, infoIdA)))
//              } else {
//                ((v._1, d1, d2,  t2), ((t1, 1), (infoIdA, infoIdB)))
//              }
//            }else{
//              (("", "", "", 0L), ((0L, 0), ("", "")))
//            }
//          }catch{
//            case _: Exception=> (("", "", "", 0L), ((0L, 0), ("", "")))
//          }
//        })
//    }
//    ret.filter(v=> v._1._1.nonEmpty)
//      .reduceByKey ((x, y) => ((x._1._1 + y._1._1, x._1._2 + y._1._2), x._2))
//      .map{
//        case ((deviceValue, d1, d2,  t2), ((timeSum, counts), (infoIdA, infoIdB))) =>
//          val timeStamp1 = timeSum / counts
//          //设备，对象A，对象A类型，对象B，对象B类型, 对象A时间戳, 对象B时间戳, 时间差, 入库时间, 对象A设备Id,对象AInfoId,对象B设备Id,对象B InfoId,
//          var deviceId = deviceValue
//          if(retDeviceRoundBC.value.deviceGroups.contains((deviceValue))){
//            deviceId = retDeviceRoundBC.value.deviceGroups(deviceValue).deviceList1.head
//          }
//          (deviceId, d1, category1Level.value.toString, d2, category2Level.value.toString, timeStamp1, t2, math.abs(timeStamp1-t2),
//            nowBC.value, infoIdA, infoIdB)
//      }
//      .persist(StorageLevel.MEMORY_AND_DISK)
//  }

  def str2ret1(ss: SparkSession, retDeviceRoundBC: Broadcast[DeviceGroupS],param:Param, dataRdd: RDD[String], types: Int = 1):
  RDD[(String, (String, Long, String))] ={
    val dateFormat = ss.sparkContext.broadcast("yyyyMMddHHmmss")
    val timeSeriesThreshold = "300".toLong
    val timeSeriesThresholdBC = ss.sparkContext.broadcast(timeSeriesThreshold)

    dataRdd.map{ row =>
      try {
        val str = row.replace("(","").replace(")","")
        val r = str.split(",")
        val time = s"20${r(0).toString}"
        val id = r(1).toString.toUpperCase
        val deviceId = r(2).toString
        val infoId = r(3).toString
        val timeStamp = DateUtils.parseDate(time, dateFormat.value).getTime / 1000L
        var deviceIdR = deviceId
        if(retDeviceRoundBC.value.deviceGroups.nonEmpty){
          if(types == 2){
            deviceIdR = retDeviceRoundBC.value.find2(deviceIdR)
          }else{
            deviceIdR = retDeviceRoundBC.value.find1(deviceIdR)
          }
        }
        ((id, deviceIdR), (timeStamp, infoId))
      }catch {
        case _: Exception =>
          (("", ""), (0L, ""))
      }
    }.filter(v=> v._1._2.nonEmpty)
      .combineByKey(
        createCombiner =  (v: (Long, String)) => List[(Long, String)](v),
        mergeValue = (c: List[(Long, String)], v: (Long, String)) => c:+ v,
        mergeCombiners = (list1: List[(Long, String)], list2: List[(Long, String)]) => list1 ::: list2)
      .map { r=>
        val key = r._1
        val ret = ArrayBuffer[ArrayBuffer[(Long, String)]]()
        var a = ArrayBuffer[(Long, String)]()
        val list: List[(Long, String)] = r._2.sortWith(_._1 < _._1)
        a.append(list.head)
        for(i <- 0 until list.size - 1){
          val j = i + 1
          val t = list(j)._1 - list(i)._1
          if(t > timeSeriesThresholdBC.value){
            ret.append(a)
            a = ArrayBuffer[(Long, String)]()
          }
          a.append(list(j))
        }
        if(a.nonEmpty) {
          ret.append(a)
        }
        (key, ret.map(r=> (r.map(_._1).sum / r.size, r.head._2)).toList)
      }
      .flatMap(v=>{
        val id = v._1._1
        val deviceId =  v._1._2
        v._2.map(r=> {
          (deviceId, (id, r._1, r._2))
        })
      })
      .distinct()
//      .persist()
//      .persist(StorageLevel.MEMORY_AND_DISK)
  }
}
