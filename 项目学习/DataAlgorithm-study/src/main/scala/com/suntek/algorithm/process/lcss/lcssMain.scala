package com.suntek.algorithm.process.lcss

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime}

import com.suntek.algorithm.common.conf.Param
import com.suntek.algorithm.common.util.SparkUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}


object lcssMain {
  def main(args: Array[String]): Unit = {
    val param = new Param("")
    val spark = SparkUtil.getSparkSesstion("local", "lcssJob", param, false)
    val lcssRelateDetailRdd = spark.sparkContext.textFile("output/LcssRelateDetailRdd")
    val relateDetailRdd = str2relatedRdd(lcssRelateDetailRdd)
    val lcsRdd = combineLCS(param, relateDetailRdd)

    val lcssObjectSeqRdd = spark.sparkContext.textFile("output/LcssObjectSeqRdd")
    val objectSeqRdd = str2ObjectSeqRdd(param, lcssObjectSeqRdd)
    val resultDF = computeSimDay(param, lcsRdd, objectSeqRdd)

    val rdd = resultDF.collect()
    println(1)
  }

  def combineLCS(param: Param, dataRdd: RDD[((String, Int, String, Int), (Int, String, Long, Long, String, String, String))])
  : RDD[((String,Int, String, Int), (Int, String,String,String, Long, Long, String, Long, String))] = {
    val minImpactCount = 1

    //每个小时进行合并
    dataRdd.combineByKey(
      createCombiner = (value: (Int, String, Long, Long, String, String, String)) => List[(Int, String, Long, Long, String, String, String)](value),
      mergeValue = (list: List[(Int, String, Long, Long, String, String, String)], value: (Int, String, Long, Long, String, String, String)) => list:+ value,
      mergeCombiners = (list1: List[(Int, String, Long, Long, String, String, String)], list2: List[(Int, String, Long, Long, String, String, String)]) => list1 ::: list2
    )
      //    ((对象A，对象A类型，对象B，对象B类型)，（常量值1，设备ID(公共子序列)，时间差，对象A发生时间（首次时间），设备ID（首次设备），对象B发生时间（最近时间），设备ID（最近设备)）
      //    ((冀B3P40N-0, 2, 粤Q17PA3-0, 2), List((1, 440118626491017001, 0, 1621308072,               440118626491017001, 1621308072, 440118626491017001),
      //                                   (1, 440118626491017001, 0, 1621308072,               440118626491017001, 1621308072, 440118626491017001)))
      .mapValues{ row =>
        // 相同的key进来，意味着两辆车的所有轨迹
        //    （常量值1，设备ID(公共子序列)，时间差，对象A发生时间（首次时间），设备ID（首次设备），对象B发生时间（最近时间），设备ID（最近设备)
        //     (1, 440118626491017001, 0, 1621308072,               440118626491017001, 1621308072, 440118626491017001)
        val seq_same_length = row.map(_._1).sum //1 + 1 = 2
        val sub_time_sum = row.map(_._3).sum // 0 + 0 = 0

        //根据时间排序，（对象A发生时间（首次时间），设备ID（首次设备），设备ID(公共子序列)，info_a，info_b）
        //                  (1621308072,           440118626491017001 440118626491017001, info_a，info_b) 1
        //                  (1621308072,           440118626491017001 440118626491017001, info_a，info_b) 2
        val timeList = row.map(r => (r._4, r._5,r._2,r._6,r._7)).sortWith(_._1 < _._1)
        val seq_str_same = timeList.map(_._3).mkString(param.seqSeparator) // 公共子序列 (440118626491017001, 440118626491017001)
        val seq_str_same_info_a = timeList.map(_._4).mkString(param.seqSeparator) // (info_a，info_a)
        val seq_str_same_info_b = timeList.map(_._5).mkString(param.seqSeparator) // (info_b，info_b)

        val first = timeList.head //(1621308072,           440118626491017001 440118626491017001, info_a，info_b)  1
        val last = timeList.last //(1621308072,           440118626491017001 440118626491017001, info_a，info_b)  2

        //P相同卡口次数 2        公共子序列   info_a，info_a      info_b，info_b    时间差（单位：s）的绝对值的和 0     对象A发生时间（首次时间） 对象A末次时间 对象B发生时间（首次时间） 对象B末次时间）
        (seq_same_length, seq_str_same,seq_str_same_info_a,seq_str_same_info_b, sub_time_sum, first._1, first._2, last._1, last._2)
      }
      .filter(_._2._1 >= minImpactCount)
  }

  def computeSimDay(param:Param, lcsRdd:RDD[((String,Int, String, Int), (Int, String,String,String, Long, Long, String, Long, String))],
                    objectSeqRdd: RDD[(String, (Int, String))] ): RDD[Row] = {

    lcsRdd.map {
      case ( (object_id_a, object_a_type, object_id_b, object_b_type),
      (seq_same_length, seq_same_str,seq_str_same_info_a,seq_str_same_info_b, timeSum, ft,first_device, lt, last_device)
        ) =>
        val first_time = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.ofInstant(Instant.ofEpochMilli(ft * 1000), param.zoneId))
        val last_time = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.ofInstant(Instant.ofEpochMilli(lt * 1000), param.zoneId))
        (object_id_a, (object_a_type, object_id_b, object_b_type, timeSum, seq_same_length, seq_same_str,seq_str_same_info_a,seq_str_same_info_b, first_time, first_device, last_time, last_device))
    }
      .join(objectSeqRdd)
      .map{
        case ( object_id_a, ((object_a_type, object_id_b, object_b_type, timeSum, seq_same_length, seq_same_str,seq_str_same_info_a,seq_str_same_info_b, first_time,first_device, last_time, last_device),
        (seq_length_a, seq_str_a))) =>
          (object_id_b, (object_id_a, object_a_type, object_b_type, seq_length_a, seq_str_a, timeSum, seq_same_length, seq_same_str,seq_str_same_info_a,seq_str_same_info_b, first_time, first_device, last_time, last_device))
      }
      .join(objectSeqRdd)
      .map {
        case (object_id_b, ((object_id_a, object_a_type, object_b_type, seq_length_a, seq_str_a, timeSum, seq_same_length,seq_same_str,seq_str_same_info_a,seq_str_same_info_b, first_time, first_device, last_time, last_device),
        (seq_length_b, seq_str_b))) =>

          //计算AB
          val timeExpectValue: Double = timeSum / seq_same_length
          val part2: Double = (1 / (1 + (timeExpectValue / (param.secondsSeriesThreshold.toString.toInt * 2)))) * param.mf

          val part1ab: Double = seq_same_length * 1.0 / seq_length_a * 1.0
          val sim_ab = (part1ab * part2).formatted("%.4f").toDouble

          //计算BA
          val part1ba: Double = seq_same_length * 1.0 / seq_length_b * 1.0
          val sim_ba = (part1ba * part2).formatted("%.4f").toDouble

          //调和函数计算
          val sim_harmonic_mean = ((2 * sim_ab * sim_ba) / (sim_ab + sim_ba)).formatted("%.4f").toDouble

          Row.fromTuple(object_id_a, object_a_type, object_id_b, object_b_type, sim_ab, sim_ba, sim_harmonic_mean,
            timeSum, seq_same_length, seq_same_str, seq_length_a, seq_str_a, seq_length_b, seq_str_b, first_time, first_device, last_time, last_device,seq_str_same_info_a,seq_str_same_info_b)
      }
  }

  def str2relatedRdd(dataRdd: RDD[String]): RDD[((String, Int, String, Int), (Int, String, Long, Long, String, String, String))] ={
    dataRdd.map{ row =>
      val str = row.replace("(","").replace(")","")
      val r = str.split(",")
      val object_id_a = r(0).toString
      val object_a_type = r(1).toString.toInt
      val object_id_b = r(2).toString
      val object_b_type = r(3).toString.toInt
      val device_id = r(5).toString
      val event_time_a = r(7).toString.toLong
      //      val event_time_b = r.get(6).toString.toLong
      val sub_time = r(6).toString.toLong
      val info_id_a  = r(9)
      val info_id_b  = r(10)
      ((object_id_a, object_a_type, object_id_b, object_b_type), (1, device_id, sub_time, event_time_a, device_id, info_id_a, info_id_b))
    }
  }

  def str2ObjectSeqRdd(param:Param, dataRdd: RDD[String]): RDD[(String, (Int, String))] ={
    dataRdd.map{ row =>
      val str = row.replace("(","").replace(")","")
      val r = str.split(",")
      val object_id = r(0).toString
      val seq_length = r(1).toString.toInt
      val seq_str = r(2).toString
      (object_id, (seq_length, seq_str))
    }.reduceByKey((x, y )=> {
      (x._1 + y._1, s"${x._2}${param.seqSeparator}${y._2}")
    })
      .persist(StorageLevel.MEMORY_AND_DISK)
  }
}
