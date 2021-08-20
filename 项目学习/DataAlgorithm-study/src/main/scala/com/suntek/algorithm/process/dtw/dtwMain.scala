package com.suntek.algorithm.process.dtw

import com.suntek.algorithm.algorithm.dtw.SparseDTW
import com.suntek.algorithm.common.conf.{Constant, Param}
import com.suntek.algorithm.common.util.SparkUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

object dtwMain {
  var timeSeconds = 600
  val weight = 0.4

  def main(args: Array[String]): Unit = {
    val param = new Param("")
    val spark = SparkUtil.getSparkSesstion("local", "dtwJob", param, false)
    val dtwObjectRdd = spark.sparkContext.textFile("output/DtwObjectRdd")
    val objectRdd = str2dtwObjectRdd(dtwObjectRdd)

    val DtwData = spark.sparkContext.textFile("output/DtwData")
    val data = str2Data(DtwData)

    val ret = process(param, spark, objectRdd, data)
//
    val ans = ret.collect()
    println(1)
  }

  def process(param:Param,
              sparkSession:SparkSession,
              objectRdd:RDD[(String, (List[(String, Long)], Map[String, List[Long]]))],
              data:RDD[((String, String), (Long, Long, String))] ):
  RDD[Row] = {
    var category1 = "car"
    var category2 = "car"

    val objectID_AB = data.map(v=>(v._1._1, v._1._2)).distinct() //(id1, id2)

    // val objectID_A_RDD = getObjectID_A(data, isDiffCategoryFlag) //(id1, List(设备id, id1时间戳), Map[设备id, List[id1时间戳]])

    //val objectID_B_RDD =  if(!isDiffCategoryFlag) objectID_A_RDD else getObjectID_B(data)  //(id1, List(设备id, id1时间戳), Map[设备id, List[id1时间戳]])

    val rdd =  objectRdd
      .join(objectID_AB)
      .map(v=> (v._2._2,(v._2._1, v._1))) // (id2, (List(设备id, id1时间戳), id1))
      .join(objectRdd)//(id2, (  (List(设备id, id1时间戳), id1),      (List(设备id, id2时间戳))    )    )
      .map(v=>{
        val objectA = v._2._1._2
        val objectB = v._1
        val objectAList = v._2._1._1._1
        val objectAMap = v._2._1._1._2
        val objectBList = v._2._2._1
        val objectBMap = v._2._2._2
        // (travel.toList, nums) travel A满足条件的时间和B满足条件最小时间差的时间
        val (trackSubListA, numsA) = makeDtwArray(objectAList, objectBMap) // numsA 为 A对B没有匹配到的点
        val (trackSubListB, numsB) = makeDtwArray(objectBList, objectAMap)  // numsB 为 B对A没有匹配到的点
        (objectA, objectB, trackSubListA, trackSubListB, numsA * 1.0 / objectAList.size * 1.0,
          numsB * 1.0 / objectBList.size * 1.0, numsA, numsB)
      })

    val dddd = rdd.collect()

    val weightBC = sparkSession.sparkContext.broadcast(weight)

    //travel( device1(travelA(timeStamp1), travelB( minTimeStamp._1))  device2(travelA(timeStamp1), travelB( minTimeStamp._1))   )
    //(沪SVW4QM-0,粤Q17PA3-0,List(  (List(1621304472),List(1621304472))  )   ,List(  (List(1621304472),List(1621304472))   ),0.0,0.6666666666666666,0,2)
    val retDtw = rdd.map(v=>{
      val dtwListA = v._3.map(r=>{
        //device1(travelA(timeStamp1), travelB( minTimeStamp._1))
        if(r._1.size == 1 && r._2.size == 1){
          SparseDTW.euclideanDistance(r._1.head, r._2.head, timeSeconds)
        }else{
          SparseDTW.spDTW_v2(r._1.toArray, r._2.toArray, timeSeconds)
        }
      })
      //(objectA, objectB, costA, trackSubListB)
      val costA = dtwListA.sum.formatted("%.3f").toDouble
      val dtwListB = v._4.map(r=>{
        if(r._1.size == 1 && r._2.size == 1){
          SparseDTW.euclideanDistance(r._1.head, r._2.head, timeSeconds)
        }else{
          SparseDTW.spDTW_v2(r._1.toArray, r._2.toArray, timeSeconds)
        }
      })
      val costB = dtwListB.sum.formatted("%.3f").toDouble
      val gapAPre = v._5.formatted("%.3f").toDouble // A没有匹配的点占比
      val gapBPre = v._6.formatted("%.3f").toDouble //B没有匹配的点占比
      val scoreA = (weightBC.value * costA + (1.0 - weightBC.value) * (1.0 - gapAPre)  * costA).formatted("%.3f").toDouble
      val scoreB =  (weightBC.value * costB + (1.0 - weightBC.value) * (1.0 - gapBPre)  * costB).formatted("%.3f").toDouble
      // ((id1, id2), (A综合相似度，B综合相似度, A没有匹配的点占比，B没有匹配的点占比，A没有匹配的点个数， B没有匹配的点个数，分数A, 分数B))
      ((v._1, v._2), (scoreA, scoreB, gapAPre, gapBPre, v._7, v._8, costA, costB))
    })
    // .persist(StorageLevel.MEMORY_AND_DISK)

    //val valueRdd = retDtw.map(v=> ("key", ((v._2._1, v._2._2), (v._2._1, v._2._2)))).distinct()
    /*val min_max = ss.sparkContext.broadcast(getMaxMin(valueRdd))
    logger.info(s"最大值${min_max.value._2}，最小值${min_max.value._1}")
    */
    val dddd1 = retDtw.collect()

    //车1被拍到第一个时间和设备id以及最后一个
    val retDetail = data // ((id1，id2),(id1时间戳，id2时间戳，设备id))
      .groupByKey()
      .map(v=>{
        val key = v._1
        val list = v._2.toList.sortBy(_._1)
        (key, (list.head._1, list.head._3, list.last._1, list.last._3, list.size))
      })

    val category1Level = sparkSession.sparkContext.broadcast(Constant.PRIORITY_LEVEL(category1))
    val category2Level = sparkSession.sparkContext.broadcast(Constant.PRIORITY_LEVEL(category2))

    //((id1，id2), (最早时间戳，最早关联设备，最晚时间戳，最晚关联设备，总关联次数))
    val ret = retDetail
      //((id1, id2), (A综合相似度，B综合相似度, A没有匹配的点占比，B没有匹配的点占比，A没有匹配的点个数， B没有匹配的点个数，分数A, 分数B))
      .join(retDtw)
      .map(v=>{
        //((id1，id2),( (最早时间戳，最早关联设备，最晚时间戳，最晚关联设备，总关联次数),  (A综合相似度，B综合相似度, A没有匹配的点占比，B没有匹配的点占比，A没有匹配的点个数， B没有匹配的点个数，分数A, 分数B) )
        // id1, id1类型， id2, id2类型，分数，最早时间戳，最早关联设备，最晚时间戳，最晚关联设备
        val gapA = v._2._2._5 // A没有匹配的点个数
        val gapB = v._2._2._6 // B没有匹配的点个数
        val gapAPre = v._2._2._3 // A没有匹配的点占比
        val gapBPre = v._2._2._4 //B没有匹配的点占比
        val scoreA = v._2._2._7
        val scoreB = v._2._2._8
        // val scoreA = (v._2._2._1 - min_max.value._1._1) / (min_max.value._2._1 - min_max.value._1._1)
        // val scoreB = (v._2._2._2 - min_max.value._1._2) / (min_max.value._2._2 - min_max.value._1._2)
        val score = (scoreA + scoreB) / 2
        // (id1, id1类型， id2, id2类型，总关联次数，分数，最早时间戳，最早关联设备，最晚时间戳，最晚关联设备
        // A没有匹配的点个数, A没有匹配的点个数, B没有匹配的点占比,B没有匹配的点占比,分数A, 分数B)
        Row.fromTuple((v._1._1, category1Level.value.toString, v._1._2, category2Level.value.toString,
          v._2._1._5, score.formatted("%.3f").toDouble, v._2._1._1, v._2._1._2, v._2._1._3, v._2._1._4,
          gapA * 1.0, gapAPre.formatted("%.3f").toDouble,
          gapB * 1.0, gapBPre.formatted("%.3f").toDouble,
          scoreA.formatted("%.3f").toDouble, scoreA.formatted("%.3f").toDouble,
          scoreB.formatted("%.3f").toDouble, scoreB.formatted("%.3f").toDouble))
      })
      .coalesce(param.numPartitions.toString.toInt, false)

    ret
  }

  def makeDtwArray(objectAList: List[(String, Long)],
                   objectBMap:Map[String, List[Long]])
  : (List[(List[Long], List[Long])], Int) = {
    //  (List((设备id,时间戳),(设备id,时间戳)),Map((设备id->List(时间,时间)))
    var nums = 0
    val travel = ArrayBuffer[(List[Long], List[Long])]() //满足条件的轨迹
    val travelA = ArrayBuffer[Long]()
    val travelB = ArrayBuffer[Long]()
    objectAList.foreach(r1 =>{
      val deviceId1 = r1._1
      val timeStamp1 = r1._2
      if(objectBMap.contains(deviceId1)){
        val minTimeStamp = objectBMap(deviceId1).map(r2 => (r2, Math.abs(r2 - r1._2))).minBy(_._2) //最小的时间差
        if(minTimeStamp._2 > timeSeconds) {
          nums += 1
          if(travelA.size > 0 && travelB.size > 0){
            travel.append((travelA.toList, travelB.toList))
          }
          travelA.clear()
          travelB.clear()
        }else{
          travelA.append(timeStamp1)
          travelB.append( minTimeStamp._1)
        }
      }else{
        nums += 1
        if(travelA.size > 0 && travelB.size > 0){
          travel.append((travelA.toList, travelB.toList))
        }
        travelA.clear()
        travelB.clear()
      }
    })
    if(travelA.size > 0 && travelB.size > 0){
      travel.append((travelA.toList, travelB.toList))
    }
    (travel.toList, nums) //
  }

  def str2dtwObjectRdd(rdd: RDD[String]): RDD[(String, (List[(String, Long)], Map[String, List[Long]]))] ={
    rdd.map({ row =>{
      val str = row.replace("[","").replace("]","")
      val r = str.split(",")
      val object_id = r(0)
      val object_timeStamp = r(1).toLong
      val device_id = r(2)
      (object_id, (device_id, object_timeStamp)) //(id,(设备id, 时间戳))
    }
    }).filter(_._1.nonEmpty)
      .groupByKey()
      .map(v => {
        val list = v._2.toList.distinct.sortBy(_._2)
        val map = list.groupBy(_._1).map(v => (v._1, v._2.map(_._2)))
        (v._1, (list, map))
      })
  }

  def str2Data(rdd: RDD[String]): RDD[((String, String), (Long, Long, String))] ={
    rdd.map({ row =>{
      try{
        val str = row.replace("(","").replace(")","")
        val r = str.split(",")
        val id1 = r(0).toString
        val id2 = r(1).toString
        val id1_timeStamp = r(2).toString.toLong
        val id2_timeStamp = r(3).toString.toLong
        val device_id = r(4).toString
        ((id1, id2), (id1_timeStamp, id2_timeStamp, device_id)) //((id1，id2),(id1时间戳，id2时间戳，设备id))
      }catch {
        case _:Exception =>
          (("", ""), (0L, 0L, ""))
      }
    }
    }).filter(_._1._1.nonEmpty)
  }
}
