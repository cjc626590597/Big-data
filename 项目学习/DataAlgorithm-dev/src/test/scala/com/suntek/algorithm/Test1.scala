package com.suntek.algorithm

import org.apache.spark.sql.{Row, SparkSession}
import java.util.UUID

import com.suntek.algorithm.common.conf.Constant
import com.suntek.algorithm.common.util.SparkUtil
import com.suntek.algorithm.fusion.FusionHandler.{INNERSPLIT, SPLITPATITIONS}
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

/**
  * @author zhy
  * @date 2020-12-3 17:50
  */
object Test1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getSparkSession("local", s"test", null)
    val dm = spark.sql(s"show partitions dm_relation_distribute_detail ").collect()
    println("sdad")
    println(dm.head.getString(0))
    //Row("stat_date=2020052609/ rel_type=2/ rel_id=06-0004-0002-02")
    val schemaArray = dm.head.getString(0).split(SPLITPATITIONS).map(x=> {
      val strs = x.split(INNERSPLIT)
      println(strs.head)
      StructField(strs.head, StringType, nullable = false)
    })

    val data  = dm.map{ row =>
      val partions =  row.getString(0).split(SPLITPATITIONS)
      val r = partions.map(x=>{
        val strs = x.split(INNERSPLIT)
        strs.last
      })
      Row.fromSeq(r)
    }.toList
    println(data)
    val schema = StructType(schemaArray)
    val df =  spark.createDataFrame(spark.sparkContext.parallelize(data),schema)
    df.show()

    val w = df.agg(max("stat_date") as "maxValue")
      .select("maxValue").collect()
    w.foreach(v=> println(v))
  }

  def main2(args: Array[String]): Unit = {


    var s = List[((String, Long), (String, Long))]()
    s+:=(("440114300141004001",53297975L),("粤AV088D",20200901134754L))
    s+:=(("440114302950000001",53297502L),("粤AV088D",20200901095105L))
    s+:=(("440114302950000001",53297565L),("粤AV088D",20200901102255L))
    s+:=(("440114302950000002",53297502L),("粤AV088D",20200901095102L))
    s+:=(("440114481700000054",53298029L),("粤AV088D",20200901141452L))
    s+:=(("440114624301001006",53298040L),("粤AV088D",20200901142016L))
    s+:=(("440114624301003002",53298063L),("粤AV088D",20200901143133L))
    s+:=(("440114624301003002",53298564L),("粤AV088D",20200901184217L))
    s+:=(("440114624301003100",53298581L),("粤AV088D",20200901185031L))
    s+:=(("440114624301003101",53298055L),("粤AV088D",20200901142748L))
    s+:=(("440114624301004014",53298068L),("粤AV088D",20200901143410L))
    s+:=(("440114624301005035",53298001L),("粤AV088D",20200901140045L))
    s+:=(("440114624301011002",53298000L),("粤AV088D",20200901140028L))
    s+:=(("440114624326054001",53298037L),("粤AV088D",20200901141859L))
    s+:=(("440114724401002001",53297975L),("粤AV088D",20200901134753L))
    s+:=(("440114724420149062",53297989L),("粤AV088D",20200901135452L))
    s+:=(("440114724420149062",53298803L),("粤AV088D",20200901204131L))
    s+:=(("440114724440146056",53297958L),("粤AV088D",20200901133917L))
    s+:=(("440114724440146056",53298767L),("粤AV088D",20200901202358L))
    s+:=(("440114725430000002",53297993L),("粤AV088D",20200901135650L))
    s+:=(("440114725436004005",53298812L),("粤AV088D",20200901204617L))
    s+:=(("440114726000148058",53297966L),("粤AV088D",20200901134323L))
    s+:=(("440114726000148058",53298773L),("粤AV088D",20200901202657L))
    s+:=(("440114726001002001",53297973L),("粤AV088D",20200901134649L))
    s+:=(("440114819751003001",53298605L),("粤AV088D",20200901190234L))
    s+:=(("440114824561002002",53297537L),("粤AV088D",20200901100833L))
    s+:=(("440114824740000500",53297951L),("粤AV088D",20200901133553L))
    s+:=(("440114824740000500",53298757L),("粤AV088D",20200901201854L))
    s+:=(("440114930740107019",53298030L),("粤AV088D",20200901141515L))
    s+:=(("440114946171001001",53298592L),("粤AV088D",20200901185626L))
    s+:=(("440114946171001001",53298592L),("粤AV088D",20200901185627L))

    val ss = SparkSession
      .builder
      .master("local[1]")
      .appName(s"${UUID.randomUUID()}")
      //          .config("spark.sql.shuffle.partitions", partitions)
      .config("spark.serializer", Constant.SPARK_SERIALIZER)
      .config("spark.sql.warehouse.dir","file:///")
      //          .config("spark.default.parallelism", 36)
      .config("spark.shuffle.file.buffer","64K")
      .config("spark.reducer.maxSizeInFlight","96M")
      .config("spark.executor.heartbeatInterval","30000ms")
      .config("spark.local.dir","D:\\tmp")
      //            .config("spark.memory.offHeap.enabled",true)
      //            .config("spark.memory.offHeap.size","10g")
      //          .config("spark.driver.maxResultSize", "10g")
      .enableHiveSupport()
      .config("hive.metastore.uris","thrift://172.25.21.2:9083")
      .getOrCreate()

    val w1 = ss.sparkContext.parallelize(s)
      .filter(_._1._1.nonEmpty)
      .groupByKey()
      .filter(_._2.map(_._1).toSet.size <= 60) //同一个设备,同一分片下,去掉人员对象个数大于阙值(60)的(设备,分片)
      .flatMap( row => row._2.toList.map(r => ((row._1._1, r._1), r._2))) // ((设备,人员对象),(抓拍时间) ))
   // w1.collect().foreach(v=> println(v))
    println("*************************************************************************")
    w1.combineByKey[List[Long]](
      createCombiner = (v: Long) => {
        val w = new ArrayBuffer[Long]()
        w.append(v)
        w.toList
      },
      mergeValue = (c: List[Long], v: Long) => {
        c :+ v
      },
      mergeCombiners = (list1: List[Long], list2: List[Long]) => {
        list1 ::: list2
      }
    ).map(r=>{
      val key = r._1
      val ret = ArrayBuffer[ArrayBuffer[Long]]()
      var a = ArrayBuffer[Long]()
      val list: List[Long] = r._2.sortBy(r=> r)
      a.append(list.head)
      for(i <- 0 until list.size - 1){
        val j = i + 1
        val t = list(j) - list(i)
        if(t > 30){
          ret.append(a)
          a = ArrayBuffer[Long]()
        }
        a.append(list(j))
      }
      if(a.nonEmpty) {
        ret.append(a)
      }
      (key, ret.map(r=> r.sum / r.size).toList)
    }) .flatMap(v => v._2.map(r => (v._1._1, (v._1._2.toUpperCase(), r)))) // 设备,人员对象, 抓拍时间
      .distinct()
      .sortBy(_._2._2)
      .foreach(x => println(x._1 + " -- " + x._2._1 + " -> " + x._2._2))

    println("*************************************************************************")

   /* var v = List[Long]()
    v +:= 20200901185627L
    v +:= 20200901185626L
    v +:= 20200901141515L
    v +:= 20200901201854L
    v +:= 20200901133553L
    v +:= 20200901100833L
    v +:= 20200901190234L
    v +:= 20200901134649L
    v +:= 20200901202657L
    v +:= 20200901134323L
    v +:= 20200901204617L
    v +:= 20200901135650L
    v +:= 20200901202358L
    v +:= 20200901133917L
    v +:= 20200901204131L
    v +:= 20200901135452L
    v +:= 20200901134753L
    v +:= 20200901141859L
    v +:= 20200901140028L
    v +:= 20200901140045L
    v +:= 20200901143410L
    v +:= 20200901142748L
    v +:= 20200901185031L
    v +:= 20200901184217L
    v +:= 20200901143133L
    v +:= 20200901142016L
    v +:= 20200901141452L
    v +:= 20200901095102L
    v +:= 20200901102255L
    v +:= 20200901095105L
    v +:= 20200901134754L

    // 压缩数据，统一id和设备下前后timeSeriesThreshold时间的数据做压缩，时间取平均值
    val ret = ArrayBuffer[ArrayBuffer[Long]]()
    var a = ArrayBuffer[Long]()
    val list: List[Long] = v.sortBy(r => r)
    a.append(list.head)
    for (i <- 0 until list.size - 1) {
      val j = i + 1
      val t = list(j) - list(i)
      if (t > 30) {
        ret.append(a)
        a = ArrayBuffer[Long]()
      }
      a.append(list(j))
    }
    if (a.nonEmpty) {
      ret.append(a)
    }
    ret.map(r => r.sum / r.size).toList
      .foreach(println)*/
  }
}
