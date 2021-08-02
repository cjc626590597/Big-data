package com.suntek.algorithm.algorithm.entropymethod

import com.suntek.algorithm.algorithm.nondimensionalize.NondimensionalizeAlgorithm
import com.suntek.algorithm.common.conf.Param
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

/**
  * Created with IntelliJ IDEA.
  * Author: yuanweilin
  * Date: 2020-10-31 15:19
  * Description:熵值法计算各个指标权重，定性指标根据频率计算，定量指标根据占比计算;是否需要进行无量纲化；
  */

object EntropyAlgorithm {
  val logger = LoggerFactory.getLogger(this.getClass)


  def getMinMax(resultRdd: RDD[(String, Int, String, Int, Double, Long, String, String, String, String)]): (Double, Double) = {
    resultRdd.map(m => (("key"), (m._5, m._5)))
      .reduceByKey { (x, y) =>
        val min = Math.min(x._1, y._1)
        val max = Math.max(x._2, y._2)

        (min, max)
      }
      .collect()
      .map(_._2)
      .head
  }

  /**
    * 结果计算：通过加法合成法计算相似度：问题1：定性变量怎么算？ 问题2：定性/定量变量是否需要用归一化的值进行计算结果？
    *
    * @param dataRdd
    * @param weightMapBroadcast
    * @return (对象A， 对象B ，相似度分数)
    */
  def computeResult(sparkSession: SparkSession,dataRdd: DataFrame, weightMapBroadcast: Broadcast[collection.Map[String, Double]], indexLabelMap:  Map[String, (Int, Boolean)]) = {

    var newDataDF = dataRdd

    val totalCount = dataRdd.count()

    indexLabelMap
      .filter(_._2._1 == 0)
      .foreach{i =>
        //计算每个定性指标的频率，组装成新的DF
        newDataDF = computeFiForQualitativeColumn(sparkSession, i._1, newDataDF, totalCount)
      }

    val resultRdd = newDataDF
      .rdd
      .map { r =>
        val weightMap = weightMapBroadcast.value
        var score: Double = 0.0

        weightMap.keys.foreach { k =>
          var keyName = k
          if (indexLabelMap(k) == 0) {
            //定性变量获取新字段值
            keyName = k + "_FI"
          }
          val indexValue = r.getAs(keyName).toString.toDouble
          score += (indexValue * weightMap(k))
        }

        val object_id_a = r.getAs("object_id_a").toString
        val object_type_a = r.getAs("type_a").toString.toInt
        val object_id_b = r.getAs("object_id_b").toString
        val object_type_b = r.getAs("type_b").toString.toInt
        val times = r.getAs("times").toString.toLong
        val first_time = r.getAs("first_time").toString
        val first_device = r.getAs("first_device").toString
        val last_time = r.getAs("last_time").toString
        val last_device = r.getAs("last_device").toString

        (object_id_a, object_type_a, object_id_b, object_type_b, score, times, first_time, first_device, last_time, last_device)
      }


    //归一化
    var minMaxBroadCast: Broadcast[(Double, Double)] = null
    if( !resultRdd.isEmpty()){
      val minMax:(Double, Double) = getMinMax(resultRdd)
      minMaxBroadCast = sparkSession.sparkContext.broadcast(minMax)
    }

    val result = resultRdd.map{ m =>
      val object_id_a = m._1
      val object_type_a = m._2
      val object_id_b = m._3
      val object_type_b = m._4
      val score = m._5
      val times = m._6
      val first_time = m._7
      val first_device = m._8
      val last_time = m._9
      val last_device = m._10

      val minMax = minMaxBroadCast.value

      val scoreNew = ((score - minMax._1) / (minMax._2 - minMax._1)).formatted("%.4f").toDouble

      Row.fromTuple(object_id_a, object_type_a, object_id_b, object_type_b, scoreNew, times, first_time, first_device, last_time, last_device)
    }

    result
      
  }


  /**
    * 计算定性指标的熵值：统计各个不同取值的频率，然后根据统计熵公式计算熵值
    *
    * @param indexName
    * @param indexValue
    * @param totalCount
    * @return
    */
  def computeEntropyForQualitativeVar(indexName: String, indexValue: DataFrame, totalCount:Long, isNondimensionalize:Boolean = false): (String, Double) = {

    val indexStatRdd = indexValue
      .rdd
      .map( r => (r.get(0).toString, 1))
      .reduceByKey(_ + _)

    val indexFiRdd_0 = indexStatRdd.map{i =>
      val indexCount = i._2
      val fi = (indexCount * 1.0 )/totalCount
//      val lnFi = Math.log(fi)
//      (fi * lnFi)
      fi
    }

    var tmpRdd:RDD[Double]  = indexFiRdd_0
    if(isNondimensionalize && !indexFiRdd_0.isEmpty()){
//      indexFiRdd_0.persist(StorageLevel.MEMORY_AND_DISK)
      //无量钢化
      tmpRdd = NondimensionalizeAlgorithm.extremumMethod(indexFiRdd_0.persist(StorageLevel.MEMORY_AND_DISK))

    }

    val indexFiRdd = tmpRdd
      .map(fi => fi * Math.log(fi))

    val fiList = indexFiRdd.collect().toList
    val k = fiList.size
    var sh = 1.0  //如果K==1说明只有一种情况，则熵值为1即权重为0
    if(k > 1){
      sh = -fiList.sum/(Math.log(k))
    }

    (indexName, sh )

  }

  /**
    * 计算定性指标对应字段的评率：统计各个不同取值的频率
    *
    * @param indexName
    * @param indexValue
    * @param totalCount
    * @return
    */
  def computeFiForQualitativeColumn(sparkSession: SparkSession,indexName: String, data: DataFrame, totalCount:Long) = {

    val structFields = StructType(Seq(StructField(indexName, StringType, true), StructField(s"${indexName}_FI", DoubleType, true)))

    val indexStatRdd = data
      .selectExpr(indexName)
      .rdd
      .map(r => (r.get(0).toString, 1))
      .reduceByKey(_ + _)

    val indexFIRow = indexStatRdd.map { i =>
      val indexCount = i._2
      val fi = (indexCount * 1.0) / totalCount
      Row.fromTuple((i._1, fi))
    }
    val indexDF: DataFrame = sparkSession.createDataFrame(indexFIRow, structFields)

    indexDF.cache() //缓存，达到mapjoin的效果

    val newDF = data
      .join(indexDF, Seq(indexName))
      .drop(indexName)

    newDF
  }


  /**
    * 计算定量指标的熵值：计算各个值的占比，然后根据熵公式计算熵值
    * @param indexName
    * @param indexValue
    * @param totalCount
    * @return
    */
  def computeEntropyForQuantitativeVar(indexName: String, indexValue: DataFrame, totalCount: Long, isNondimensionalize:Boolean = false): (String, Double) = {

    val indexStatRdd = indexValue
      .rdd
      .map(_.get(0).toString.toDouble)

    var tmpRdd:RDD[Double]  = indexStatRdd
    if(isNondimensionalize){
      //无量钢化
      tmpRdd = NondimensionalizeAlgorithm.extremumMethod(indexStatRdd.persist(StorageLevel.MEMORY_AND_DISK))

    }

    val indexSum = tmpRdd
      .sum()

    val tmpRdd2 = tmpRdd
      .map{i =>
        if(i != 0){
          val fi = i/indexSum
          val lnFi = Math.log(fi)
//          logger.info("============================= indexSum = "+ indexSum)
//          logger.info("============================= i = "+ i)
//          logger.info("============================= fi = "+ fi)
//          logger.info("============================= lnFi = "+ lnFi)
//          logger.info("============================= fi * lnFi = "+ fi * lnFi)
          (fi * lnFi)
        }else{
          0
        }

    }

    val lnFiSum = tmpRdd2.sum()

    var h = 1.0   //如果totalCount==1说明只有一种情况，则熵值为1即权重为0
    if(totalCount > 1){
      logger.info("============================= lnFiSum = "+ lnFiSum)
      logger.info("============================= totalCount = "+ totalCount)
      logger.info("============================= Math.log(totalCount) = "+ Math.log(totalCount))
      val h0 = lnFiSum/Math.log(totalCount)
      if(h0 < -1){
        //避免由于精度问题导致h0<-1，如果<-1则置为1
        h = 1.0
      }else{
        h = - h0
      }
    }

    logger.info("#################### h  = "+ h )
    (indexName, h)

  }

  /**
    *计算每个指标的权重
    * @param entropyList
    * @return
    */
  def computeWeightForEachIndex(entropyList: ListBuffer[(String, Double)]): List[( String, Double)] = {

    entropyList.foreach(e => println("**************************** entropyList = "+e))
    val sum = entropyList.map((1 - _._2)).sum

    val weightList = ListBuffer[(String, Double)]()

    for(index_entropy <- entropyList){
      val tmp = (index_entropy._1, ((1- index_entropy._2)/sum))
      weightList += tmp
    }
    weightList.toList
  }

  /**
    * 计算各维度指标的熵值,权重
    *
    * @param df
    * @param indexLabel
    */

  def computeWeight(param:Param, df:DataFrame, indexLabel: Array[(String, Int, Boolean)]): List[( String, Double)] ={

    var index_entropy:(String, Double) = null

    val totalCount = df.count();

    val entropyList = ListBuffer[(String,Double)]()

    //计算每一个指标的熵值
    for(index <- indexLabel){
      //根据配置判断指标是定性还是定量指标
      if(index._2 == 0 ){
        //定性变量计算
        index_entropy = computeEntropyForQualitativeVar(index._1, df.selectExpr(index._1), totalCount, index._3)
      }else{
        //定量变量计算
        index_entropy = computeEntropyForQuantitativeVar(index._1, df.selectExpr(index._1), totalCount, index._3)
      }
      entropyList += index_entropy
    }

    val weightList:List[(String, Double)] = computeWeightForEachIndex( entropyList)

    weightList

  }



}
