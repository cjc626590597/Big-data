package com.suntek.algorithm.algorithm.nondimensionalize

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
  * Created with IntelliJ IDEA.
  * Author: yuanweilin
  * Date: 2020-9-1 15:47
  * Description:无量钢化
  */

object NondimensionalizeAlgorithm {

  /**
    * 极值法
    * @param indexVaue
    * @param maxValue
    * @param minValue
    * @return
    */
  def extremumMethod(indexVaue:Double, maxValue:Double, minValue:Double ): Double ={
    if(maxValue == minValue){
      1
    }else{
      (indexVaue - minValue)/(maxValue - minValue)
    }
  }


  /**
    * 极值法
    * @param indexRdd
    * @return
    */
  def extremumMethod(indexRdd:RDD[Double]): RDD[Double] = {
    val minMax = indexRdd.map(m => (("key"), (m, m)))
      .reduceByKey { (x, y) =>
        val min = Math.min(x._1, y._1)
        val max = Math.max(x._2, y._2)

        (min, max)
      }
      .collect()
      .map(_._2)

    indexRdd
      .map(m =>
        extremumMethod(m, minMax(0)._2, minMax(0)._1)
      )
  }

}
