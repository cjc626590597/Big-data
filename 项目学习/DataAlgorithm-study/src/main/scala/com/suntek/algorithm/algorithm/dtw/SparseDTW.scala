package com.suntek.algorithm.algorithm.dtw

import scala.collection.immutable.TreeMap
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks

/**
  * DTW改进算法：基于稀疏矩阵的DTW算法
  *
  * @author zhy
  * @date 2020-11-3 16:06
  */
object SparseDTW {
  var res = 0.5f

  def quantizeSeq(q: Array[Long]): Array[Double] ={
    val minS = q.min
    val maxS = q.max
    q.map(v=>{
      ((v * 1.0 - minS * 1.0) / (maxS * 1.0 - minS * 1.0)).formatted("%.2f").toDouble
    })
  }

  def quantize(x: Array[Long], y: Array[Long])
  : (Array[Double], Array[Double]) ={
    (quantizeSeq(x), quantizeSeq(y))
  }

  def find(lowerBound: Float, upperBound: Float, a: Array[Double]): Array[Int] = {
    val indexs = ArrayBuffer[Int]()
    var i = 0
    for(i<- a.indices){
      val v = a(i)
      if(v >= lowerBound && v <= upperBound){
        indexs.append(i)
      }
    }
    indexs.toArray
  }

  def initSM(xLength: Int, yLength: Int)
  : TreeMap[Int, SMBean] = {
    var sm: TreeMap[Int, SMBean] = new TreeMap[Int, SMBean]()
    for(i<- 0 until xLength){
      for(j<- 0 until yLength){
        val key =  yLength * j + (i+1)
        val bean = new SMBean(i, j, key)
        sm+= (key-> bean)
      }
    }
    sm
  }

  def findLowerNeighors(v: Int, n: Int, sm: TreeMap[Int, SMBean])
  : Array[Int] = {
    val lowerNeighors = Array[Int](v - 1, v - n, v - (n + 1))
    val ret = new ArrayBuffer[Int]()
    val node = sm(v)
    lowerNeighors.foreach(i =>{
      if(sm.contains(i)){
        val neighors = sm(i)
        if((node.getX() == neighors.getX() && node.getY() - 1 == neighors.getY())
          ||(node.getX() - 1 == neighors.getX() && node.getY() - 1 == neighors.getY())
          || (node.getX() - 1 == neighors.getX() && node.getY() == neighors.getY())
        ){
          ret.append(i)
        }
      }
    })
    ret.toArray
  }

  def findUpperNeighors(v: Int, n: Int, sm: TreeMap[Int, SMBean]): Array[Int] = {
    val upperNeighors = Array[Int](v + 1, v + n, v + n + 1)
    val node = sm(v)
    val ret = new ArrayBuffer[Int]()
    var flag = false
    val loop = new Breaks
    loop.breakable{
      for(i<- upperNeighors){
        if(!sm.contains(i)){
          loop.break()
        }
        val neighors = sm(i)
        if(
          ((node.getX() == neighors.getX() && node.getY() +1 == neighors.getY() )
            || (node.getX() + 1 == neighors.getX() && node.getY() == neighors.getY())
            || (node.getX() + 1 == neighors.getX() && node.getY() + 1 == neighors.getY())
            )
        ){
          if(sm(i).getValue() != -1){
            flag = true
            loop.break()
          }
          ret.append(i)
        }
      }
    }
    if(!flag){
      ret.toArray
    }else{
      Array[Int]()
    }
  }

  def findWarpingPath(sm: TreeMap[Int, SMBean], n: Int)
  : Array[SMBean] = {
    var i = sm.last._1
    val warpingPath = ArrayBuffer[SMBean]()
    warpingPath.append(sm(i))
    val loop = new Breaks
    loop.breakable{
      while(i >= 0){
        var hup = sm(i)
        val lowerNeighors = findLowerNeighors (i, n, sm)
        if(lowerNeighors.isEmpty){
          loop.break()
        }
        var minCost = Double.MaxValue
        var flag = true
        lowerNeighors.foreach(index => {
          if(sm(index).getValue() != -1){
            flag = false
          }
        })
        if(flag){
          hup = sm(lowerNeighors.head)
          warpingPath.append(hup)
        }else{
          lowerNeighors.foreach(index => {
            if(sm(index).getValue() != -1 && minCost >= sm(index).getValue()){
              minCost = sm(index).getValue()
              i = index
            }
          })
          hup = sm(i)
          warpingPath.append(hup)
        }

      }
    }
    warpingPath.toArray
  }

  def euclideanDistance(v1: Double, v2: Double, timeSeconds: Long): Double = {
    val r = Math.pow(timeSeconds, 2) * 1.0
    val dis = Math.pow(Math.abs(v2 - v1), 2)  * 1.0
    (r /(r + dis)).formatted("%.3f").toDouble
    /*var value = Math.abs(v2 - v1)
    if(value >= timesScends) value = timesScends
    Math.pow(value, 2).formatted("%.3f").toDouble
    */
  }

  def unblockingSM(x: Array[Long],
                   y: Array[Long],
                   x1: Array[Double],
                   y1: Array[Double],
                   n: Int,
                   timeSeconds: Long
                  )
  : TreeMap[Int, SMBean] ={
    var sm: TreeMap[Int, SMBean] = initSM(x.length, y.length)
    var lowerBound = 0f
    var upperBound = res
    while (lowerBound >=0 && lowerBound <= (1 - res / 2)){
      val idxx = find(lowerBound, upperBound, x1)
      val idxy = find(lowerBound, upperBound, y1)
      lowerBound = lowerBound + res / 2
      upperBound = lowerBound + res
      idxx.foreach(i=> {
        idxy.foreach(j=> {
          val v1 = x(i)
          val v2 = y(j)
          val eucDist = euclideanDistance(v2, v1, timeSeconds)
          val key =  n * j + (i+1)
          val bean = new SMBean(i, j, key)
          if(sm(key).getValue() == -1) {
            if(eucDist != 0){
              bean.setValue(eucDist)
              sm += (key->bean)
            }
          }
        })
      })
    }
    sm
  }

  def unblockingSM_v2(x: Array[Long],
                      y: Array[Long],
                      n: Int,
                      timeSeconds: Long
                  )
  : TreeMap[Int, SMBean] ={
    var sm: TreeMap[Int, SMBean] = new TreeMap[Int, SMBean]()
    for(i<- 0 until x.length){
      for(j<- 0 until y.length){
        val key =  y.length * j + (i+1) //?????
        val bean = new SMBean(i, j, key)
        val v1 = x(i)
        val v2 = y(j)
        val eucDist = euclideanDistance(v2, v1, timeSeconds)
        bean.setValue(eucDist)
        sm+= (key-> bean)
      }
    }
    sm
  }

  def findBestCost_v2(sm: TreeMap[Int, SMBean],
                      n: Int,
                      x: Array[Long],
                      y: Array[Long]
                     )
  : Double = {
    //device1(travelA(timeStamp1), travelB( minTimeStamp._1))
    sm.keySet.foreach(v=>{
      var minCost = 0D
      val lowerNeighors = findLowerNeighors (v, n, sm)
      lowerNeighors.foreach(index =>{
        if(sm.contains(index) && sm(index).getValue() != -1){
          if(minCost == 0){
            minCost = sm(index).getValue()
          }else if(minCost > sm(index).getValue()){
            minCost = sm(index).getValue()
          }
        }
      })
      minCost = minCost + sm(v).getValue()
      sm(v).setValue(minCost)
    })
    sm.last._2.getValue()
  }

  def findBestCost(sm: TreeMap[Int, SMBean],
                   n: Int,
                   x: Array[Long],
                   y: Array[Long],
                   timeSeconds: Long)
  : Double = {
    sm.keySet.foreach(v=>{
      var minCost = 0D
      if(sm(v).getValue() != -1){
        val lowerNeighors = findLowerNeighors (v, n, sm)
        lowerNeighors.foreach(index =>{
          if(sm.contains(index) && sm(index).getValue() != -1){
            if(minCost == 0){
              minCost = sm(index).getValue()
            }else if(minCost > sm(index).getValue()){
              minCost = sm(index).getValue()
            }
          }
        })
        minCost = minCost + sm(v).getValue()
        sm(v).setValue(minCost)
      }
      val upperNeighors = findUpperNeighors(v, n, sm)
      upperNeighors.foreach(index =>{
        val xIndex = sm(index).getX()
        val yIndex = sm(index).getY()
        val v = euclideanDistance(x(xIndex), y(yIndex), timeSeconds)
        sm(index).setValue(v)
      })

    })
    sm.last._2.getValue()
  }

  def spDTW_v2(x: Array[Long],
               y: Array[Long],
               timeSeconds: Long)
  : Double ={
    //device1( x(1timeStamp1,2timeStamp1), y( 1minTimeStamp._1,2minTimeStamp))
    //val (x1, y1) = quantize(x, y)
    val n = y.length
    val sm = unblockingSM_v2(x, y, n, timeSeconds)
    findBestCost_v2(sm, n , x, y)
  }

  def spDTW(x: Array[Long],
            y: Array[Long],
            timeSeconds: Long)
  : Double ={
    val (x1, y1) = quantize(x, y)
    val n = y.length
    val sm = unblockingSM(x, y, x1, y1, n, timeSeconds)
    findBestCost(sm, n , x, y, timeSeconds)
  }
}
