package com.suntek.algorithm.algorithm.dtw

/**
  * @author zhy
  * @date 2020-11-3 17:58
  */
class SMBean(x1: Int, y1: Int, value1: Double, index1: Int) {
  var x: Int = x1
  var y: Int = y1
  var index: Int = index1
  var value: Double = value1

  def this(x1: Int, y1: Int, index1: Int){
    this(x1, y1, -1D, index1)
  }

  def setIndex(index1: Int): Unit = {
    this.index = index1
  }

  def getIndex(): Int = {
    this.index
  }

  def setValue(value1: Double): Unit = {
    this.value = value1
  }

  def getValue(): Double ={
    this.value
  }

  def setX(x: Int): Unit = {
    this.x = x
  }

  def getX(): Int = {
    this.x
  }

  def setY(y: Int): Unit = {
    this.y = y
  }

  def getY(): Int = {
    this.y
  }

  override def toString: String = {
    s"x = $x, y = $y, value=$value, index=$index"
  }
}
