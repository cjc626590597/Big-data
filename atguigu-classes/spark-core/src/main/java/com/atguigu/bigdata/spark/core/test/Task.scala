package com.atguigu.bigdata.spark.core.test

class Task extends Serializable {
  val list = List(1,2,3,4)

  val logic:  (Int) => Int = _ * 2

  def compute(): List[Int] = {
    list.map(logic)
  }
}
