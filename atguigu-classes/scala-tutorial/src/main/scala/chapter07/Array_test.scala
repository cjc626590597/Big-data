package chapter07

import scala.collection.mutable.ArrayBuffer

object Array_test {
  def main(args: Array[String]): Unit = {
    val arr1: Array[Int] = new Array[Int](10)
    val arr2 = Array(11,22,33,44,55)
    println(arr2.mkString(","))
    println()
    arr2.update(0,0)
    println(arr2.mkString(","))
    println(arr2.mkString(","))
    for(i <- arr2){
      print(i +" ")
    }
    println()
    val newArray2 = 33 +: 22+: arr2 :+66 :+77
    println(newArray2.mkString("--"))

    val arr3 = ArrayBuffer[Any](1,2,3)
    for(i <- arr3){
      print(i + " ")
    }
    println(arr3.length)
    println(arr3.hashCode())
    val newArr3 = arr3.+=(4)
    arr3.append(5)
    for(i <- arr3){
      print(i + " ")
    }
    println(arr3 == newArr3)
    println(arr3)

    val list = List(1,2,3,4)
    println(list)
    val list2 = 11 :: 22 :: 33 :: 44 :: Nil
    println(list2)
  }
}
