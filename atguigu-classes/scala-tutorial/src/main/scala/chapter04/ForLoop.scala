package chapter04

import scala.collection.immutable
import scala.util.control.Breaks._


object ForLoop {
  def main(args: Array[String]): Unit = {
    for( i <- 1 to 10 if i!= 2 ){
      print(i + " ")
    }
    println()
//
//    for( i <- 1 to 10){
//      for( j <- 1 to i){
//        print(s"$i * $i = ${i * j} ")
//      }
//      println()
//    }

    val ints: immutable.IndexedSeq[Int] = for (i <- 1 to 10) yield i * i
    println(ints)

    for( i <- 1 to 10 reverse ){
      print(i + " ")
    }
    println()

    breakable(
      for(i <- 1 to 5){
        if (i == 3)
          break()
        println(i)
      }
    )
  }
}
