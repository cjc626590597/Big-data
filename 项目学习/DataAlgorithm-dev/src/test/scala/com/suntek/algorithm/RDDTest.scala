package scala.com.suntek.algorithm

/**
  * Created with IntelliJ IDEA.
  * Author: yuanweilin
  * Date: 2020-9-2 15:51
  * Description:
  */
object RDDTest {


  def main(args: Array[String]): Unit = {

//    val test:List[(String, Long, String)] = List(("222",11111L, "aaaaaaaaa"),("ddd",222222L, "bbbbbb"),("00000",333333L, "cccccccc"))
//    val tt = test.sortBy(x => x._1.length)
//
//    println(test.sortBy(x => x._1.length))
//    println(test.sortBy(x => x._1))
//    println(test.contains(("111111",11111L, "dddddd")))
//
//
//    val str:Array[String] = Array("bbbb", "aaaa","cccc")
//    println(""+ str.mkString(" "))

    val iter = Iterator(1,2,3)
    println("xx"+ iter.size)
    println("xx" +iter.toList)

  }
}
