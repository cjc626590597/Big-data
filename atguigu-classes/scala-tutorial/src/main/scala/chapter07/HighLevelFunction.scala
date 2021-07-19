package chapter07

import scala.collection.mutable

object HighLevelFunction {
  def main(args: Array[String]): Unit = {
    val list = List(1,2,3,4,5,6,7,8,9)

    val evenList = list.filter(_ % 2 == 0)
    println(evenList)

    println(list.map(_ * 2))
    println(list.map(x => x * x))

    val map1 = Map("a"-> 1, "b"-> 3, "c"-> 6)
    val map2 = mutable.Map("a"-> 6, "b"-> 2, "c"-> 9, "d"-> 3)
    val foldMap = map1.foldLeft(map2)(
      (mergedMap, kv) => {
        var key = kv._1
        var value = kv._2
        mergedMap(key) = mergedMap.getOrElse(key, 0) + value
        mergedMap
      }
    )
    println(foldMap)

    val stringList = List(
      "hello",
      "hello world",
      "hello scala",
      "hello spark from scala",
      "hello flink from scala",
    )

    val words: List[String] = stringList.flatMap(_.split(" "))
    println(words)
    val groupedWords: Map[String, List[String]] = words.groupBy(word => word)
    println(groupedWords)
    val amount: Map[String, Int] = groupedWords.map(tuple => (tuple._1, tuple._2.length))
    println(amount)
    val result: List[(String, Int)] = amount.toList.sortWith(_._2 > _._2).take(3)
    println(result)

    val tupleList = List(("Hello Scala Spark World ", 4), ("Hello Scala Spark", 3), ("Hello Scala", 2), ("Hello", 1))
    val tupleList1: List[String] =  tupleList.map(tuple => {
      (tuple._1.trim + " ") * tuple._2
    })
    val tupleList2: List[String] = tupleList1.flatMap(_.split(" "))
    val tupleList3: Map[String, List[String]] = tupleList2.groupBy(word => word)
    val tupleList4: Map[String, Int] = tupleList3.map(kv => (kv._1, kv._2.length))
    val tupleList5 = tupleList4.toList.sortWith((a, b) => a._2 > b._2).take(3)
    println(tupleList5)

    val newList1: List[(String, Int)] = tupleList.flatMap(kv => {
      val strings = kv._1.split(" ")
      strings.map(word => (word, kv._2))
    })
    println(newList1)
    val newList2: Map[String, List[(String, Int)]] = newList1.groupBy(kv => kv._1)
    println(newList2)
    val newList3: Map[String, List[Int]] = newList2.mapValues(list => list.map(data => data._2))
    println(newList3)
    val newList4 = newList3.mapValues(list => list.sum)
    println(newList4)
    val newList5: List[(String, Int)] = newList4.toList.sortWith((a, b) => a._2 > b._2)
    println(newList5)
  }

}
