package com.suntek.algorithm

import java.util
import java.util.UUID

import scala.collection.JavaConverters._
import org.apache.spark.mllib.fpm.{AssociationRules, FPGrowth}
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
/**
  * @author zhy
  * @date 2020-8-25 17:19
  */
object AprioriTest {

  def main(args: Array[String]): Unit = {
    val ss = SparkSession
      .builder
      .master("local[2]")
      .appName(s"suntek-offline${UUID.randomUUID()}")
      .config("spark.sql.shuffle.partitions", 5)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val sc = ss.sparkContext
    val freqItemsets = sc.parallelize(Seq(
      new FreqItemset(Array("MAC1", "MAC2"), 1L),
      new FreqItemset(Array("MAC1", "MAC3", "MAC4", "MAC5"), 1L),
      new FreqItemset(Array("MAC2", "MAC3", "MAC4", "MAC6"), 1L),
      new FreqItemset(Array("MAC1", "MAC2", "MAC3", "MAC4"), 1L),
      new FreqItemset(Array("MAC1", "MAC2", "MAC3", "MAC6", "MAC7"), 1L)
    ))
    val transactions = sc.parallelize(Seq(
      Array("MAC1", "MAC3", "MAC4", "MAC5"),
      Array("MAC2", "MAC3", "MAC4", "MAC6"),
      Array("MAC1", "MAC2", "MAC3", "MAC4"),
      Array("MAC1", "MAC2", "MAC3", "MAC6", "MAC7")
    ))
    val fpg = new FPGrowth().setMinSupport(0.3).setNumPartitions(10)
    val model = fpg.run(transactions)
    val freqItem = model.freqItemsets
    val list_freqItem = freqItem.collect().toList
    println("list_freqItem .size: " + list_freqItem.length)
    list_freqItem.foreach(v=>{
      println("[" + v.items.mkString(":") + "], " + v.freq)
    })

    /*val minConfidence = 0.3
    //置信下限
    val rule = model.generateAssociationRules(minConfidence)
    val list_rule = rule.collect()
    println("list_rule.size: " + list_rule.size)
    for (rule <- list_rule) {
      println(rule.javaAntecedent + " => " + rule.javaConsequent + ", " + rule.confidence)
    }

    val ar = new AssociationRules().setMinConfidence(0.3)
    val results1 = ar.run(freqItem)*/

   // results1.collect().foreach { rule =>
   //   println("[" + rule.antecedent.mkString(",") + "=>" + rule.consequent.mkString(",") + "]," + rule.confidence)
   // }

  }
}
