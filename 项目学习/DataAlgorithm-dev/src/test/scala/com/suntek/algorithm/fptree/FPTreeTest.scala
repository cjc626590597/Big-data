package com.suntek.algorithm.fptree

import com.suntek.algorithm.algorithm.association.fptree.FPTree
import com.suntek.algorithm.algorithm.association.fptreenonecpb.FPTreeNoneCpb
import org.junit.{Assert, Test}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
  * @author zhy
  */
class FPTreeTest extends Assert{

  @Test
  def association(): Unit = {
    val transRecords = new ArrayBuffer[java.util.List[String]]()
    transRecords.append(Array("mac1", "mac2", "mac4", "mac6", "mac8", "mac9").toList.asJava)
    transRecords.append(Array("mac2", "mac3", "mac6", "mac7", "mac9").toList.asJava)
    transRecords.append(Array("mac3", "mac4", "mac6", "mac8", "mac9").toList.asJava)
    transRecords.append(Array("mac1", "mac3", "mac5", "mac6", "mac7", "mac8", "mac9", "mac10").toList.asJava)
    transRecords.append(Array("mac2", "mac5", "mac7", "mac8", "mac9", "mac10", "mac11").toList.asJava)
    transRecords.append(Array("mac3", "mac4", "mac7", "mac8", "mac9", "mac11").toList.asJava)
    transRecords.append(Array("mac2", "mac6", "mac8", "mac9", "mac10", "mac11").toList.asJava)
    transRecords.append(Array("mac4", "mac5", "mac6", "mac7", "mac8", "mac10", "mac11").toList.asJava)
    transRecords.append(Array("mac4", "mac5", "mac8", "mac10", "mac11").toList.asJava)
    transRecords.append(Array("mac1", "mac2", "mac4", "mac7", "mac8", "mac10", "mac11").toList.asJava)
    transRecords.append(Array("mac2", "mac3", "mac4", "mac6", "mac8", "mac9", "mac10").toList.asJava)

    /*transRecords.append(Array("MAC1", "MAC3", "MAC4").toList.asJava)
    transRecords.append(Array("MAC1", "MAC3", "MAC4", "MAC5").toList.asJava)
    transRecords.append(Array("MAC2", "MAC3", "MAC4", "MAC6").toList.asJava)
    transRecords.append(Array("MAC1", "MAC2", "MAC3", "MAC4").toList.asJava)
    transRecords.append(Array("MAC1", "MAC2", "MAC3", "MAC6", "MAC7").toList.asJava)*/
    val fpTree = new FPTree()
    fpTree.setConfident(0)
    fpTree.setMinSuport(3)
    fpTree.setMinSuportPercent(0)
    fpTree.buildFPTree(transRecords.toList.asJava)
    // ([mac11, mac10, mac6, mac9, mac8],5)
    val pattens = fpTree.getFrequentItems
    val w = pattens.entrySet().iterator()
    while (w.hasNext){
      val node = w.next()
      println(node.getValue, node.getKey)
    }
  }
  @Test
  def fpTreeNoneTest(): Unit = {
    val transRecords = new ArrayBuffer[java.util.List[String]]()
    /*transRecords.append(Array("MAC1", "MAC3", "MAC4").toList.asJava)
    transRecords.append(Array("MAC1", "MAC3", "MAC4", "MAC5").toList.asJava)
    transRecords.append(Array("MAC2", "MAC3", "MAC4", "MAC6").toList.asJava)
    transRecords.append(Array("MAC1", "MAC2", "MAC3", "MAC4").toList.asJava)
    transRecords.append(Array("MAC1", "MAC2", "MAC3", "MAC6", "MAC7").toList.asJava)*/
    transRecords.append(Array("mac1", "mac2", "mac4", "mac6", "mac8", "mac9").toList.asJava)
    transRecords.append(Array("mac2", "mac3", "mac6", "mac7", "mac9").toList.asJava)
    transRecords.append(Array("mac3", "mac4", "mac6", "mac8", "mac9").toList.asJava)
    transRecords.append(Array("mac1", "mac3", "mac5", "mac6", "mac7", "mac8", "mac9", "mac10").toList.asJava)
    transRecords.append(Array("mac2", "mac5", "mac7", "mac8", "mac9", "mac10", "mac11").toList.asJava)
    transRecords.append(Array("mac3", "mac4", "mac7", "mac8", "mac9", "mac11").toList.asJava)
    transRecords.append(Array("mac2", "mac6", "mac8", "mac9", "mac10", "mac11").toList.asJava)
    transRecords.append(Array("mac4", "mac5", "mac6", "mac7", "mac8", "mac10", "mac11").toList.asJava)
    transRecords.append(Array("mac4", "mac5", "mac8", "mac10", "mac11").toList.asJava)
    transRecords.append(Array("mac1", "mac2", "mac4", "mac7", "mac8", "mac10", "mac11").toList.asJava)
    transRecords.append(Array("mac2", "mac3", "mac4", "mac6", "mac8", "mac9", "mac10").toList.asJava)
    val fpTreeNoneCpb = new FPTreeNoneCpb()
    fpTreeNoneCpb.setN(transRecords.size)
    fpTreeNoneCpb.setIsFindMaxFrequentSet(true)
    fpTreeNoneCpb.setMinCount(3)
    fpTreeNoneCpb.setMinSupport(0)
    val root = fpTreeNoneCpb.buildTree(transRecords.toList.asJava)
    fpTreeNoneCpb.getHeaders(root)
    val currentTimeStamp: Long = System.currentTimeMillis
    fpTreeNoneCpb.fPMining("", currentTimeStamp)
    println("$$$$$$$$$$$$$1")
    val it1 = fpTreeNoneCpb.getTransformTableMax.keySet().iterator()
    while (it1.hasNext){
      val w = fpTreeNoneCpb.getTransformTableMax.get(it1.next())
      w.getFrequentList.asScala.foreach(v=>{
        val it2 = v.keySet().iterator()
        while (it2.hasNext){
          val key = it2.next()
          val value = v.get(key)
          println(key, value.toString)
        }
        println("$$$$$$$$$$$$$2")
      })

    }
    System.out.println("sadadsadsa")
//    /*transRecords.append(Array("MAC1", "MAC3", "MAC4").toList.asJava)
//    transRecords.append(Array("MAC1", "MAC3", "MAC4", "MAC5").toList.asJava)
//    transRecords.append(Array("MAC2", "MAC3", "MAC4", "MAC6").toList.asJava)
//    transRecords.append(Array("MAC1", "MAC2", "MAC3", "MAC4").toList.asJava)
//    transRecords.append(Array("MAC1", "MAC2", "MAC3", "MAC6", "MAC7").toList.asJava)*/
//    transRecords.append(Array("mac1", "mac2", "mac4", "mac6", "mac8", "mac9").toList.asJava)
//    transRecords.append(Array("mac2", "mac3", "mac6", "mac7", "mac9").toList.asJava)
//    transRecords.append(Array("mac3", "mac4", "mac6", "mac8", "mac9").toList.asJava)
//    transRecords.append(Array("mac1", "mac3", "mac5", "mac6", "mac7", "mac8", "mac9", "mac10").toList.asJava)
//    transRecords.append(Array("mac2", "mac5", "mac7", "mac8", "mac9", "mac10", "mac11").toList.asJava)
//    transRecords.append(Array("mac3", "mac4", "mac7", "mac8", "mac9", "mac11").toList.asJava)
//    transRecords.append(Array("mac2", "mac6", "mac8", "mac9", "mac10", "mac11").toList.asJava)
//    transRecords.append(Array("mac4", "mac5", "mac6", "mac7", "mac8", "mac10", "mac11").toList.asJava)
//    transRecords.append(Array("mac4", "mac5", "mac8", "mac10", "mac11").toList.asJava)
//    transRecords.append(Array("mac1", "mac2", "mac4", "mac7", "mac8", "mac10", "mac11").toList.asJava)
//    transRecords.append(Array("mac2", "mac3", "mac4", "mac6", "mac8", "mac9", "mac10").toList.asJava)
//    val fpTreeNoneCpb = new FPTreeNoneCpb()
//    fpTreeNoneCpb.setN(transRecords.size)
//    fpTreeNoneCpb.setMinCount(4)
//    fpTreeNoneCpb.setMinSupport(0)
//    val root = fpTreeNoneCpb.buildTree(transRecords.toList.asJava)
//    fpTreeNoneCpb.getHeaders(root)
//    val currentTimeStamp: Long = System.currentTimeMillis
//    fpTreeNoneCpb.fPMining("", currentTimeStamp)
//    val it = fpTreeNoneCpb.getTransformTable
//    println("success")
//    it.getFrequentList.asScala
//      .map(r=>{
//        println(r.asScala.map(r1=>r1._2.getItem).mkString(","))
//        //(v._1, r.asScala.map(r1=>r1._2.getItem).mkString(","), categoryBC.value,v._2.getSupport, v._2.getFrequentCount, dataSrcBC.value)
//      })
//    println("frequentMap 输出" + it.toString)
  }
}
