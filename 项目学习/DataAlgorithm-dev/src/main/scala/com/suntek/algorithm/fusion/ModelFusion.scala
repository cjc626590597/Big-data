package com.suntek.algorithm.fusion

import com.suntek.algorithm.fusion.ModelFusionType.{AVG, RANK, VOTE}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.collection.{Iterable, mutable}
import scala.reflect.ClassTag

/**
 * 模型融合，包括平均，投票，排名平均。
 *
 * @param data
 * @param method
 */
class ModelFusion private(data: Array[RDD[((String, String), Double)]], method: String)
  extends Serializable {

  val N = 100

  def funsion(): RDD[((String, String), Double)] = {
    val cData = data.reduce(_.union(_))
    val nalg = data.length
    method match {
      case AVG => groupBykey(cData).mapValues(avg)
      case RANK => getRankData(data).mapValues(groupAvg(_, nalg)).flatMap(format)
      case VOTE => getRankData(data).mapValues(voteByGroupIndex).flatMap(format)
      //      case "logistic" => null
    }
  }


  def funsion(topN: Int ): RDD[((String, String), Double)] = {
    if (topN <= 0) {
      funsion()
    } else {
      val data = funsion().map { case ((a, b), sim) => (a, (b, sim)) }
      val value = groupBykey(data).mapValues { iter =>
        iter.toList.sortWith(_._2 > _._2)
          .take(topN)
      }
        .flatMap { case (a, list) => list.map(x => ((a, x._1), x._2)) }
      val tuples = value.collect()
      value
    }

  }

  /**
   * 转换格式
   *
   * @param tup
   * @return
   */
  def format(tup: (String, Map[String, Double])): Map[(String, String), Double] = {
    val (a, sti) = tup
    sti.map { case (b, i) =>
      ((a, b), i.formatted("%.2f").toDouble * N)
    }
  }


  /**
   * 按b分组后对排名求平均 todo
   *
   * @param iter
   * @param nalg
   * @return
   */
  def groupAvg(iter: Iterable[((String, Double), Int)], nalg: Int): Map[String, Double] = {
    val max = iter.maxBy(_._2)._2
    val dd = iter.groupBy(_._1._1).map { case (b: String
    , biter: Iterable[((String, Double), Int)]) =>
      (b, biter.map(_._2.toFloat / max).sum.toDouble / nalg)
    }
    dd
  }


  /**
   * 对排序编号进行投票，如果多个则随机取一个做排名。
   *
   * @param iter
   * @return
   */
  def voteByGroupIndex(iter: Iterable[((String, Double), Int)]): Map[String, Double] = {
    val max = iter.maxBy(_._2)._2
    val dd = iter.groupBy(_._2).flatMap { case (rank: Int, biter: Iterable[((String, Double), Int)]) =>
      val bMap = mutable.Map[String, Double]()
      biter.foreach { case ((b, _), _) =>
        val v = bMap.getOrElseUpdate(b, 0)
        bMap.update(b, v + 1)
      }
      var maxValue = -1.0
      val ab = ArrayBuffer[String]()
      for ((k, v) <- bMap) {
        if (v > maxValue) {
          maxValue = v
          ab.clear()
          ab.append(k)
        } else if (v == maxValue) {
          ab.append(k)
        }
      }
      ab.map(x => (x, rank.toDouble / max))
    }
    dd
  }


  /**
   *   对每个算法的数据：(（a,b）,score)先按 a 分组，得到的结果进行排名 ：即[a,list(b,score,index)].
   *   最后通过a join 每个算法的结果。
   * @param data
   * @return
   */
  def getRankData(data:Array[RDD[((String,String),Double)]]): RDD[(String, Iterable[((String, Double), Int)])] = {

    val dataArray = data.map(x=>{
      val rData = x.map { case ((a,b), score) =>(a,(b,score)) }
      groupBykey(rData).mapValues{ iter=>
        iter.toList.sortWith(_._2<_._2).zipWithIndex.map{case ((a,b),c)=>
          ((a,b),c+1)
        }
      }
    })
    groupBykey(dataArray.reduce(_.union(_)))
      .mapValues{iter=> iter.flatten}
  }


  def avg(iter: Iterable[Double]): Double = {
    iter.sum.formatted("%.2f").toDouble * N
  }


  def groupBykey[K: ClassTag, V: ClassTag](data: RDD[(K, V)]): RDD[(K, Iterable[V])] = {
    val createCombiner = (v: V) => Seq(v)
    val mergeValue = (c: Seq[V], v: V) => c.+:(v)
    val mergeCombiners = (c1: Seq[V], c2: Seq[V]) => c1 ++: c2
    data.combineByKey(createCombiner, mergeValue, mergeCombiners).asInstanceOf[RDD[(K, Iterable[V])]]
  }


}


object ModelFusionType {
  val AVG = "avg"
  val RANK = "rank"
  val VOTE = "vote"
}


object ModelFusion {

  def apply(data: Array[RDD[((String, String), Double)]], method: String,topN:Int=0): RDD[((String, String), Double)] = {
    new ModelFusion(data, method).funsion(topN)
  }


}