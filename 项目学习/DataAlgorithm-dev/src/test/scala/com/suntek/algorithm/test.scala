package com.suntek.algorithm

import java.util.Properties

import com.suntek.algorithm.common.bean.SaveParamBean
import com.suntek.algorithm.common.conf.{Constant, Param}
import com.suntek.algorithm.common.database.DataBaseFactory
import com.suntek.algorithm.common.util.SparkUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * @author zhy
  * @date 2020-12-3 14:38
  */
object test {
  def process(param: Param): Unit ={
    val ss = SparkUtil.getSparkSession(param.master, s"test", param)
    val type1 = "CAR"
    val type2 = "IMSI"
    val sql1 =
      s"""
         |SELECT obja, objb
         |  FROM tb_acc_sample
         | WHERE type='${type1}-${type2}'
         |   AND bd='20201112'
      """.stripMargin
    val ret1 = ss.sql(sql1)
      .rdd
      .map(v=>{
        val obja = v.getString(0)
        val objb = v.getString(1)
        (obja, objb)
      })

    /* val sql2 =
      s"""
        |SELECT object_id_a, object_id_b, score
        | FROM DM_ENTROPY_RESULT
        |WHERE stat_date='20200907'
        |  AND upper(type)='${type1}-${type2}'
        |  AND upper(RESULT_TYPE)= '${type1}-${type2}-PER-LONG-DAY'
        |  AND times > 2
      """.stripMargin*/

    val sql2 =
     s"""
       |SELECT object_id_a, object_id_b, score
       | FROM DM_ENTROPY_RESULT
       |WHERE stat_date='20200907'
       |  AND upper(type)='${type1}-${type2}'
       |  AND upper(RESULT_TYPE)= '${type1}-${type2}-PER-LONG-DAY'
       |  AND times > 2
     """.stripMargin


    val ret2 = ss.sql(sql2)
      .rdd
      .map(v=>{
        val obja = v.get(0).toString
        val objb = v.get(1).toString
        val score = v.get(2).toString
        (obja, (objb, score.toDouble))
      })

    val ret = ret2.groupByKey()
      .map(v=>{
        val id = v._1
        val list = v._2.toList.sortBy(_._2 * -1)// 从大到小
        (id, list)
      })
    val ret_f = ret1.join(ret)
      .map(r=>{
        val id1 = r._1
        val id2 = r._2._1
        var i = 0
        var flag = true
        val list = r._2._2
        if(list != null && list.size > 0){
          var mingzNode = ("", -1D)
          while (flag && i< list.size){
            val node = list(i)
            if(node._1.equals(id2)){
              mingzNode = node
              flag = false
            }
            i += 1
          }
          // (分数， 这个分数有多少个)
          val list1 = list.groupBy(_._2).map(v=> (v._1, v._2.size)).toList.sortBy(_._1 * -1 )// 从大到小
          var minNums = 0
          var maxNums = 0
          var curNode = (-1D, 0)
          var nums = 0
          var flag1 = true
          i = 0
          while (flag1 && i< list1.size){
            val node = list1(i)
            if(node._1 > mingzNode._2){
              nums = nums + node._2
            }
            if(node._1 == mingzNode._2){
              curNode = node
              flag1 = false
            }
            i += 1
          }
          minNums = nums + 1
          maxNums = nums + curNode._2
          (id1, mingzNode, minNums, maxNums)
        }else{
          ("", ("", -1D), 0, 0)
        }

      })
      .filter(_._2._1.nonEmpty)
      .map(v=>{
        // id1, id2, 分数, 最低排名，最高排名
        Row.fromTuple(v._1, v._2._1, v._2._2.toString, v._3.toString, v._4.toString)
      })
    println("sssssssssssssssssss" + ret_f.count())
    insertDataDetail(ss, ret_f, s"dtw_${type1}_${type2}".toLowerCase)
  }

  def insertDataDetail(ss: SparkSession,
                       rowRdd: RDD[Row],
                       types: String)
  : Unit = {
    if(rowRdd == null){
      return
    }
    // (ID1, ID2, 分数, 最低排名, 最高排名)
    val params = "OBJECT_ID_A,OBJECT_ID_B,SCORE,LOW_RANK,UPPER_RANK"
    val schema = StructType(List(
      StructField("OBJECT_ID_A", StringType, nullable = true),
      StructField("OBJECT_ID_B", StringType, nullable = true),
      StructField("SCORE", StringType, nullable = true),
      StructField("LOW_RANK", StringType, nullable = true),
      StructField("UPPER_RANK", StringType, nullable = true)
    ))

    //转换成Row
    val df = ss.createDataFrame(rowRdd, schema)
    // df.show()

    val database = DataBaseFactory(ss, new Properties(), "hive")
    val saveParamBean = new SaveParamBean("test_zhy", Constant.OVERRIDE_DYNAMIC, database.getDataBaseBean)
    saveParamBean.dataBaseBean =  database.getDataBaseBean
    saveParamBean.params = params
    saveParamBean.partitions = Array[(String, String)](("TYPE", s"$types"))
    database.save(saveParamBean, df)
  }
}
