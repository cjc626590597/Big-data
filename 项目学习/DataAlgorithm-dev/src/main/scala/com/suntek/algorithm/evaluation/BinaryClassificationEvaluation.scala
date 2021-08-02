package com.suntek.algorithm.evaluation


import java.text.SimpleDateFormat
import java.util.{ Date, Properties, TimeZone}

import com.suntek.algorithm.common.bean.{QueryBean, SaveParamBean}
import com.suntek.algorithm.common.conf.{Constant, Param}
import com.suntek.algorithm.common.database.DataBaseFactory
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}




/**
 * 二分类评估 包括 查全率，查准率，F1值，ROC，AUC
 * 支持lcss，fptree
 * 支持 car_car ，imsi_imsi ，car_imsi，imsi_car 四种同行关系的评估
 *
 * @author liaojp
 *         2020-11-09
 */
object BinaryClassificationEvaluation {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def genProperties(param: Param): Properties = {
    val properties = new Properties()
    properties.put("jdbc.url", param.keyMap.getOrElse("jdbc.url", ""))
    properties.put("username", param.keyMap.getOrElse("jdbc.username", ""))
    properties.put("password", param.keyMap.getOrElse("jdbc.password", ""))
    properties.put("numPartitions", param.keyMap.getOrElse("NUM_PARTITIONS", "10"))
    properties
  }



  def insertData(ss: SparkSession,
                 retDetail: RDD[(String, String, String, String, String)],
                 properties: Properties,
                 tableName: String,
                 param: Param)
  : Unit = {
    if (retDetail == null) {
      return
    }

    val databaseType = param.keyMap("databaseType").toString
    val params = "POSID,NEGID,THR_or_FPR,VAL_or_TPR,EVALUATION"

    //  (具体算法，正样本集ID，负样本集ID，阈值，值，"评估指标")
    val schema = StructType(List(
      StructField("POSID", StringType, nullable = true),
      StructField("NEGID", StringType, nullable = true),
      StructField("THR_or_FPR", DoubleType, nullable = true),
      StructField("VAL_or_TPR", DoubleType, nullable = true),
      StructField("EVALUATION", StringType, nullable = true)
    ))
    //转换成Row
    val rowRdd = retDetail.asInstanceOf[RDD[(String, String, String, String, String)]]
      .map(r => Row.fromTuple((r._1,r._2,if(r._3.isEmpty) 0 else r._3.toDouble,r._4.toDouble,r._5)))

    val df = ss.createDataFrame(rowRdd, schema)

    val batchIdDateFormat = new SimpleDateFormat("yyyyMMddHH")
    batchIdDateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))

    val database = DataBaseFactory(ss, properties, databaseType)
    val saveParamBean = new SaveParamBean(tableName, Constant.OVERRIDE_DYNAMIC, database.getDataBaseBean)
    saveParamBean.dataBaseBean = database.getDataBaseBean
    saveParamBean.numPartitions = param.keyMap.getOrElse("numPartitions", "10").toString.toInt
    saveParamBean.params = params
    saveParamBean.partitions = Array[(String, String)](("alg", s"${param.keyMap("algorithm")}-${param.keyMap("aType")}")
      , ("bd", s"${param.keyMap("batchId").toString.substring(0, 8)}")
      , ("ad", s"${batchIdDateFormat.format(new Date())}")
    )
    database.save(saveParamBean, df)
  }


  /**
   *
   * 每一个预测值作为阈值，计算预测值大于该阈值的 查准率，查全率，F1值，ROC，AUC
   *
   * @param ss
   * @param data (数据集ID,预测的相似度，真实标签)
   * @param param
   * @return
   * 查准率，查全率，F1值
   * (具体算法，正样本集ID，负样本集ID，阈值，值，"评估指标")
   * ROC
   * (具体算法，正样本集ID，负样本集ID，false positive rate, true positive rate，"ROC")
   * AUC
   * (具体算法，正样本集ID，负样本集ID，空, AUC值，"AUC")
   */
  def evaluation(ss: SparkSession, data: RDD[(String, Double, Int)], param: Param)
  : RDD[(String, String, String, String, String)] = {
    //正样本标识
    val posFlag = 1
    //负样本标识
    val negFlag = 0
    val positiveSampleIDs = data.filter { case (_, _, lable) => lable == posFlag }
      .map { case (id, _, _) => id }.collect().toSet
    val negativeSampleIDs = data.filter { case (_, _, lable) => lable == negFlag }
      .map { case (id, _, _) => id }.collect().toSet
    val result = positiveSampleIDs.flatMap { posid =>
      negativeSampleIDs.map { negid =>
        val evaluationData = data.filter { case (id, _, _) => id == negid }
          .union(data.filter { case (id, _, _) => id == posid })
          .map { case (_, score, lable) => (score, lable.toDouble) }
        val metrics = new BinaryClassificationMetrics(evaluationData)
        val extra = ss.sparkContext.broadcast((posid, negid))
        val precision = metrics.precisionByThreshold.map { case (t, p) =>
          val (pid, nid) = extra.value
          (pid, nid, t, p, "precision")
        }
        val recall = metrics.recallByThreshold.map { case (t, p) =>
          val (pid, nid) = extra.value
          (pid, nid, t, p, "recall")
        }
        val f1 = metrics.fMeasureByThreshold.map { case (t, p) =>
          val (pid, nid) = extra.value
          (pid, nid, t, p, "f1")
        }
        //        // ROC Curve
        val roc = metrics.roc.map {
          case (fpr, tpr) =>
            val (pid, nid) = extra.value
            (pid, nid, fpr, tpr, "ROC")
        }
        //        // AUROC
        val auROC = metrics.areaUnderROC
        val (pid, nid) = extra.value
        val auc = ss.sparkContext.parallelize[(String, String, Double, Double, String)](Seq((pid, nid, 0.0, auROC, "AUC")))
        precision.union(recall).union(f1).union(roc).union(auc)
      }
    }
    val array = result.toArray
    if (array.length == 0) {
      null
    } else {
      var r = array.head
      for (i <- 1 until array.length) {
        r = r.union(array(i))
      }
      r.map(x => (x._1, x._2, x._3.toString, x._4.toString, x._5))
    }
  }

  def verify(ss: SparkSession,
             param: Param): Unit = {
    val properties = genProperties(param: Param)
    param.keyMap.put("properties", properties)
    val queryParam = new QueryBean()
    queryParam.parameters = List[Object]()
    queryParam.isPage = false
    val tableName = param.keyMap("tableName").toString


    val data = DataManager.loadData(ss, param, queryParam,dataTransform)
    //(数据集ID,预测的相似度，真实标签)
    val r = evaluation(ss, data, param)
    insertData(ss, r, properties, tableName, param)

  }

  /**
   * 获取正样本，负样本采用随机无重复取样，大小为正样本的2倍: neg_cnt = 2*pos_cnt
   * @param simData  相似度数据 （目标1，目标2，相似度）
   * @param posSetBc  正样本（目标1，目标2）
   * @param param   参数
   * @param ss    SparkSession
   * @return
   */
  def dataTransform(simData:RDD[(String,String,Double)],posSetBc:Broadcast[Set[(String,String)]],param:Param,ss:SparkSession)
  : RDD[(String, Double, Int)] =
  {
    val posID = param.keyMap("posId").toString
    //过滤正样本
    val posEvaData = simData.filter(x=> posSetBc.value.contains((x._1,x._2))).map{ case (_, _, score) =>(posID, score,1) }
    //随机抽取负样本
    val poscnt = posEvaData.count().toInt

    val negEvaData = simData.filter(x=> !posSetBc.value.contains((x._1,x._2)))
      .map{ case (_, _, score) =>(s"neg_$posID", score,0) }.takeSample(withReplacement = false,2*poscnt)
    logger.info(s"正样本个数:$poscnt,负样本的个数:${negEvaData.length}")
    posEvaData.union(ss.sparkContext.parallelize(negEvaData))
    // (数据集ID,预测的相似度，真实标签)
  }

}




