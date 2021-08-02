package com.suntek.algorithm.evaluation

import com.suntek.algorithm.common.conf.Param
import com.suntek.algorithm.common.util.SparkUtil
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

/**
 * @author liaojp
 *         2020-11-09
 */
object AcompanyEvaluation {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val tableName = "TB_ACCOMPANY_EVALUATION_RESULT"

  def process(param: Param): Unit = {
    val ss: SparkSession = SparkUtil.getSparkSession(param.master, "EvaluationForAccompany", param)
    param.keyMap.put("tableName", tableName)
    param.keyMap("evaluationType").toString match {
      case "rank" => RankingEvaluation.verify(ss, param)
      case "classify" => BinaryClassificationEvaluation.verify(ss, param)
      case _ => logger.error("请填写 evaluationType 配置：rank or classify")
        System.exit(1)
    }
    ss.close()
  }
}
