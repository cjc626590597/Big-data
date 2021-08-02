package com.suntek.algorithm.common.util

import java.text.SimpleDateFormat
import java.util.TimeZone

import com.alibaba.fastjson.{JSON, JSONObject}
import com.suntek.algorithm.common.conf.Param
import com.suntek.algorithm.common.http.HttpUtil
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.JavaConverters._

/**
  * @author zhy
  * @date 2020-9-24 15:43
  */
object ConstantUtil {

  val STATUS_UPDATE_CONTEXT_PATH = "/status/update"

  val STATUS_UPDATE_TASK_INSTANCE = "/service/manage/task/updateTaskStatusUpdate"

  val STATUS_SELECT_TASK_INSTANCE = "/service/manage/task/selectTaskInstanceList"

  val logger = LoggerFactory.getLogger(this.getClass)
  val RUNNING = "1"
  val SUCCESS = "2"
  val FAIL = "3"
  val UNKNOWN = "0"

  var taskId:String = null
  var stepId:String = null
  var jobId:String = null
  var logId:String = null
  var jobServiceUrl:String = null
  var taskInstanceUrl:String = null
  var taskSelectInstanceUrl:String = null
  var moduleName:String = "算法引擎"
  var subModuleName:String = null
  var batchId = ""


  def initTaskInfo(jobId:String, logId:String, jobServiceUrl:String): Unit = {
    this.jobId = jobId
    this.logId = logId
    this.jobServiceUrl = jobServiceUrl + STATUS_UPDATE_CONTEXT_PATH

  }

  def init(param: Param) = {
    this.taskId = param.keyMap("taskId").toString
    this.stepId = param.keyMap("stepId").toString
    this.moduleName = param.keyMap.getOrElse("moduleName", "算法引擎").toString
    this.subModuleName = param.keyMap("mainClass").toString.substring(param.keyMap("mainClass").toString.lastIndexOf(".")+1) + ":" + param.releTypeStr
    this.batchId = param.keyMap.getOrElse("batchId", "-1").toString
    this.taskInstanceUrl = param.keyMap("xxl.console.url").toString + STATUS_UPDATE_TASK_INSTANCE
    this.taskSelectInstanceUrl = param.keyMap("xxl.console.url").toString + STATUS_SELECT_TASK_INSTANCE
  }


  def successTask(): Unit = {
    completeTask( SUCCESS, "执行成功")
  }


  def errorTask( errMessage: String) = {
    val messageTmp = s"执行失败：${errMessage}"
    completeTask( FAIL, messageTmp)
  }

  def completeTask( statusCode: String, execMsg: String): Unit = {
    val map: mutable.Map[String, Object] = mutable.Map()
    map.put("jobId", jobId)
    map.put("logId", logId)
    map.put("moduleName", moduleName)
    map.put("subModuleName", subModuleName)
    map.put("status", statusCode)
    map.put("execMsg", if(execMsg.length > 99)execMsg.substring(0, 99) else execMsg)
    map.put("batchTime", batchId)

    logger.info(s"CompleteTask And Update status ${jobServiceUrl} :${jobId},  ${logId}, ${moduleName}, ${subModuleName}, ${batchId}, ${statusCode}, ${execMsg}")
    val resultStr = HttpUtil.sendPost(jobServiceUrl, new JSONObject(map.asJava).toString)
    val resultJSON = JSON.parseObject(resultStr)
    val resultCode = resultJSON.getIntValue("code")
    if (resultCode != 200) {
      logger.error(s"任务状态更新失败：${ConstantUtil.jobServiceUrl} ,result=${resultStr}")
    }else{
      logger.info(s"任务状态更新成功：${ConstantUtil.jobServiceUrl} ,result=${resultStr}")
    }

  }


  def success(): Unit = {
    val remarks = "执行成功"
    updateTaskInstanceStatus(  SUCCESS, remarks)
  }


  def error(errMessage: String) = {
    val remarks = s"执行失败：${errMessage}"
    updateTaskInstanceStatus( FAIL, remarks)
  }

  def updateTaskInstanceStatus(status:String, remarks: String){
    val map: mutable.Map[String, Object] = mutable.Map()
    map.put("taskId", taskId)
    map.put("stepId", stepId)
    map.put("batchId", batchId)
    map.put("status", status)
    map.put("remarks", remarks)
    HttpUtil.sendPost(taskInstanceUrl, new JSONObject(map.asJava).toString)
  }

  def selectTaskInstanceList(parentStepId: String): JSONObject = {
    val map: mutable.Map[String, Object] = mutable.Map()
    map.put("stepIdList", parentStepId)
    logger.info(s"taskSelectInstanceUrl = ${taskSelectInstanceUrl},parentStepId= ${parentStepId}")
    val ret = HttpUtil.sendPost(taskSelectInstanceUrl, new JSONObject(map.asJava).toString)
    JSON.parseObject(ret)
  }

  def checkTask(batchId: String, parentStepId: String): Boolean = {
    logger.info(s"checkTask:batchId=${batchId}  parentStepId=${parentStepId}")
    if(parentStepId.isEmpty) return false
    try{
      val sdf = new SimpleDateFormat("yyyyMMdd")
      sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
      val curTime = sdf.parse(batchId.substring(0, 8)).getTime

      val jsonObjectP = selectTaskInstanceList(parentStepId.toString)
      val parentTaskStepInstanceList = jsonObjectP.getJSONObject("content").getJSONArray("ttaskStepInstanceList")
      for(i <- 0 until parentTaskStepInstanceList.size()){
        val parentJsonObject = parentTaskStepInstanceList.getJSONObject(i)
        val parentBatchId = parentJsonObject.getString("batchId")
        val jobId = parentJsonObject.getString("jobId")
        val parentTime = sdf.parse(parentBatchId.substring(0, 8)).getTime
        val parentStatus = parentJsonObject.getString("status")
        logger.info(s"检查:batchId=${jobId} parentTime=${parentTime} parentStatus=${parentStatus} parentStepId=${parentBatchId} parentBatchId=${parentBatchId}")
        if(parentTime < curTime || (parentTime == curTime && !"2".equals(parentStatus))){
          logger.info(s"父任务 jobId=${jobId} curTime = ${curTime} parentTime = ${parentTime} parentBatchId = ${parentBatchId} parentStatus=${parentStatus}")
          return true
        }
      }
      return false
    }catch {
      case ex: Exception =>
        logger.error("任务状态检查异常：",ex)
        return false
    }
  }

  def isIntByRegex(str: String): Boolean = {
    val pattern = """^(\d+)$""".r
    str match {
      case pattern(_*) => true
      case _ => false
    }
  }
}
