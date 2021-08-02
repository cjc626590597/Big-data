package com.suntek.algorithm

import com.suntek.algorithm.common.conf.Param
import com.suntek.algorithm.common.http.HttpUtil
import org.json.JSONObject
import org.junit.{Assert, Test}
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * @author zhy
  * @date 2020-12-11 15:58
  */
class HttpUtilTest  extends Assert{
  @Test
  def sendGetTest3(): Unit ={
    val url = "http://172.25.21.2:8084/das/service/manage/task/selectTaskInstanceList"
    val map: mutable.Map[String, Object] = mutable.Map()
    map.put("stepIdList", "99101,99102,99800,99801,99900,99901")
    val w = HttpUtil.sendPost(url, new JSONObject(map.asJava).toString)
    println("sdadsa")
    println(w)
  }

  @Test
  def sendGetTest(): Unit ={
    val url = "http://nacos-center.v-base:30848/nacos/v1/cs/configs?dataId=base-components&group=prophet&tenant=a85a37ef-5bec-478c-a60f-0b11f10b3da4"
    val w = HttpUtil.sendGet(url)
    println(w)
  }

  @Test
  def sendGetTest2(): Unit ={
    val url = "http://nacos-center.v-base:30848/nacos/v1/cs/configs?dataId=applications&group=prophet&tenant=a85a37ef-5bec-478c-a60f-0b11f10b3da4"
    val w = HttpUtil.sendGet(url)
    println(w)
  }


  @Test
  def initParamTest(): Unit ={

    val testJson = "local#--#{\"analyzeModel\":{\"params\":[{\"cnName\":\"关系类别\",\"enName\":\"relType\",\"desc\":\"关系类别,如：imei-imsi,imsi-imsi\",\"value\":\"car-car\"},{\"cnName\":\"数据库名称\",\"enName\":\"dataBaseName\",\"desc\":\"数据库名称\",\"value\":\"default\"},{\"cnName\":\"数据库类型\",\"enName\":\"databaseType\",\"desc\":\"数据库类型\",\"value\":\"hive\"}],\"isTag\":1,\"tagInfo\":[],\"mainClass\":\"com.suntek.algorithm.process.lifecycle.HiveLifeCycleHandler\",\"modelId\":8,\"modelName\":\"car-car的DTW\",\"stepId\":15,\"taskId\":12,\"descInfo\":\"\"},\"batchId\":20201106214000}"
    val param = new Param(testJson);

    println(param.keyMap)

  }
}
